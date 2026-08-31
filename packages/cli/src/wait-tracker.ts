/**
 * ============================================================================
 * WaitTracker - 等待执行追踪器
 * ============================================================================
 * 
 * 这个服务负责管理工作流执行中的"等待"状态。当工作流执行到 Wait 节点时，
 * 执行会被暂停并持久化到数据库，WaitTracker 负责在适当的时候恢复这些执行。
 * 
 * 核心功能：
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │ 1. 追踪等待中的执行 (waitingExecutions)                                │
 * │    - 维护内存中的定时器映射                                            │
 * │    - 每个执行 ID 对应一个定时器                                        │
 * │                                                                         │
 * │ 2. 定期查询数据库中的等待执行 (getWaitingExecutions)                   │
 * │    - 每 60 秒轮询一次                                                 │
 * │    - 为每个等待执行设置定时器                                          │
 * │                                                                         │
 * │ 3. 恢复等待的执行 (startExecution)                                    │
 * │    - 从数据库加载执行数据                                              │
 * │    - 通过 WorkflowRunner 重新启动执行                                 │
 * │    - 如果存在父执行，触发父执行恢复                                    │
 * │                                                                         │
 * │ 4. 子工作流完成后恢复父执行 (resumeParentExecution)                    │
 * │    - 等待子执行完成                                                   │
 * │    - 将子执行结果合并到父执行                                          │
 * │    - 重新启动父执行                                                   │
 * │                                                                         │
 * │ 5. Leader 选举支持                                                    │
 * │    - 只有 Leader 节点执行追踪                                          │
 * │    - Leader 切换时自动停止/启动追踪                                    │
 * └─────────────────────────────────────────────────────────────────────────┘
 * 
 * 执行状态流转：
 * ┌──────────────┐    等待时间到达    ┌──────────────┐    执行完成    ┌──────────────┐
 * │   waiting    │ ──────────────────▶ │   running    │ ──────────────▶ │   success    │
 * │  (持久化)    │                     │   (恢复)     │                 │   / error    │
 * └──────────────┘                     └──────────────┘                 └──────────────┘
 * 
 * 关键概念：
 * - waitingExecutions: 内存缓存，存储等待中的执行及其定时器
 * - mainTimer: 主轮询定时器，每 60 秒检查数据库
 * - parentExecution: 父执行（当子工作流执行完成时，需要恢复父执行）
 * ============================================================================
 */

import { Logger } from '@n8n/backend-common';
import { ExecutionRepository } from '@n8n/db';
import { OnLeaderStepdown, OnLeaderTakeover } from '@n8n/decorators';
import { Service } from '@n8n/di';
import { InstanceSettings } from 'n8n-core';
import { ensureError } from '@n8n/utils/errors/ensure-error';
import { sleep } from '@n8n/utils/sleep';
import {
    UnexpectedError,
    UserError,
    type IRun,
    type IWorkflowExecutionDataProcess,
    type RelatedExecution,
} from 'n8n-workflow';

import { ActiveExecutions } from '@/active-executions';
import { ExecutionAlreadyResumingError } from '@/errors/execution-already-resuming.error';
import { ExecutionPersistence } from '@/executions/execution-persistence';
import { OwnershipService } from '@/services/ownership.service';
import { WorkflowRunner } from '@/workflow-runner';

import {
    shouldRestartParentExecution,
    updateParentExecutionWithChildResults,
} from './workflow-helpers';

/** 
 * 父执行恢复的最大重试次数 
 * 每个步骤（数据库更新 + 启动执行）最多尝试 3 次
 */
const MAX_PARENT_RESUME_ATTEMPTS = 3;

/**
 * ============================================================================
 * isRetryableResumeError() - 判断错误是否可重试
 * ============================================================================
 * 
 * 决定一个错误是否值得重试：
 * 
 * 不可重试的错误（直接抛出）：
 * - UserError: 用户输入错误，重试不会改变结果
 * - UnexpectedError: 预期外的错误，需要人工介入
 * 
 * 可重试的错误（会进行重试）：
 * - OperationalError: 表示临时性问题（如数据库连接超时）
 * - 数据库或 Redis 故障: 原始错误（非 n8n 错误类）
 * - 其他所有错误
 * 
 * @param error - 要检查的错误
 * @returns 如果应该重试返回 true
 */
function isRetryableResumeError(error: unknown): boolean {
    return !(error instanceof UserError || error instanceof UnexpectedError);
}

/**
 * ============================================================================
 * WaitTracker 服务类
 * ============================================================================
 * 
 * 使用 @Service() 装饰器标记为可注入的服务
 * 
 * 设计模式：
 * - 单例模式：整个应用只有一个实例
 * - Leader 选举模式：只有 Leader 节点执行追踪
 * - 异步队列模式：使用定时器队列管理等待执行
 */
@Service()
export class WaitTracker {
    /**
     * 等待中的执行映射表
     * 
     * 数据结构：
     * {
     *   "executionId": {
     *     executionId: string,      // 执行 ID
     *     timer: NodeJS.Timeout     // 定时器，在等待时间到达时触发
     *   }
     * }
     * 
     * 作用：内存缓存，避免重复查询数据库
     * 注意：Leader 切换时会被清理
     */
    private waitingExecutions: {
        [key: string]: {
            executionId: string;
            timer: NodeJS.Timeout;
        };
    } = {};

    /**
     * 主轮询定时器
     * 每 60 秒触发一次，检查数据库中是否有新的等待执行
     */
    mainTimer: NodeJS.Timeout;

    constructor(
        private readonly logger: Logger,
        private readonly executionRepository: ExecutionRepository,
        private readonly executionPersistence: ExecutionPersistence,
        private readonly ownershipService: OwnershipService,
        private readonly activeExecutions: ActiveExecutions,
        private readonly workflowRunner: WorkflowRunner,
        private readonly instanceSettings: InstanceSettings,
    ) {
        // 为日志添加作用域前缀，便于区分
        this.logger = this.logger.scoped('waiting-executions');
    }

    /**
     * ========================================================================
     * has() - 检查执行是否在等待中
     * ========================================================================
     * 
     * @param executionId - 执行 ID
     * @returns 如果该执行正在等待中返回 true
     * 
     * @remarks
     * 用于快速检查，避免重复添加定时器
     */
    has(executionId: string) {
        return this.waitingExecutions[executionId] !== undefined;
    }

    /**
     * ========================================================================
     * init() - 初始化 WaitTracker
     * ========================================================================
     * 
     * 在应用启动时调用，只在 Leader 节点上启动追踪
     * 
     * @remarks
     * 使用 @OnLeaderTakeover 装饰器确保：
     * - 只有 Leader 节点执行追踪
     * - Leader 切换时自动调用 startTracking
     */
    init() {
        if (this.instanceSettings.isLeader) this.startTracking();
    }

    /**
     * ========================================================================
     * @OnLeaderTakeover - startTracking()
     * ========================================================================
     * 
     * 在节点成为 Leader 时自动调用，启动等待执行追踪
     * 
     * 工作流程：
     * 1. 设置主轮询定时器（每 60 秒）
     * 2. 立即执行一次查询（获取当前所有等待执行）
     * 3. 为每个等待执行设置独立定时器
     * 
     * @remarks
     * 为什么是 60 秒？
     * - 平衡实时性和性能开销
     * - 数据库轮询频率不宜过高
     * - 短等待（<60 秒）由内存定时器处理
     */
    @OnLeaderTakeover()
    private startTracking() {
        // 每 60 秒轮询一次数据库，查找即将到期的等待执行
        this.mainTimer = setInterval(() => {
            void this.getWaitingExecutions();
        }, 60000);

        // 立即执行一次，避免等待第一个 60 秒
        void this.getWaitingExecutions();

        this.logger.debug('Started tracking waiting executions');
    }

    /**
     * ========================================================================
     * getWaitingExecutions() - 获取等待中的执行
     * ========================================================================
     * 
     * 从数据库查询所有等待中的执行，并为它们设置定时器
     * 
     * 执行流程：
     * ┌─────────────────────────────────────────────────────────────────────┐
     * │ 1. 查询数据库中的等待执行 (getWaitingExecutions)                  │
     * │ 2. 如果没有等待执行，返回                                         │
     * │ 3. 遍历每个等待执行：                                             │
     * │    a. 检查是否已经在 waitingExecutions 中                        │
     * │    b. 如果不在，计算触发时间 (waitTill - now)                    │
     * │    c. 创建定时器，在触发时间到达时调用 startExecution            │
     * │    d. 将定时器存入 waitingExecutions                             │
     * └─────────────────────────────────────────────────────────────────────┘
     * 
     * @remarks
     * 定时器的触发时间 = waitTill - 当前时间
     * 如果 waitTill 已经过去，定时器会立即触发
     */
    async getWaitingExecutions() {
        this.logger.debug('Querying database for waiting executions');

        // 从数据库查询所有等待中的执行
        // 这些执行的状态为 'waiting'，且 waitTill 不为空
        const executions = await this.executionRepository.getWaitingExecutions();

        if (executions.length === 0) {
            return;
        }

        const executionIds = executions.map((execution) => execution.id).join(', ');
        this.logger.debug(
            `Found ${executions.length} executions. Setting timer for IDs: ${executionIds}`,
        );

        // 为每个等待执行设置定时器
        for (const execution of executions) {
            const executionId = execution.id;
            // 避免重复添加定时器
            if (this.waitingExecutions[executionId] === undefined) {
                // 计算距离触发时间还有多久（毫秒）
                const triggerTime = execution.waitTill!.getTime() - new Date().getTime();
                this.waitingExecutions[executionId] = {
                    executionId,
                    // 设置定时器，在触发时间到达时恢复执行
                    timer: setTimeout(() => {
                        void this.startExecution(executionId);
                    }, triggerTime),
                };
            }
        }
    }

    /**
     * ========================================================================
     * stopExecution() - 停止追踪执行
     * ========================================================================
     * 
     * 当执行不再需要等待时调用，清理对应的定时器
     * 
     * 使用场景：
     * - 执行被取消
     * - 执行已完成
     * - 执行被手动恢复
     * 
     * @param executionId - 要停止追踪的执行 ID
     */
    stopExecution(executionId: string) {
        if (!this.waitingExecutions[executionId]) return;

        // 清除定时器，防止误触发
        clearTimeout(this.waitingExecutions[executionId].timer);

        // 从内存映射中删除
        delete this.waitingExecutions[executionId];
    }

    /**
     * ========================================================================
     * startExecution() - 恢复等待的执行
     * ========================================================================
     * 
     * 当等待时间到达或 Webhook 触发时调用，重新启动执行
     * 
     * 执行流程：
     * ┌─────────────────────────────────────────────────────────────────────┐
     * │ 1. 从等待映射中删除该执行                                         │
     * │ 2. 从数据库加载完整的执行数据 (findSingleExecution)               │
     * │ 3. 验证执行状态：                                                 │
     * │    - 执行必须存在                                                │
     * │    - 执行不能已完成 (finished)                                   │
     * │    - 工作流必须已保存 (有 ID)                                    │
     * │ 4. 获取工作流所属项目 (用于权限检查)                              │
     * │ 5. 构建执行数据 (IWorkflowExecutionDataProcess)                  │
     * │ 6. 通过 WorkflowRunner 重新启动执行                              │
     * │ 7. 如果存在父执行，触发父执行恢复                                 │
     * └─────────────────────────────────────────────────────────────────────┘
     * 
     * @param executionId - 要恢复的执行 ID
     * @throws UnexpectedError - 如果执行不存在、已完成或工作流未保存
     * @throws ExecutionAlreadyResumingError - 如果执行已经在恢复中（幂等性保护）
     */
    async startExecution(executionId: string) {
        this.logger.debug(`Resuming execution ${executionId}`, { executionId });
        
        // 从等待映射中删除（防止重复触发）
        delete this.waitingExecutions[executionId];

        // === 步骤 1: 从数据库加载执行数据 ===
        const fullExecutionData = await this.executionPersistence.findSingleExecution(executionId, {
            includeData: true,      // 包含执行数据
            unflattenData: true,    // 展开嵌套数据
        });

        // === 步骤 2: 验证执行状态 ===
        if (!fullExecutionData) {
            throw new UnexpectedError('Execution does not exist.', { extra: { executionId } });
        }
        if (fullExecutionData.finished) {
            throw new UnexpectedError('The execution did succeed and can so not be started again.');
        }

        if (!fullExecutionData.workflowData.id) {
            throw new UnexpectedError('Only saved workflows can be resumed.');
        }

        // === 步骤 3: 获取工作流所属项目 ===
        const workflowId = fullExecutionData.workflowData.id;
        const project = await this.ownershipService.getWorkflowProjectCached(workflowId);

        // === 步骤 4: 构建执行数据 ===
        const data: IWorkflowExecutionDataProcess = {
            executionMode: fullExecutionData.mode,
            executionData: fullExecutionData.data,
            workflowData: fullExecutionData.workflowData,
            projectId: project.id,
            pushRef: fullExecutionData.data.pushRef,   // 用于推送消息到前端
            startedAt: fullExecutionData.startedAt,    // 保持原始开始时间
        };

        // === 步骤 5: 重新启动执行 ===
        try {
            await this.workflowRunner.run(data, false, false, {
                executionId,
                expectedStatus: 'waiting',  // 期望的执行状态
            });
        } catch (error) {
            // 幂等性保护：如果执行已经在恢复中，忽略错误
            // 这发生在 "run once for each item" 模式中，多个子执行同时完成
            if (error instanceof ExecutionAlreadyResumingError) {
                this.logger.debug(
                    `Execution ${executionId} is already being resumed, skipping duplicate resume`,
                    { executionId },
                );
                return;
            }
            // 其他错误重新抛出
            throw error;
        }

        // === 步骤 6: 处理父执行恢复 ===
        // 如果这个执行是子工作流，父执行可能需要恢复
        const { parentExecution } = fullExecutionData.data;
        if (shouldRestartParentExecution(parentExecution)) {
            // 等待子执行完成后，恢复父执行
            // 使用 fire-and-forget 模式，不阻塞当前流程
            void this.resumeParentExecution(
                parentExecution,
                this.activeExecutions.getPostExecutePromise(executionId),
                { executionId, workflowId },
            );
        }
    }

    /**
     * ========================================================================
     * resumeParentExecution() - 恢复父执行
     * ========================================================================
     * 
     * 当子工作流执行完成后，恢复父工作流的执行
     * 
     * 场景：
     * ┌─────────────────────────────────────────────────────────────────────┐
     * │ 父工作流:                                                         │
     * │   ┌────────────┐     ┌──────────────┐     ┌─────────────┐       │
     * │   │   Start    │ ──▶ │ Execute Node │ ──▶ │   Wait      │       │
     * │   └────────────┘     └──────────────┘     └─────────────┘       │
     * │                              │                                    │
     * │                              ▼                                    │
     * │                       子工作流执行                                 │
     * │                      (异步运行)                                    │
     * │                              │                                    │
     * │                              ▼                                    │
     * │                      子执行完成                                    │
     * │                     触发父执行恢复                                 │
     * └─────────────────────────────────────────────────────────────────────┘
     * 
     * 执行流程：
     * 1. 等待子执行完成 (executePromise)
     * 2. 如果子执行仍在等待，不处理
     * 3. 重试机制：将子执行结果更新到父执行（最多 3 次）
     * 4. 重试机制：启动父执行（最多 3 次）
     * 
     * @param parentExecution - 父执行信息（ID、工作流等）
     * @param executePromise - 子执行的 Promise，等待其完成
     * @param childExecution - 子执行信息（可选）
     * 
     * @remarks
     * 重试机制：
     * - 每个步骤最多尝试 3 次
     * - 使用指数退避（100ms, 200ms, 400ms）
     * - 只有可重试的错误才会重试
     * - 所有重试失败后，记录错误日志（不抛出）
     * 
     * 注意：这个方法是 fire-and-forget 的，永远不会 reject
     */
    async resumeParentExecution(
        parentExecution: RelatedExecution,
        executePromise: Promise<IRun | undefined>,
        childExecution?: RelatedExecution,
    ): Promise<void> {
        try {
            // === 步骤 1: 等待子执行完成 ===
            const subworkflowResults = await executePromise;
            if (!subworkflowResults) return;
            
            // 如果子执行仍在等待（不是完成状态），不处理
            if (subworkflowResults.status === 'waiting') return;

            // === 步骤 2: 更新父执行（带重试） ===
            // 将子执行的结果合并到父执行中
            await this.withRetry(
                async () => {
                    await updateParentExecutionWithChildResults(
                        parentExecution.executionId,
                        subworkflowResults,
                        childExecution,
                    );
                },
                MAX_PARENT_RESUME_ATTEMPTS,
                isRetryableResumeError,
            );

            // === 步骤 3: 启动父执行（带重试） ===
            await this.withRetry(
                async () => {
                    await this.startExecution(parentExecution.executionId);
                },
                MAX_PARENT_RESUME_ATTEMPTS,
                isRetryableResumeError,
            );
        } catch (error) {
            // 所有重试都失败后，记录错误日志
            // 注意：不抛出错误，因为调用方使用 fire-and-forget
            this.logger.error('Failed to resume parent execution after sub-workflow completed', {
                parentExecutionId: parentExecution.executionId,
                error: ensureError(error).message,
            });
        }
    }

    /**
     * ========================================================================
     * withRetry() - 带重试的操作执行器
     * ========================================================================
     * 
     * 通用重试机制，支持指数退避和可重试错误判断
     * 
     * @param operation - 要执行的操作（返回 Promise）
     * @param maxAttempts - 最大尝试次数
     * @param shouldRetry - 判断错误是否可重试的函数（默认所有错误都可重试）
     * 
     * @throws 如果所有尝试都失败，抛出最后一次的错误
     * 
     * @example
     * ```typescript
     * await withRetry(
     *   async () => await db.update(executionId, data),
     *   3,
     *   (error) => error instanceof DatabaseConnectionError
     * );
     * ```
     * 
     * @remarks
     * 退避策略：100ms * 2^(attempt-1)
     * - 第 1 次失败后等待 100ms
     * - 第 2 次失败后等待 200ms
     * - 第 3 次失败后等待 400ms
     */
    private async withRetry(
        operation: () => Promise<void>,
        maxAttempts: number,
        shouldRetry: (error: unknown) => boolean = () => true,
    ): Promise<void> {
        for (let attempt = 1; ; attempt++) {
            try {
                await operation();
                return;  // 成功，退出循环
            } catch (error) {
                // 如果达到最大尝试次数或错误不可重试，抛出错误
                if (attempt >= maxAttempts || !shouldRetry(error)) throw error;
                
                // 指数退避：100ms, 200ms, 400ms, ...
                await sleep(100 * 2 ** (attempt - 1));
            }
        }
    }

    /**
     * ========================================================================
     * @OnLeaderStepdown - stopTracking()
     * ========================================================================
     * 
     * 当节点不再是 Leader 时自动调用，停止追踪
     * 
     * 清理工作：
     * 1. 清除主轮询定时器
     * 2. 清除所有等待执行的定时器
     * 3. 清空 waitingExecutions 映射
     * 
     * @remarks
     * 为什么需要清理？
     * - 避免多个节点同时轮询数据库
     * - 释放资源
     * - 防止定时器在错误的节点上触发
     */
    @OnLeaderStepdown()
    stopTracking() {
        if (!this.mainTimer) return;

        // 清除主轮询定时器
        clearInterval(this.mainTimer);
        
        // 清除所有等待执行的定时器
        Object.keys(this.waitingExecutions).forEach((executionId) => {
            clearTimeout(this.waitingExecutions[executionId].timer);
        });

        this.logger.debug('Stopped tracking waiting executions');
    }
}
