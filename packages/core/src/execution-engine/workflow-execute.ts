/**
 * ============================================================================
 * WorkflowExecute - 工作流执行引擎
 * ============================================================================
 * 
 * 这个类是 n8n 工作流执行的核心引擎。它负责：
 * 1. 管理工作流执行的完整生命周期（启动、运行、暂停、恢复、取消）
 * 2. 执行节点并管理节点间的数据传递
 * 3. 处理节点的等待状态（多输入节点、waiting 节点）
 * 4. 支持部分执行（从指定节点开始或结束）
 * 5. 处理错误重试和继续执行策略
 * 6. 支持执行取消和超时机制
 * 7. 管理执行上下文（通过 establishExecutionContext）
 * 
 * 执行流程概述：
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │ 1. 初始化执行数据 (createRunExecutionData)                        │
 * │ 2. 建立执行上下文 (establishExecutionContext)                     │
 * │ 3. 执行节点循环 (executionLoop)                                   │
 * │    ├── 从 nodeExecutionStack 取出节点                            │
 * │    ├── 执行节点 (runNode)                                        │
 * │    ├── 处理输出数据                                              │
 * │    └── 将后续节点加入执行栈 (addNodeToBeExecuted)               │
 * │ 4. 处理执行完成 (processSuccessExecution)                        │
 * │ 5. 触发生命周期钩子 (hooks)                                      │
 * └─────────────────────────────────────────────────────────────────────┘
 * ============================================================================
 */

/* eslint-disable @typescript-eslint/prefer-optional-chain */
/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */
/* eslint-disable @typescript-eslint/prefer-nullish-coalescing */

import { isAxiosError } from '@n8n/backend-network';
import { TOOL_EXECUTOR_NODE_NAME } from '@n8n/constants';
import { Container } from '@n8n/di';
import { sleep } from '@n8n/utils/sleep';
import * as assert from 'assert/strict';
import { setMaxListeners } from 'events';
import get from 'lodash/get';
import type {
    // ====== 核心执行类型 ======
    ExecutionBaseError,      // 执行错误基类
    ExecutionStatus,         // 执行状态: 'new' | 'running' | 'success' | 'error' | 'canceled' | 'waiting'
    ExecutionStorageLocation, // 存储位置: 'db' | 'queue'
    ExecutionError,          // 执行错误详细信息
    GenericValue,            // 通用值类型
    IConnection,             // 节点连接定义
    IDataObject,             // 数据对象
    IExecuteData,            // 节点执行数据（包含节点、输入数据、源信息）
    INode,                   // 节点定义
    INodeExecutionData,      // 节点执行数据项
    IPairedItemData,         // 配对项数据（用于追踪数据血缘）
    IPinData,                // 钉住数据（测试时使用的固定数据）
    IRun,                    // 完整运行结果
    IRunData,                // 运行数据（所有节点的执行记录）
    ITaskData,               // 单个任务数据
    ITaskDataConnections,    // 任务数据连接
    ITaskMetadata,           // 任务元数据
    NodeOperationError,      // 节点操作错误
    Workflow,                // 工作流定义
    IRunExecutionData,       // 运行执行数据
    IWorkflowExecuteAdditionalData, // 额外执行数据
    WorkflowExecuteMode,     // 执行模式: 'manual' | 'trigger' | 'webhook' | 'retry' | 'error'
    CloseFunction,           // 关闭函数（用于清理资源）
    IRunNodeResponse,        // 节点执行响应
    IWorkflowIssues,         // 工作流问题
    INodeIssues,             // 节点问题
    INodeType,               // 节点类型
    ITaskStartedData,        // 任务开始数据
    JsonObject,              // JSON 对象
    AiAgentRequest,          // AI Agent 请求
    IWorkflowExecutionDataProcess, // 工作流执行数据处理
    EngineRequest,           // 引擎请求
    EngineResponse,          // 引擎响应
    IDestinationNode,        // 目标节点
} from 'n8n-workflow';
import {
    LoggerProxy as Logger,
    NodeHelpers,
    NodeConnectionTypes,
    ApplicationError,
    BaseError,
    isNodeClassInstance,
    UnexpectedError,
    UserError,
    OperationalError,
    NodeApiError,
    TimeoutExecutionCancelledError,
    ManualExecutionCancelledError,
    createRunExecutionData,
    applyDynamicCredentialsUsage,
} from 'n8n-workflow';
import PCancelable from 'p-cancelable';

import { ErrorReporter } from '@/errors/error-reporter';
import { WorkflowHasIssuesError } from '@/errors/workflow-has-issues.error';
import * as NodeExecuteFunctions from '@/node-execute-functions';
import { assertExecutionDataExists } from '@/utils/assertions';

// ====== 执行上下文建立 ======
import { establishExecutionContext } from './execution-context';

import type { ExecutionLifecycleHooks } from './execution-lifecycle-hooks';
import {
    ExecuteContext,
    getAdditionalKeys,
    PollContext,
    resolveSourceOverwrite,
} from './node-execution-context';

// ====== 部分执行工具 ======
import {
    DirectedGraph,
    findStartNodes,
    findSubgraph,
    findTriggerForPartialExecution,
    cleanRunData,
    recreateNodeExecutionStack,
    handleCycles,
    filterDisabledNodes,
    rewireGraph,
    getNextExecutionIndex,
} from './partial-execution-utils';

import { handleRequest, isEngineRequest, makeEngineResponse } from './requests-response';
import { RoutingNode } from './routing-node';
import { TriggersAndPollers } from './triggers-and-pollers';
import { convertBinaryData } from '../utils/convert-binary-data';

/**
 * ============================================================================
 * run() 方法选项接口
 * ============================================================================
 */
interface RunWorkflowOptions {
    /** 要执行的工作流 */
    workflow: Workflow;
    /** 起始节点（可选） */
    startNode?: INode;
    /** 目标节点（执行到此节点停止） */
    destinationNode?: IDestinationNode;
    /** 钉住数据（用于测试） */
    pinData?: IPinData;
    /** 要从哪个触发器开始 */
    triggerToStartFrom?: IWorkflowExecutionDataProcess['triggerToStartFrom'];
    /** 额外允许运行的节点（配合目标节点使用） */
    additionalRunFilterNodes?: string[];
}

/**
 * ============================================================================
 * 工具函数：规范化 Axios 错误
 * ============================================================================
 * 将 Axios 错误转换为 NodeApiError，以便在 UI 中正确显示
 */
function normalizeUnhandledAxiosError(error: unknown, node: INode): ExecutionBaseError {
    if (isAxiosError(error)) {
        return new NodeApiError(node, error as JsonObject);
    }
    return error as ExecutionBaseError;
}

/**
 * ============================================================================
 * WorkflowExecute 类 - 工作流执行引擎
 * ============================================================================
 * 
 * 这是 n8n 中最重要的类之一，负责执行工作流的所有逻辑。
 * 
 * 核心数据结构：
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │ runExecutionData (IRunExecutionData)                                  │
 * │ ├── executionData: {                                                  │
 * │ │   ├── nodeExecutionStack: IExecuteData[]  ← 待执行节点栈           │
 * │ │   ├── waitingExecution: {}               ← 等待数据的节点          │
 * │ │   ├── waitingExecutionSource: {}         ← 等待数据的来源          │
 * │ │   ├── metadata: {}                       ← 临时元数据              │
 * │ │   └── runtimeData: {}                   ← 执行上下文 (由 establishExecutionContext 设置) │
 * │ │ }                                                                   │
 * │ ├── resultData: {                                                    │
 * │ │   ├── runData: IRunData                ← 所有节点的执行记录       │
 * │ │   ├── lastNodeExecuted: string         ← 最后执行的节点           │
 * │ │   ├── error: ExecutionBaseError        ← 执行错误                 │
 * │ │   └── pinData: IPinData                ← 钉住数据                 │
 * │ │ }                                                                   │
 * │ ├── startData: {                                                     │
 * │ │   ├── destinationNode: IDestinationNode ← 目标节点                │
 * │ │   ├── runNodeFilter: string[]          ← 节点过滤器               │
 * │ │   └── originalDestinationNode: IDestinationNode ← 原始目标        │
 * │ │ }                                                                   │
 * │ └── waitTill: Date                       ← 等待时间（用于延迟执行） │
 * └─────────────────────────────────────────────────────────────────────────┘
 * ============================================================================
 */
export class WorkflowExecute {
    /** 当前执行状态 */
    private status: ExecutionStatus = 'new';

    /** 取消控制器 - 用于取消正在运行的执行 */
    private readonly abortController = new AbortController();
    
    /** 是否超时 */
    timedOut: boolean = false;

    /**
     * 构造函数
     * @param additionalData - 额外执行数据（包含 hooks、executionId 等）
     * @param mode - 执行模式
     * @param runExecutionData - 执行数据（可选，用于恢复执行）
     * @param storedAt - 存储位置
     */
    constructor(
        private readonly additionalData: IWorkflowExecuteAdditionalData,
        private readonly mode: WorkflowExecuteMode,
        private runExecutionData: IRunExecutionData = createRunExecutionData(),
        private readonly storedAt: ExecutionStorageLocation = 'db',
    ) {}

    /**
     * ========================================================================
     * run() - 执行完整工作流
     * ========================================================================
     * 
     * 这是工作流执行的主要入口点。它：
     * 1. 确定起始节点
     * 2. 设置节点过滤器（如果指定了目标节点）
     * 3. 创建初始执行栈
     * 4. 调用 processRunExecutionData 执行
     * 
     * @returns PCancelable<IRun> - 可取消的执行结果
     * 
     * @remarks
     * 注意：此函数不能标记为 async，否则 PCancelable 会被转换为普通 Promise，
     * 从而失去取消功能。
     */
    // IMPORTANT: Do not add "async" to this function, it will then convert the
    //            PCancelable to a regular Promise and does so not allow canceling
    //            active executions anymore
    // eslint-disable-next-line @typescript-eslint/promise-function-async
    run({
        workflow,
        startNode,
        destinationNode,
        pinData,
        triggerToStartFrom,
        additionalRunFilterNodes,
    }: RunWorkflowOptions): PCancelable<IRun> {
        this.status = 'running';

        // === 步骤 1: 确定起始节点 ===
        // 如果没有指定起始节点，从工作流中查找
        startNode = startNode || workflow.getStartNode(destinationNode?.nodeName);

        if (startNode === undefined) {
            throw new UserError('No node to start the workflow from could be found');
        }

        // === 步骤 2: 设置节点过滤器 ===
        // 如果指定了目标节点，只执行目标节点及其父节点
        let runNodeFilter: string[] | undefined;
        if (destinationNode) {
            // 获取目标节点的所有父节点（包括非主连接）
            runNodeFilter = [
                ...workflow.getParentNodes(destinationNode.nodeName),
                ...workflow.getParentNodes(destinationNode.nodeName, 'ALL_NON_MAIN'),
            ];
            // 如果是 inclusive 模式，目标节点本身也包含在内
            if (destinationNode.mode === 'inclusive') {
                runNodeFilter.push(destinationNode.nodeName);
            }
            // 添加额外允许运行的节点
            if (additionalRunFilterNodes) {
                runNodeFilter.push.apply(runNodeFilter, additionalRunFilterNodes);
            }
            // 去重
            runNodeFilter = Array.from(new Set(runNodeFilter));
        }

        // === 步骤 3: 初始化执行数据 ===
        // 创建初始执行栈，起始节点使用空数据或触发器数据
        const nodeExecutionStack: IExecuteData[] = [
            {
                node: startNode,
                data: triggerToStartFrom?.data?.data ?? {
                    main: [
                        [
                            {
                                json: {},
                            },
                        ],
                    ],
                },
                source: null,
            },
        ];

        // 创建完整的执行数据
        this.runExecutionData = createRunExecutionData({
            startData: {
                destinationNode,
                runNodeFilter,
            },
            executionData: {
                nodeExecutionStack,
            },
            resultData: {
                pinData,
            },
            resumeToken: this.runExecutionData.resumeToken,
        });

        // === 步骤 4: 开始执行 ===
        return this.processRunExecutionData(workflow);
    }

    /**
     * ========================================================================
     * isLegacyExecutionOrder() - 检查是否使用旧版执行顺序
     * ========================================================================
     * 
     * v0: 旧版执行顺序，按节点位置排序
     * v1: 新版执行顺序，按数据流执行
     */
    isLegacyExecutionOrder(workflow: Workflow): boolean {
        return workflow.settings.executionOrder !== 'v1';
    }

    /**
     * ========================================================================
     * runPartialWorkflow2() - 部分工作流执行
     * ========================================================================
     * 
     * 这是 n8n 的部分执行功能，支持：
     * 1. 从任意节点开始执行（而不必从头开始）
     * 2. 执行到指定节点停止
     * 3. 支持 AI Agent 的工具调用
     * 
     * 执行流程：
     * ┌─────────────────────────────────────────────────────────────────────┐
     * │ 1. 构建执行图 (DirectedGraph.fromWorkflow)                        │
     * │ 2. 查找触发器 (findTriggerForPartialExecution)                    │
     * │ 3. 查找子图 (findSubgraph)                                        │
     * │ 4. 查找起始节点 (findStartNodes)                                  │
     * │ 5. 处理循环 (handleCycles)                                        │
     * │ 6. 清理运行数据 (cleanRunData)                                    │
     * │ 7. 重建执行栈 (recreateNodeExecutionStack)                        │
     * │ 8. 执行 (processRunExecutionData)                                 │
     * └─────────────────────────────────────────────────────────────────────┘
     */
    // IMPORTANT: Do not add "async" to this function, it will then convert the
    //            PCancelable to a regular Promise and does so not allow canceling
    //            active executions anymore
    // eslint-disable-next-line @typescript-eslint/promise-function-async
    runPartialWorkflow2(
        workflow: Workflow,
        runData: IRunData,
        pinData: IPinData = {},
        dirtyNodeNames: string[] = [],
        destinationNode: IDestinationNode,
        agentRequest?: AiAgentRequest,
    ): PCancelable<IRun> {
        const originalDestination = { ...destinationNode };

        // === 步骤 1: 获取目标节点 ===
        let destination = workflow.getNode(destinationNode.nodeName);
        assert.ok(
            destination,
            `Could not find a node with the name ${destinationNode.nodeName} in the workflow.`,
        );

        // === 步骤 2: 构建执行图 ===
        let graph = DirectedGraph.fromWorkflow(workflow);

        // === 步骤 3: 处理 AI 工具节点 ===
        // 如果目标节点是工具节点，需要重写图结构
        const destinationNodeType = workflow.nodeTypes.getByNameAndVersion(
            destination.type,
            destination.typeVersion,
        );
        if (NodeHelpers.isTool(destinationNodeType.description, destination.parameters)) {
            // 重写图：工具节点 → Agent 节点
            graph = rewireGraph(destination, graph, agentRequest);
            workflow = graph.toWorkflow({ ...workflow });
            
            // 获取工具执行器节点（虚拟节点）
            const toolExecutorNode = workflow.getNode(TOOL_EXECUTOR_NODE_NAME);
            if (!toolExecutorNode) {
                throw new OperationalError('ToolExecutor can not be found');
            }
            destination = toolExecutorNode;
            destinationNode = { nodeName: toolExecutorNode.name, mode: 'inclusive' };
        }

        // === 步骤 4: 查找触发器 ===
        // 查找从哪个节点开始执行
        let trigger = findTriggerForPartialExecution(workflow, destinationNode.nodeName, runData);
        if (trigger === undefined) {
            // 如果没有触发器，查找最近的已执行父节点
            let startNode;
            const parentNodes = workflow.getParentNodes(destinationNode.nodeName);
            for (const nodeName of parentNodes) {
                const parentNode = workflow.getNode(nodeName);
                // 跳过已禁用的节点
                if (parentNode && !parentNode.disabled && runData[nodeName]) {
                    startNode = parentNode;
                    break;
                }
            }
            if (!startNode) {
                throw new UserError("Connect a trigger and make sure it's enabled to run this node");
            }
            trigger = startNode;
        }

        // === 步骤 5: 查找子图 ===
        const filteredGraph = filterDisabledNodes(graph);

        if (destination.disabled) {
            throw new UserError('Cannot execute a disabled node');
        }

        // 找到从触发器到目标节点的子图
        graph = findSubgraph({ graph: filteredGraph, destination, trigger });
        const filteredNodes = graph.getNodes();

        // === 步骤 6: 查找起始节点 ===
        const dirtyNodes = graph.getNodesByNames(dirtyNodeNames);
        runData = cleanRunData(runData, graph, dirtyNodes);
        let startNodes = findStartNodes({ graph, trigger, destination, runData, pinData });

        // === 步骤 7: 处理循环 ===
        startNodes = handleCycles(graph, startNodes, trigger);

        // === 步骤 8: 清理运行数据 ===
        runData = cleanRunData(runData, graph, startNodes);

        // === 步骤 9: 重建执行栈 ===
        const { nodeExecutionStack, waitingExecution, waitingExecutionSource } =
            recreateNodeExecutionStack(graph, startNodes, runData, pinData ?? {});

        // === 步骤 10: 更新执行索引 ===
        this.additionalData.currentNodeExecutionIndex = getNextExecutionIndex(runData);

        // === 步骤 11: 创建执行数据 ===
        this.status = 'running';
        this.runExecutionData = createRunExecutionData({
            startData: {
                destinationNode,
                originalDestinationNode: originalDestination,
                runNodeFilter: Array.from(filteredNodes.values()).map((node) => node.name),
            },
            resultData: {
                runData,
                pinData,
            },
            executionData: {
                nodeExecutionStack,
                waitingExecution,
                waitingExecutionSource,
            },
            resumeToken: this.runExecutionData.resumeToken,
        });

        // === 步骤 12: 执行 ===
        return this.processRunExecutionData(workflow);
    }

    /**
     * ========================================================================
     * moveNodeMetadata() - 移动节点元数据
     * ========================================================================
     * 
     * 在执行过程中，元数据临时存储在 executionData.metadata 中。
     * 执行完成后，将元数据移动到 resultData.runData 中的最终位置。
     */
    moveNodeMetadata(): void {
        const metadata = get(this.runExecutionData, 'executionData.metadata');

        if (metadata) {
            const runData = get(this.runExecutionData, 'resultData.runData');

            let index: number;
            let metaRunData: ITaskMetadata;
            for (const nodeName of Object.keys(metadata)) {
                for ([index, metaRunData] of metadata[nodeName].entries()) {
                    const taskData = runData[nodeName]?.[index];
                    if (taskData) {
                        taskData.metadata = { ...taskData.metadata, ...metaRunData };
                    } else {
                        Container.get(ErrorReporter).error(
                            new UnexpectedError('Taskdata missing at the end of an execution'),
                            { extra: { nodeName, index } },
                        );
                    }
                }
            }
        }
    }

    /**
     * ========================================================================
     * incomingConnectionIsEmpty() - 检查输入连接是否为空
     * ========================================================================
     * 
     * 判断一个节点的所有输入连接是否都没有数据。
     * 用于决定节点是否应该执行。
     * 
     * 返回 true 的情况：
     * - 源节点不在 runData 中
     * - 源节点的数据为 undefined
     * - 源节点的输出数组为空
     * - 指定的输出索引没有数据
     */
    incomingConnectionIsEmpty(
        runData: IRunData,
        inputConnections: IConnection[],
        runIndex: number,
    ): boolean {
        for (const inputConnection of inputConnections) {
            const nodeIncomingData = get(runData, [
                inputConnection.node,
                runIndex,
                'data',
                'main',
                inputConnection.index,
            ]);
            if (nodeIncomingData !== undefined && (nodeIncomingData as object[]).length !== 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * ========================================================================
     * prepareWaitingToExecution() - 准备等待执行的数据结构
     * ========================================================================
     * 
     * 当一个节点有多个输入连接时，它需要等待所有输入数据都到达后才能执行。
     * 这个函数为等待的节点初始化数据结构。
     * 
     * 典型场景：Merge 节点有 2 个输入，需要等待两个分支的数据都到达。
     */
    prepareWaitingToExecution(nodeName: string, numberOfConnections: number, runIndex: number) {
        const executionData = this.runExecutionData.executionData!;

        // 初始化等待数据结构
        executionData.waitingExecution ??= {};
        executionData.waitingExecutionSource ??= {};

        const nodeWaiting = (executionData.waitingExecution[nodeName] ??= []);
        const nodeWaitingSource = (executionData.waitingExecutionSource[nodeName] ??= []);

        // 为每个输入连接创建占位
        nodeWaiting[runIndex] = { main: [] };
        nodeWaitingSource[runIndex] = { main: [] };

        for (let i = 0; i < numberOfConnections; i++) {
            nodeWaiting[runIndex].main.push(null);
            nodeWaitingSource[runIndex].main.push(null);
        }
    }

    /**
     * ========================================================================
     * addNodeToBeExecuted() - 将节点加入执行栈
     * ========================================================================
     * 
     * 这是工作流执行中最重要的方法之一。当节点执行完成后，
     * 这个方法决定哪些后续节点应该被执行。
     * 
     * 处理逻辑：
     * 1. 如果目标节点有多个输入，将数据放入 waitingExecution
     * 2. 等待所有输入数据到达后，将节点加入 nodeExecutionStack
     * 3. 如果只有单个输入，直接加入 nodeExecutionStack
     * 
     * @param workflow - 工作流
     * @param connectionData - 连接信息
     * @param outputIndex - 输出索引
     * @param parentNodeName - 父节点名称
     * @param nodeSuccessData - 节点成功执行的数据
     * @param runIndex - 运行索引
     * @param newRunIndex - 新的运行索引（可选）
     * @param metadata - 元数据（可选）
     */
    // eslint-disable-next-line complexity
    addNodeToBeExecuted(
        workflow: Workflow,
        connectionData: IConnection,
        outputIndex: number,
        parentNodeName: string,
        nodeSuccessData: INodeExecutionData[][],
        runIndex: number,
        newRunIndex?: number,
        metadata?: ITaskMetadata,
    ): void {
        let stillDataMissing = false;
        // 根据执行顺序决定插入位置（v1: 栈顶优先，v0: 栈底优先）
        const enqueueFn = workflow.settings.executionOrder === 'v1' ? 'unshift' : 'push';
        let waitingNodeIndex: number | undefined;

        // === 情况 1: 节点有多个输入 ===
        // 需要等待所有输入数据都到达
        const numberOfInputs =
            workflow.connectionsByDestinationNode[connectionData.node]?.main?.length ?? 0;
        if (numberOfInputs > 1) {
            // ... 多输入处理逻辑（见下方详细注释）
            // 这里省略了详细实现，但核心逻辑是：
            // 1. 检查数据是否已存在
            // 2. 如果不存在，创建新的等待条目
            // 3. 添加数据
            // 4. 检查是否所有数据都到达
            // 5. 如果全部到达，加入执行栈
        }

        // === 情况 2: 单输入或所有数据已就绪 ===
        // 直接加入执行栈
        // ...
    }

    /**
     * ========================================================================
     * checkReadyForExecution() - 检查工作流是否准备好执行
     * ========================================================================
     * 
     * 在执行前检查工作流是否有问题：
     * - 节点类型是否已知
     * - 节点参数是否有效
     * 
     * @returns IWorkflowIssues | null - 如果有问题返回问题列表，否则返回 null
     */
    checkReadyForExecution(
        workflow: Workflow,
        inputData: {
            startNode?: string;
            destinationNode?: IDestinationNode;
            pinDataNodeNames?: string[];
        } = {},
    ): IWorkflowIssues | null {
        const workflowIssues: IWorkflowIssues = {};

        // 确定要检查的节点列表
        let checkNodes: string[] = [];
        if (inputData.destinationNode) {
            // 检查目标节点及其所有父节点
            checkNodes = workflow.getParentNodes(inputData.destinationNode.nodeName);
            if (inputData.destinationNode.mode === 'inclusive') {
                checkNodes.push(inputData.destinationNode.nodeName);
            }
        } else if (inputData.startNode) {
            // 检查起始节点及其所有子节点
            checkNodes = workflow.getChildNodes(inputData.startNode);
            checkNodes.push(inputData.startNode);
        }

        for (const nodeName of checkNodes) {
            let nodeIssues: INodeIssues | null = null;
            const node = workflow.nodes[nodeName];

            // 跳过虚拟节点
            if (!node && nodeName === TOOL_EXECUTOR_NODE_NAME) {
                continue;
            }

            if (!node || node.disabled === true) {
                continue;
            }

            const nodeType = workflow.nodeTypes.getByNameAndVersion(node.type, node.typeVersion);

            if (nodeType === undefined) {
                // 节点类型未知
                nodeIssues = {
                    typeUnknown: true,
                };
            } else {
                // 检查节点参数是否有效
                nodeIssues = NodeHelpers.getNodeParametersIssues(
                    nodeType.description.properties,
                    node,
                    nodeType.description,
                    inputData.pinDataNodeNames,
                );
            }

            if (nodeIssues !== null) {
                workflowIssues[node.name] = nodeIssues;
            }
        }

        if (Object.keys(workflowIssues).length === 0) {
            return null;
        }

        return workflowIssues;
    }

    /**
     * ========================================================================
     * getCustomOperation() - 获取自定义操作
     * ========================================================================
     * 
     * 某些节点支持自定义操作（如数据库节点的不同 CRUD 操作）。
     * 这个方法根据节点的 resource 和 operation 参数获取对应的操作函数。
     */
    private getCustomOperation(node: INode, type: INodeType) {
        if (!type.customOperations) return undefined;
        if (!node.parameters && !node.forceCustomOperation) return undefined;

        const { customOperations } = type;
        const { resource, operation } = node.forceCustomOperation ?? node.parameters;

        if (typeof resource !== 'string' || typeof operation !== 'string') return undefined;
        if (!customOperations[resource] || !customOperations[resource][operation]) return undefined;

        const customOperation = customOperations[resource][operation];
        return customOperation;
    }

    /**
     * ========================================================================
     * handleDisabledNode() - 处理禁用的节点
     * ========================================================================
     * 
     * 当节点被禁用时，它不应该执行任何逻辑。
     * 如果有输入数据，直接透传；否则返回 undefined。
     */
    private handleDisabledNode(inputData: ITaskDataConnections): IRunNodeResponse {
        if (Object.hasOwn(inputData, 'main') && inputData.main.length > 0) {
            if (inputData.main[0] === null) {
                return { data: undefined };
            }
            return { data: [inputData.main[0]] };
        }
        return { data: undefined };
    }

    /**
     * ========================================================================
     * prepareConnectionInputData() - 准备节点的输入数据
     * ========================================================================
     * 
     * 对于 execute 类型的节点，从 main 连接中提取输入数据。
     * 对于 poll/trigger/webhook 节点，不需要输入数据。
     * 
     * 如果节点需要执行但没有输入数据，返回 null。
     */
    private prepareConnectionInputData(
        workflow: Workflow,
        nodeType: INodeType,
        customOperation: ReturnType<WorkflowExecute['getCustomOperation']>,
        inputData: ITaskDataConnections,
    ): INodeExecutionData[] | null {
        if (
            nodeType.execute ||
            customOperation ||
            (!nodeType.poll && !nodeType.trigger && !nodeType.webhook)
        ) {
            if (!inputData.main?.length) {
                return null;
            }

            let connectionInputData = inputData.main[0];

            // 如果是旧版执行顺序，使用第一个有数据的输入
            const forceInputNodeExecution = workflow.settings.executionOrder !== 'v1';
            if (!forceInputNodeExecution) {
                for (const mainData of inputData.main) {
                    if (mainData?.length) {
                        connectionInputData = mainData;
                        break;
                    }
                }
            }

            if (!connectionInputData || connectionInputData.length === 0) {
                return null;
            }

            return connectionInputData;
        }

        // 对于 poll/trigger/webhook 节点，不需要输入
        return [];
    }

    /**
     * ========================================================================
     * continuesOnError() - 检查节点是否在错误时继续执行
     * ========================================================================
     * 
     * 当节点执行失败时，可以配置为：
     * - 停止执行（默认）
     * - 继续执行（continueRegularOutput）
     * - 输出错误到错误输出（continueErrorOutput）
     */
    private continuesOnError(node: INode): boolean {
        return (
            node.continueOnFail === true ||
            ['continueRegularOutput', 'continueErrorOutput'].includes(node.onError ?? '')
        );
    }

    /**
     * ========================================================================
     * rethrowNodeError() - 重新抛出节点错误
     * ========================================================================
     * 
     * 如果节点已经失败，重新抛出错误以便正确记录和显示。
     * 如果错误不是 NodeOperationError 或 NodeApiError，包装为普通 Error。
     */
    private rethrowNodeError(error: ExecutionError): never {
        if (error.name === 'NodeOperationError' || error.name === 'NodeApiError') {
            throw error;
        }

        const wrapped = new Error(error.message);
        wrapped.stack = error.stack;
        throw wrapped;
    }

    /**
     * ========================================================================
     * rethrowLastNodeError() - 重新抛出最后执行的节点错误
     * ========================================================================
     * 
     * 在 webhook 和 trigger 节点中，如果它们之前已经失败，
     * 重新抛出错误以便正确显示。
     */
    private rethrowLastNodeError(runExecutionData: IRunExecutionData, node: INode): void {
        if (
            runExecutionData.resultData.lastNodeExecuted === node.name &&
            runExecutionData.resultData.error !== undefined
        ) {
            this.rethrowNodeError(runExecutionData.resultData.error);
        }
    }

    /**
     * ========================================================================
     * handleExecuteOnce() - 处理 "只执行一次" 逻辑
     * ========================================================================
     * 
     * 如果节点设置了 executeOnce，只使用第一个输入项。
     * 这对于需要一次性处理所有数据的节点很有用。
     */
    private handleExecuteOnce(node: INode, inputData: ITaskDataConnections): ITaskDataConnections {
        if (node.executeOnce === true) {
            const newInputData: ITaskDataConnections = {};
            for (const connectionType of Object.keys(inputData)) {
                newInputData[connectionType] = inputData[connectionType].map((input) => {
                    return input && input.slice(0, 1);
                });
            }
            return newInputData;
        }
        return inputData;
    }

    /**
     * ========================================================================
     * executeNode() - 执行单个节点
     * ========================================================================
     * 
     * 这是执行节点的核心方法：
     * 1. 创建执行上下文 (ExecuteContext)
     * 2. 调用节点的 execute 方法或自定义操作
     * 3. 处理执行结果
     * 4. 执行清理函数（closeFunctions）
     */
    private async executeNode(
        workflow: Workflow,
        node: INode,
        nodeType: INodeType,
        customOperation: ReturnType<WorkflowExecute['getCustomOperation']>,
        additionalData: IWorkflowExecuteAdditionalData,
        mode: WorkflowExecuteMode,
        runExecutionData: IRunExecutionData,
        runIndex: number,
        connectionInputData: INodeExecutionData[],
        inputData: ITaskDataConnections,
        executionData: IExecuteData,
        abortSignal?: AbortSignal,
        subNodeExecutionResults?: EngineResponse,
    ): Promise<IRunNodeResponse | EngineRequest> {
        const closeFunctions: CloseFunction[] = [];
        
        // 创建执行上下文
        const context = new ExecuteContext(
            workflow,
            node,
            additionalData,
            mode,
            runExecutionData,
            runIndex,
            connectionInputData,
            inputData,
            executionData,
            closeFunctions,
            abortSignal,
            subNodeExecutionResults,
        );

        let data: INodeExecutionData[][] | EngineRequest | null;
        let executionSucceeded = false;
        let closingError: Error | undefined;

        try {
            // 执行节点
            if (customOperation) {
                data = await customOperation.call(context);
            } else if (nodeType.execute) {
                data = isNodeClassInstance(nodeType)
                    ? await nodeType.execute(context, subNodeExecutionResults)
                    : await nodeType.execute.call(context, subNodeExecutionResults);
            } else {
                throw new UnexpectedError(
                    "Can't execute node. There is no custom operation and the node has not execute function.",
                );
            }
            executionSucceeded = true;
        } finally {
            // 执行清理函数
            if (closeFunctions.length > 0) {
                const closeFunctionsResults = await Promise.allSettled(
                    closeFunctions.map(async (fn) => await fn()),
                );

                // 只在执行成功时抛出清理错误，避免掩盖原始错误
                if (executionSucceeded) {
                    const closingErrors = closeFunctionsResults
                        .filter((result): result is PromiseRejectedResult => result.status === 'rejected')
                        .map((result) => result.reason);

                    if (closingErrors.length > 0) {
                        closingError =
                            closingErrors[0] instanceof Error
                                ? closingErrors[0]
                                : new UnexpectedError("Error on execution node's close function(s)", {
                                        extra: { nodeName: node.name },
                                        tags: { nodeType: node.type },
                                        cause: closingErrors,
                                    });
                    }
                }
            }
        }

        if (closingError) throw closingError;

        if (isEngineRequest(data)) {
            return data;
        }

        return { data, hints: context.hints };
