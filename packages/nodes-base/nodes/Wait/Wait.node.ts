/**
 * ============================================================================
 * Wait 节点 - 工作流等待节点
 * ============================================================================
 * 
 * 这个节点允许工作流在继续执行之前等待特定条件：
 * 
 * 支持的等待模式：
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │ 1. After Time Interval (时间间隔)                                      │
 * │    - 等待指定的时间长度（秒/分钟/小时/天）                             │
 * │    - 适用于定时延迟场景                                                │
 * │                                                                         │
 * │ 2. At Specified Time (指定时间)                                        │
 * │    - 等待直到指定的日期和时间                                          │
 * │    - 适用于定时任务（如下午3点执行）                                   │
 * │                                                                         │
 * │ 3. On Webhook Call (Webhook 回调)                                     │
 * │    - 等待外部系统通过 Webhook 触发恢复                                 │
 * │    - 适用于异步审批、外部系统集成等                                    │
 * │    - URL: {{ $execution.resumeUrl }}                                  │
 * │                                                                         │
 * │ 4. On Form Submitted (表单提交)                                        │
 * │    - 等待用户提交表单后恢复执行                                        │
 * │    - 适用于用户交互工作流（审批、信息收集等）                          │
 * │    - URL: {{ $execution.resumeFormUrl }}                              │
 * └─────────────────────────────────────────────────────────────────────────┘
 * 
 * 等待机制：
 * - 短等待（< 65秒）：使用内存定时器
 * - 长等待（≥ 65秒）：持久化到数据库，由后台任务恢复
 * - 无限等待（Webhook/Form）：等待外部事件触发
 * 
 * 重要概念：
 * - $execution.resumeUrl: Webhook 恢复 URL（在运行时生成）
 * - $execution.resumeFormUrl: 表单恢复 URL（在运行时生成）
 * - waitTill: 等待截止时间，用于限制最大等待时长
 * ============================================================================
 */

import type {
    IExecuteFunctions,
    INodeExecutionData,
    INodeTypeDescription,
    INodeProperties,
    IDisplayOptions,
    IWebhookFunctions,
} from 'n8n-workflow';
import {
    NodeConnectionTypes,
    WAIT_INDEFINITELY,           // 无限等待的标记值
    FORM_TRIGGER_NODE_TYPE,      // 表单触发器节点类型
    tryToParseDateTime,
    NodeOperationError,
} from 'n8n-workflow';

import { validateWaitAmount, validateWaitUnit } from './validation';
import { updateDisplayOptions } from '../../utils/utilities';
import {
    formDescription,
    formFields,
    respondWithOptions,
    formRespondMode,
    formTitle,
    appendAttributionToForm,
} from '../Form/common.descriptions';
import { formWebhook } from '../Form/utils/utils';
import {
    authenticationProperty,
    credentialsProperty,
    defaultWebhookDescription,
    httpMethodsProperty,
    optionsProperty,
    responseBinaryPropertyNameProperty,
    responseCodeProperty,
    responseDataProperty,
    responseModeProperty,
} from '../Webhook/description';
import { Webhook } from '../Webhook/Webhook.node';

/**
 * ============================================================================
 * 属性定义 - Wait Amount (等待时间)
 * ============================================================================
 */
const toWaitAmount: INodeProperties = {
    displayName: 'Wait Amount',
    name: 'amount',
    type: 'number',
    typeOptions: {
        minValue: 0,
        numberPrecision: 2,
    },
    default: 1,
    description: 'The time to wait',
    validateType: 'number',
};

/**
 * ============================================================================
 * 属性定义 - Unit Selector (时间单位选择器)
 * ============================================================================
 */
const unitSelector: INodeProperties = {
    displayName: 'Wait Unit',
    name: 'unit',
    type: 'options',
    options: [
        { name: 'Seconds', value: 'seconds' },
        { name: 'Minutes', value: 'minutes' },
        { name: 'Hours', value: 'hours' },
        { name: 'Days', value: 'days' },
    ],
    default: 'hours',
    description: 'The time unit of the Wait Amount value',
};

/**
 * ============================================================================
 * 属性定义 - Wait Time Properties (等待时间限制属性)
 * ============================================================================
 * 
 * 用于 Webhook 和 Form 模式，限制最大等待时间：
 * - 如果不限制：无限等待（直到外部事件触发）
 * - 如果限制：在指定时间后自动恢复（即使没有收到 Webhook/表单提交）
 * 
 * 限制类型：
 * 1. After Time Interval: 等待指定时长后恢复
 * 2. At Specified Time: 在指定日期时间恢复
 */
const waitTimeProperties: INodeProperties[] = [
    {
        displayName: 'Limit Wait Time',
        name: 'limitWaitTime',
        type: 'boolean',
        default: false,
        description:
            'Whether to limit the time this node should wait for a user response before execution resumes',
        displayOptions: {
            show: {
                resume: ['webhook', 'form'],
            },
        },
    },
    {
        displayName: 'Limit Type',
        name: 'limitType',
        type: 'options',
        default: 'afterTimeInterval',
        description:
            'Sets the condition for the execution to resume. Can be a specified date or after some time.',
        displayOptions: {
            show: {
                limitWaitTime: [true],
                resume: ['webhook', 'form'],
            },
        },
        options: [
            {
                name: 'After Time Interval',
                description: 'Waits for a certain amount of time',
                value: 'afterTimeInterval',
            },
            {
                name: 'At Specified Time',
                description: 'Waits until the set date and time to continue',
                value: 'atSpecifiedTime',
            },
        ],
    },
    {
        displayName: 'Amount',
        name: 'resumeAmount',
        type: 'number',
        displayOptions: {
            show: {
                limitType: ['afterTimeInterval'],
                limitWaitTime: [true],
                resume: ['webhook', 'form'],
            },
        },
        typeOptions: {
            minValue: 0,
            numberPrecision: 2,
        },
        default: 1,
        description: 'The time to wait',
    },
    {
        displayName: 'Unit',
        name: 'resumeUnit',
        type: 'options',
        displayOptions: {
            show: {
                limitType: ['afterTimeInterval'],
                limitWaitTime: [true],
                resume: ['webhook', 'form'],
            },
        },
        options: [
            { name: 'Seconds', value: 'seconds' },
            { name: 'Minutes', value: 'minutes' },
            { name: 'Hours', value: 'hours' },
            { name: 'Days', value: 'days' },
        ],
        default: 'hours',
        description: 'Unit of the interval value',
    },
    {
        displayName: 'Max Date and Time',
        name: 'maxDateAndTime',
        type: 'dateTime',
        displayOptions: {
            show: {
                limitType: ['atSpecifiedTime'],
                limitWaitTime: [true],
                resume: ['webhook', 'form'],
            },
        },
        default: '',
        description: 'Continue execution after the specified date and time',
    },
];

/**
 * ============================================================================
 * 属性定义 - Webhook Suffix (Webhook 后缀)
 * ============================================================================
 * 
 * 当工作流中有多个 Wait 节点时，每个节点需要不同的 Webhook 路径。
 * 后缀帮助区分不同节点。
 * 
 * 示例：
 * - 基础 URL: https://n8n.example.com/webhook/execution/123
 * - 带后缀: https://n8n.example.com/webhook/execution/123/approval
 */
const webhookSuffix: INodeProperties = {
    displayName: 'Webhook Suffix',
    name: 'webhookSuffix',
    type: 'string',
    default: '',
    placeholder: 'webhook',
    noDataExpression: true,
    description:
        'This suffix path will be appended to the restart URL. Helpful when using multiple wait nodes.',
};

/**
 * ============================================================================
 * 显示选项配置
 * ============================================================================
 */
const displayOnWebhook: IDisplayOptions = {
    show: { resume: ['webhook'] },
};

const displayOnFormSubmission = {
    show: { resume: ['form'] },
};

/**
 * ============================================================================
 * 合并表单提交属性
 * ============================================================================
 */
const onFormSubmitProperties = updateDisplayOptions(displayOnFormSubmission, [
    formTitle,
    formDescription,
    formFields,
    formRespondMode,
]);

/**
 * ============================================================================
 * 合并 Webhook 调用属性
 * ============================================================================
 */
const onWebhookCallProperties = updateDisplayOptions(displayOnWebhook, [
    {
        ...httpMethodsProperty,
        description: 'The HTTP method of the Webhook call',
    },
    responseCodeProperty,
    responseModeProperty,
    responseDataProperty,
    responseBinaryPropertyNameProperty,
]);

/**
 * ============================================================================
 * Webhook 路径表达式
 * ============================================================================
 * 
 * 在运行时计算 Webhook 路径，基于 options.webhookSuffix 参数
 * 如果后缀为空，路径就是空字符串
 */
const webhookPath = '={{$parameter["options"]["webhookSuffix"] || ""}}';

/**
 * ============================================================================
 * waitingTooltip - 生成等待提示信息
 * ============================================================================
 * 
 * 这是一个在 UI 中显示的动态提示，告诉用户如何恢复工作流。
 * 
 * @param parameters - 节点参数
 * @param resumeUrl - Webhook 恢复 URL（来自 $execution.resumeUrl）
 * @param formResumeUrl - 表单恢复 URL（来自 $execution.resumeFormUrl）
 * @returns HTML 格式的提示信息
 */
const waitingTooltip = (
    parameters: { resume: string; options?: Record<string, string> },
    resumeUrl: string,
    formResumeUrl: string,
) => {
    const resume = parameters.resume;

    if (['webhook', 'form'].includes(resume)) {
        const { webhookSuffix } = (parameters.options ?? {}) as { webhookSuffix: string };
        const suffix = webhookSuffix && typeof webhookSuffix !== 'object' ? `/${webhookSuffix}` : '';

        let message = '';
        const baseUrl = resume === 'form' ? formResumeUrl : resumeUrl;

        // 在查询参数之前插入后缀（适用于包含 ?signature=token 的 URL）
        // 注意：不能使用 URL 类，因为它在表达式环境中不可用
        let url: string;
        const queryIndex = baseUrl.indexOf('?');
        if (queryIndex !== -1) {
            url = baseUrl.slice(0, queryIndex) + suffix + baseUrl.slice(queryIndex);
        } else {
            url = baseUrl + suffix;
        }

        if (resume === 'form') {
            message = 'Execution will continue when form is submitted on ';
        }

        if (resume === 'webhook') {
            message = 'Execution will continue when webhook is received on ';
        }

        return `${message}<a href="${url}" target="_blank">${url}</a>`;
    }

    return 'Execution will continue when wait time is over';
};

/**
 * ============================================================================
 * Wait 节点类
 * ============================================================================
 * 
 * 继承自 Webhook 节点，复用了 Webhook 的认证、响应等功能。
 * 
 * 节点版本：
 * - v1: 初始版本，默认单位为 'hours'
 * - v1.1: 更新默认单位为 'seconds'，更直观
 */
export class Wait extends Webhook {
    /** 认证属性名称，用于获取认证配置 */
    authPropertyName = 'incomingAuthentication';

    /**
     * ========================================================================
     * 节点描述
     * ========================================================================
     * 
     * 定义了节点的所有元数据、输入输出、Webhook 配置和属性。
     * 
     * 关键配置：
     * 1. waitingNodeTooltip: 使用动态表达式生成提示信息
     * 2. webhooks: 配置了三个 Webhook 端点
     *    - GET 请求（表单页面）
     *    - POST 请求（表单提交）
     *    - 通用 Webhook 请求
     * 3. restartWebhook: true 表示这个 Webhook 用于恢复等待的执行
     */
    description: INodeTypeDescription = {
        displayName: 'Wait',
        name: 'wait',
        icon: 'node:wait',
        iconColor: 'crimson',
        group: ['organization'],
        version: [1, 1.1],
        description: 'Wait before continue with execution',
        defaults: {
            name: 'Wait',
        },
        /** 输入：一个主连接（接收数据） */
        inputs: [NodeConnectionTypes.Main],
        /** 输出：一个主连接（传递数据） */
        outputs: [NodeConnectionTypes.Main],
        credentials: credentialsProperty(this.authPropertyName),
        /** 
         * 等待节点的工具提示
         * 使用动态表达式，在运行时生成包含 URL 的提示
         */
        waitingNodeTooltip: `={{ (${waitingTooltip})($parameter, $execution.resumeUrl, $execution.resumeFormUrl) }}`,
        
        /**
         * Webhook 配置
         * 注意：restartWebhook: true 表示这是恢复执行的关键 Webhook
         */
        webhooks: [
            // ===== Webhook 1: 通用 Webhook =====
            {
                ...defaultWebhookDescription,
                responseData: '={{$parameter["responseData"]}}',
                path: webhookPath,
                restartWebhook: true,
            },
            // ===== Webhook 2: 表单 GET 请求 =====
            // 用于显示表单页面
            {
                name: 'default',
                httpMethod: 'GET',
                responseMode: 'onReceived',
                path: webhookPath,
                restartWebhook: true,
                isFullPath: true,
                nodeType: 'form',
            },
            // ===== Webhook 3: 表单 POST 请求 =====
            // 用于接收表单提交数据
            {
                name: 'default',
                httpMethod: 'POST',
                responseMode: '={{$parameter["responseMode"]}}',
                responseData: '={{$parameter["responseMode"] === "lastNode" ? "noData" : undefined}}',
                path: webhookPath,
                restartWebhook: true,
                isFullPath: true,
                nodeType: 'form',
            },
        ],
        
        /**
         * 节点属性配置
         * 根据 resume 参数动态显示不同的属性
         */
        properties: [
            // ===== 核心属性: Resume 模式选择 =====
            {
                displayName: 'Resume',
                name: 'resume',
                type: 'options',
                builderHint: {
                    propertyHint:
                        'For user approval workflows, consider using nodes with operation: "sendAndWait" (e.g., email, Slack) instead of Wait node. If using "webhook", the URL will be generated at runtime and can be referenced with {{ $execution.resumeUrl }}.',
                },
                options: [
                    {
                        name: 'After Time Interval',
                        value: 'timeInterval',
                        description: 'Waits for a certain amount of time',
                    },
                    {
                        name: 'At Specified Time',
                        value: 'specificTime',
                        description: 'Waits until a specific date and time to continue',
                    },
                    {
                        name: 'On Webhook Call',
                        value: 'webhook',
                        description: 'Waits for a webhook call before continuing',
                    },
                    {
                        name: 'On Form Submitted',
                        value: 'form',
                        description: 'Waits for a form submission before continuing',
                    },
                ],
                default: 'timeInterval',
                description: 'Determines the waiting mode to use before the workflow continues',
            },

            // ===== 认证配置（仅对表单模式） =====
            {
                displayName: 'Authentication',
                name: 'incomingAuthentication',
                type: 'options',
                options: [
                    { name: 'Basic Auth', value: 'basicAuth' },
                    { name: 'None', value: 'none' },
                ],
                default: 'none',
                description:
                    'If and how incoming resume-webhook-requests to $execution.resumeFormUrl should be authenticated for additional security',
                displayOptions: {
                    show: { resume: ['form'] },
                },
            },

            // ===== 认证配置（仅对 Webhook 模式） =====
            {
                ...authenticationProperty(this.authPropertyName),
                description:
                    'If and how incoming resume-webhook-requests to $execution.resumeUrl should be authenticated for additional security',
                displayOptions: displayOnWebhook,
            },

            // ======================================
            //  resume: specificTime - 指定时间
            // ======================================
            {
                displayName: 'Date and Time',
                name: 'dateTime',
                type: 'dateTime',
                displayOptions: {
                    show: { resume: ['specificTime'] },
                },
                default: '',
                description: 'The date and time to wait for before continuing',
                required: true,
            },

            // ======================================
            //  resume: timeInterval - 时间间隔
            // ======================================
            {
                ...toWaitAmount,
                displayOptions: {
                    show: {
                        resume: ['timeInterval'],
                        '@version': [1],  // v1: 默认小时
                    },
                },
            },
            {
                ...toWaitAmount,
                default: 5,  // v1.1: 默认 5 秒
                displayOptions: {
                    show: {
                        resume: ['timeInterval'],
                    },
                    hide: {
                        '@version': [1],
                    },
                },
            },
            {
                ...unitSelector,
                displayOptions: {
                    show: {
                        resume: ['timeInterval'],
                        '@version': [1],
                    },
                },
            },
            {
                ...unitSelector,
                default: 'seconds',  // v1.1: 默认秒
                displayOptions: {
                    show: {
                        resume: ['timeInterval'],
                    },
                    hide: {
                        '@version': [1],
                    },
                },
            },

            // ======================================
            //  resume: webhook - Webhook 等待
            // ======================================
            {
                displayName:
                    'The webhook URL will be generated at run time. It can be referenced with the <strong>$execution.resumeUrl</strong> variable. Send it somewhere before getting to this node. <a href="https://docs.n8n.io/integrations/builtin/core-nodes/n8n-nodes-base.wait/?utm_source=n8n_app&utm_medium=node_settings_modal-credential_link&utm_campaign=n8n-nodes-base.wait" target="_blank">More info</a>',
                name: 'webhookNotice',
                type: 'notice',
                displayOptions: displayOnWebhook,
                default: '',
            },

            // ======================================
            //  resume: form - 表单等待
            // ======================================
            {
                displayName:
                    'The form url will be generated at run time. It can be referenced with the <strong>$execution.resumeFormUrl</strong> variable. Send it somewhere before getting to this node. <a href="https://docs.n8n.io/integrations/builtin/core-nodes/n8n-nodes-base.wait/?utm_source=n8n_app&utm_medium=node_settings_modal-credential_link&utm_campaign=n8n-nodes-base.wait" target="_blank">More info</a>',
                name: 'formNotice',
                type: 'notice',
                displayOptions: displayOnFormSubmission,
                default: '',
            },

            // ===== 合并其他属性 =====
            ...onFormSubmitProperties,
            ...onWebhookCallProperties,
            ...waitTimeProperties,

            // ===== Options 配置（Webhook 模式） =====
            {
                ...optionsProperty,
                displayOptions: displayOnWebhook,
                options: [...(optionsProperty.options as INodeProperties[]), webhookSuffix],
            },

            // ===== Options 配置（表单模式 - 有响应模式） =====
            {
                displayName: 'Options',
                name: 'options',
                type: 'collection',
                placeholder: 'Add option',
                default: {},
                displayOptions: {
                    show: { resume: ['form'] },
                    hide: { responseMode: ['responseNode'] },
                },
                options: [appendAttributionToForm, respondWithOptions, webhookSuffix],
            },

            // ===== Options 配置（表单模式 - 无响应模式） =====
            {
                displayName: 'Options',
                name: 'options',
                type: 'collection',
                placeholder: 'Add option',
                default: {},
                displayOptions: {
                    show: { resume: ['form'] },
                    hide: { responseMode: ['onReceived', 'lastNode'] },
                },
                options: [appendAttributionToForm, webhookSuffix],
            },
        ],
    };

    /**
     * ========================================================================
     * webhook() - 处理 Webhook 请求
     * ========================================================================
     * 
     * 当外部系统调用恢复 Webhook 时调用。
     * 
     * 流程：
     * 1. 检查 resume 模式
     * 2. 如果是 'form'，委托给 formWebhook 处理
     * 3. 否则调用父类的 webhook 方法
     * 
     * @param context - Webhook 上下文
     * @returns 处理结果
     */
    async webhook(context: IWebhookFunctions) {
        const resume = context.getNodeParameter('resume', 0) as string;
        if (resume === 'form') {
            // 表单模式：使用表单专用的 Webhook 处理器
            return await formWebhook(context, this.authPropertyName);
        }
        // Webhook 模式：复用父类逻辑
        return await super.webhook(context);
    }

    /**
     * ========================================================================
     * execute() - 执行节点
     * ========================================================================
     * 
     * 这是节点的主要执行方法，在工作流运行到 Wait 节点时调用。
     * 
     * 执行流程：
     * ┌─────────────────────────────────────────────────────────────────────┐
     * │ 1. 检查 resume 模式                                               │
     * │                                                                   │
     * │ 2. 如果是 webhook 或 form 模式：                                  │
     * │    a. 设置元数据（resumeUrl / resumeFormUrl）                     │
     * │    b. 检查是否有限制等待时间                                      │
     * │    c. 调用 configureAndPutToWait() 将执行置于等待状态            │
     * │    d. 如果表单模式且有表单触发器，重定向到表单页面                │
     * │                                                                   │
     * │ 3. 如果是 timeInterval 或 specificTime 模式：                    │
     * │    a. 计算等待时间                                                │
     * │    b. 如果 < 65秒：使用内存定时器                                 │
     * │    c. 如果 ≥ 65秒：持久化到数据库等待                            │
     * └─────────────────────────────────────────────────────────────────────┘
     * 
     * @param context - 执行上下文
     * @returns 执行结果（输入数据透传）
     */
    async execute(context: IExecuteFunctions): Promise<INodeExecutionData[][]> {
        const resume = context.getNodeParameter('resume', 0) as string;

        // ================================================================
        // 模式 1 & 2: Webhook / Form 等待
        // ================================================================
        if (['webhook', 'form'].includes(resume)) {
            let hasFormTrigger = false;

            if (resume === 'form') {
                // 添加签名的 resumeFormUrl 到元数据，供前端打开表单弹窗时使用
                const resumeFormUrl = context.evaluateExpression(
                    '{{ $execution.resumeFormUrl }}',
                    0,
                ) as string;
                context.setMetadata({ resumeFormUrl });

                // 检查工作流中是否有表单触发器节点
                // 如果有，需要进行重定向
                const parentNodes = context.getParentNodes(context.getNode().name);
                hasFormTrigger = parentNodes.some((node) => node.type === FORM_TRIGGER_NODE_TYPE);
            }

            if (resume === 'webhook') {
                // 添加签名的 resumeUrl 到元数据，供前端在等待提示中使用
                const resumeUrl = context.evaluateExpression('{{ $execution.resumeUrl }}', 0) as string;
                context.setMetadata({ resumeUrl });
            }

            // 将执行置于等待状态
            const returnData = await this.configureAndPutToWait(context);

            // 如果表单模式且有表单触发器，重定向到表单页面
            // 这样用户可以直接看到表单，而不需要手动打开链接
            if (resume === 'form' && hasFormTrigger) {
                await context.sendResponse({
                    headers: {
                        location: context.evaluateExpression('{{ $execution.resumeFormUrl }}', 0),
                    },
                    statusCode: 307, // Temporary Redirect
                });
            }

            return returnData;
        }

        // ================================================================
        // 模式 3: Time Interval - 时间间隔等待
        // ================================================================
        let waitTill: Date;
        if (resume === 'timeInterval') {
            const unit = context.getNodeParameter('unit', 0);

            // 验证时间单位
            if (!validateWaitUnit(unit)) {
                throw new NodeOperationError(
                    context.getNode(),
                    "Invalid wait unit. Valid units are 'seconds', 'minutes', 'hours', or 'days'.",
                );
            }

            let waitAmount = context.getNodeParameter('amount', 0);

            // 验证等待时间
            if (!validateWaitAmount(waitAmount)) {
                throw new NodeOperationError(
                    context.getNode(),
                    'Invalid wait amount. Please enter a number that is 0 or greater.',
                );
            }

            // 转换为毫秒
            if (unit === 'minutes') {
                waitAmount *= 60;
            }
            if (unit === 'hours') {
                waitAmount *= 60 * 60;
            }
            if (unit === 'days') {
                waitAmount *= 60 * 60 * 24;
            }

            waitAmount *= 1000;

            // 时区不影响相对时间（从当前时间加上指定的秒数）
            waitTill = new Date(new Date().getTime() + waitAmount);
        }

        // ================================================================
        // 模式 4: Specific Time - 指定时间等待
        // ================================================================
        else {
            try {
                const dateTimeStrRaw = context.getNodeParameter('dateTime', 0);
                const parsedDateTime = tryToParseDateTime(dateTimeStrRaw, context.getTimezone());

                waitTill = parsedDateTime.toUTC().toJSDate();
            } catch (e) {
                throw new NodeOperationError(
                    context.getNode(),
                    'Cannot put execution to wait because `dateTime` parameter is not a valid date. Please pick a specific date and time to wait until.',
                );
            }
        }

        const waitValue = Math.max(waitTill.getTime() - new Date().getTime(), 0);

        // ================================================================
        // 短等待（< 65秒）：使用内存定时器
        // ================================================================
        // 因为数据库每 60 秒检查一次等待的执行，
        // 对于短等待，直接使用内存定时器更高效
        if (waitValue < 65000) {
            return await new Promise((resolve, _reject) => {
                const timer = setTimeout(() => resolve([context.getInputData()]), waitValue);
                // 如果执行被取消，清理定时器
                context.onExecutionCancellation(() => {
                    clearTimeout(timer);
                    resolve([context.getInputData()]);
                });
            });
        }

        // ================================================================
        // 长等待（≥ 65秒）：持久化到数据库
        // ================================================================
        return await this.putToWait(context, waitTill);
    }

    /**
     * ========================================================================
     * configureAndPutToWait() - 配置并进入等待状态
     * ========================================================================
     * 
     * 用于 Webhook 和 Form 模式：
     * 1. 检查是否设置了限制等待时间
     * 2. 如果有，计算 waitTill
     * 3. 如果没有，使用 WAIT_INDEFINITELY（无限等待）
     * 
     * @param context - 执行上下文
     * @returns 执行结果
     */
    private async configureAndPutToWait(context: IExecuteFunctions) {
        let waitTill = WAIT_INDEFINITELY;  // 默认无限等待
        const limitWaitTime = context.getNodeParameter('limitWaitTime', 0);

        if (limitWaitTime === true) {
            const limitType = context.getNodeParameter('limitType', 0);

            if (limitType === 'afterTimeInterval') {
                // 计算时间间隔
                let waitAmount = context.getNodeParameter('resumeAmount', 0) as number;
                const resumeUnit = context.getNodeParameter('resumeUnit', 0);

                if (resumeUnit === 'minutes') {
                    waitAmount *= 60;
                }
                if (resumeUnit === 'hours') {
                    waitAmount *= 60 * 60;
                }
                if (resumeUnit === 'days') {
                    waitAmount *= 60 * 60 * 24;
                }

                waitAmount *= 1000;
                waitTill = new Date(new Date().getTime() + waitAmount);
            } else {
                // 指定时间
                waitTill = new Date(context.getNodeParameter('maxDateAndTime', 0) as string);
            }
        }

        // 将执行置于等待状态
        return await this.putToWait(context, waitTill);
    }

    /**
     * ========================================================================
     * putToWait() - 将执行置于等待状态
     * ========================================================================
     * 
     * 这是核心方法，调用上下文 API 将执行持久化到数据库，
     * 在指定的时间或事件恢复。
     * 
     * @param context - 执行上下文
     * @param waitTill - 等待截止时间
     * @returns 执行结果（输入数据透传）
     */
    private async putToWait(context: IExecuteFunctions, waitTill: Date) {
        // 将执行置于等待状态
        // 这会保存执行状态到数据库，并启动一个后台任务来恢复
        await context.putExecutionToWait(waitTill);
        
        // 返回输入数据（透传）
        // 这样当执行恢复时，数据会传递给后续节点
        return [context.getInputData()];
    }
}
