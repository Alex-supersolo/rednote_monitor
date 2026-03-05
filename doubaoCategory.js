const { CATEGORIES, normalizeCategory } = require('./productCategory');

const DEFAULT_QIANFAN_CHAT_URL = 'https://qianfan.baidubce.com/v2/chat/completions';

function getDoubaoConfig() {
    const qianfanApiKey = process.env.QIANFAN_API_KEY || '';
    const qianfanModelId = process.env.QIANFAN_MODEL_ID || '';
    const qianfanBaseUrl = process.env.QIANFAN_BASE_URL || DEFAULT_QIANFAN_CHAT_URL;

    // Backward compatibility: if Qianfan env is missing, keep supporting old Doubao env.
    const doubaoApiKey = process.env.DOUBAO_API_KEY || '';
    const doubaoModelId = process.env.DOUBAO_MODEL_ID || '';
    const doubaoBaseUrl = process.env.DOUBAO_BASE_URL || '';

    const useQianfan = Boolean(qianfanApiKey || qianfanModelId || process.env.QIANFAN_BASE_URL);

    return {
        provider: useQianfan ? 'qianfan' : 'doubao',
        apiKey: useQianfan ? qianfanApiKey : doubaoApiKey,
        modelId: useQianfan
            ? (qianfanModelId || 'ERNIE-Lite-Pro-128K')
            : doubaoModelId,
        baseUrl: useQianfan
            ? qianfanBaseUrl
            : (doubaoBaseUrl || 'https://ark.cn-beijing.volces.com/api/v3/chat/completions')
    };
}

function hasDoubaoConfig() {
    const config = getDoubaoConfig();
    return Boolean(config.apiKey && config.modelId);
}

function getAiProviderName() {
    const config = getDoubaoConfig();
    return config.provider === 'qianfan' ? 'Qianfan' : 'Doubao';
}

function extractCategoryFromText(text) {
    const content = (text || '').trim();
    if (!content) {
        return null;
    }

    const normalized = normalizeCategory(content.replace(/["'`\s]/g, ''));
    if (normalized !== '其他' || content.includes('其他')) {
        return normalized;
    }

    return CATEGORIES.find(category => content.includes(category)) || null;
}

function extractJsonBlock(text) {
    const content = String(text || '').trim();
    if (!content) {
        return null;
    }

    const fencedMatch = content.match(/```json\s*([\s\S]*?)```/i) || content.match(/```\s*([\s\S]*?)```/i);
    if (fencedMatch) {
        return fencedMatch[1].trim();
    }

    const startIndex = content.indexOf('{');
    const endIndex = content.lastIndexOf('}');
    if (startIndex !== -1 && endIndex !== -1 && endIndex > startIndex) {
        return content.slice(startIndex, endIndex + 1);
    }

    return null;
}

async function classifyCategoryWithDoubao(title) {
    const trimmedTitle = (title || '').trim();
    if (!trimmedTitle || !hasDoubaoConfig()) {
        return null;
    }

    const config = getDoubaoConfig();
    const categoryList = CATEGORIES.join('、');
    const response = await fetch(config.baseUrl, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${config.apiKey}`
        },
        body: JSON.stringify({
            model: config.modelId,
            temperature: 0.1,
            max_tokens: 32,
            messages: [
                {
                    role: 'system',
                    content: `你是商品类目分类器。你只能从以下类目中选择一个最匹配的类目返回，不要解释，不要返回其他文字：${categoryList}`
                },
                {
                    role: 'user',
                    content: `商品标题：${trimmedTitle}`
                }
            ]
        })
    });

    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`AI分类失败(${getAiProviderName()}): HTTP ${response.status} ${errorText}`);
    }

    const data = await response.json();
    const content = data?.choices?.[0]?.message?.content || '';
    return extractCategoryFromText(content);
}

function normalizeBatchResult(items, rawMappings = []) {
    const mappingById = new Map();

    rawMappings.forEach(item => {
        if (!item || item.id === undefined || item.id === null) {
            return;
        }
        const normalizedCategory = extractCategoryFromText(item.category || item.result || item.label || '');
        if (normalizedCategory) {
            mappingById.set(String(item.id), normalizedCategory);
        }
    });

    return items.map(item => ({
        id: item.id,
        category: mappingById.get(String(item.id)) || null
    }));
}

async function classifyCategoriesWithDoubaoBatch(items) {
    const normalizedItems = Array.isArray(items)
        ? items
            .map(item => ({
                id: item?.id,
                title: String(item?.title || '').trim()
            }))
            .filter(item => item.id !== undefined && item.id !== null && item.title)
        : [];

    if (normalizedItems.length === 0 || !hasDoubaoConfig()) {
        return [];
    }

    const config = getDoubaoConfig();
    const categoryList = CATEGORIES.join('、');
    const compactItems = normalizedItems.map(item => ({
        id: item.id,
        title: item.title
    }));
    const response = await fetch(config.baseUrl, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${config.apiKey}`
        },
        body: JSON.stringify({
            model: config.modelId,
            temperature: 0.1,
            max_tokens: Math.max(160, normalizedItems.length * 28),
            messages: [
                {
                    role: 'system',
                    content: [
                        '你是商品类目分类器。',
                        `你只能从以下类目中选择一个最匹配的类目：${categoryList}。`,
                        '请返回严格 JSON，不要解释，不要 Markdown。',
                        '返回格式：{"items":[{"id":"商品ID","category":"类目"}]}。'
                    ].join('')
                },
                {
                    role: 'user',
                    content: `请分类这些商品标题：${JSON.stringify(compactItems)}`
                }
            ]
        })
    });

    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`AI批量分类失败(${getAiProviderName()}): HTTP ${response.status} ${errorText}`);
    }

    const data = await response.json();
    const content = data?.choices?.[0]?.message?.content || '';
    const jsonBlock = extractJsonBlock(content);

    if (!jsonBlock) {
        return normalizeBatchResult(normalizedItems, []);
    }

    try {
        const parsed = JSON.parse(jsonBlock);
        const rawMappings = Array.isArray(parsed?.items) ? parsed.items : [];
        return normalizeBatchResult(normalizedItems, rawMappings);
    } catch (error) {
        return normalizeBatchResult(normalizedItems, []);
    }
}

module.exports = {
    hasDoubaoConfig,
    getAiProviderName,
    classifyCategoryWithDoubao,
    classifyCategoriesWithDoubaoBatch
};
