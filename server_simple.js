// 设置控制台编码为UTF-8（Windows系统）
if (process.platform === 'win32') {
    process.stdout.setEncoding('utf8');
}

const express = require('express');
const puppeteer = require('puppeteer-core');
const fs = require('fs');
const path = require('path');
const cron = require('node-cron');

const envPath = path.join(__dirname, '.env');
if (typeof process.loadEnvFile === 'function' && fs.existsSync(envPath)) {
    process.loadEnvFile(envPath);
}

const { inferProductCategoryFromText } = require('./productCategory');
const { classifyCategoriesWithDoubaoBatch, hasDoubaoConfig, getAiProviderName } = require('./doubaoCategory');
const {
    ADMIN_SESSION_COOKIE_NAME,
    buildSessionExpiryDate,
    createSessionToken,
    hashPassword,
    parseCookies,
    SESSION_COOKIE_NAME,
    serializeClearSessionCookie,
    serializeSessionCookie,
    validateLoginInput,
    validateRegistrationInput,
    verifyPassword
} = require('./authService');
const {
    STORAGE_DRIVER,
    DB_PATH,
    verifyConnection,
    getProductById,
    getProductByUrl,
    getPublicProductByCanonicalUrl,
    getUserPrivateProductByCanonicalUrl,
    buildPrivateProductStorageUrl,
    createProduct,
    updateProductSnapshot,
    upsertDailySnapshot,
    listProductsWithMetrics,
    queryProductsWithMetrics,
    getTrendData,
    deleteProduct,
    updateProductCategory,
    updateProductCategoryState,
    updateProductVisibility,
    getUserByUsername,
    createUserWithInvite,
    createSession,
    getUserBySessionToken,
    deleteSession,
    addUserSelection,
    removeUserSelection,
    isProductSelectedByUser,
    getUserSelectionCount,
    redeemInviteCodeForUser,
    listUsersWithStats,
    updateUserRole,
    updateUserActiveStatus,
    listInviteCodes,
    createInviteCode,
    updateInviteCode
} = require('./store');

const app = express();
const PORT = process.env.PORT || 3000;
const refreshJobs = new Map();
const importJobs = new Map();
const aiCategoryQueue = new Map();
let activeRefreshJobId = null;
let aiCategoryWorkerRunning = false;
const AI_CATEGORY_BATCH_SIZE = Math.max(
    1,
    Math.min(
        20,
        Number(process.env.AI_CATEGORY_BATCH_SIZE || process.env.QIANFAN_CATEGORY_BATCH_SIZE || process.env.DOUBAO_CATEGORY_BATCH_SIZE || 10)
    )
);
const AI_BOOT_SYNC_ENABLED = String(process.env.AI_BOOT_SYNC_ENABLED || 'true').toLowerCase() !== 'false';
const AI_FAILED_RETRY_COOLDOWN_HOURS = Math.max(0, Number(process.env.AI_FAILED_RETRY_COOLDOWN_HOURS || 24));
const AI_FAILED_RETRY_COOLDOWN_MS = AI_FAILED_RETRY_COOLDOWN_HOURS * 60 * 60 * 1000;
const DEFAULT_BROWSER_CANDIDATES = [
    process.env.PUPPETEER_EXECUTABLE_PATH,
    process.env.CHROME_PATH,
    '/usr/bin/google-chrome',
    '/usr/bin/google-chrome-stable',
    '/usr/bin/chromium',
    '/usr/bin/chromium-browser',
    '/snap/bin/chromium',
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
].filter(Boolean);
const SNAPSHOT_DROP_GUARD_ABS = 500;
const SNAPSHOT_DROP_GUARD_RATIO = 0.15;
const SNAPSHOT_SPIKE_GUARD_ABS = 2000;
const SNAPSHOT_SPIKE_GUARD_RATIO = 2;
const LOW_QUALITY_NAME_KEYWORDS = [
    '卖家口碑',
    '粉丝数',
    '进店逛逛',
    '联系客服',
    '店铺主页',
    '全部商品'
];

// 中间件
app.use(express.json());
app.use(express.static('public'));

app.use(async (req, res, next) => {
    try {
        const cookies = parseCookies(req.headers.cookie || '');
        const userSessionToken = cookies[SESSION_COOKIE_NAME];
        const adminSessionToken = cookies[ADMIN_SESSION_COOKIE_NAME];
        req.sessionToken = userSessionToken || null;
        req.adminSessionToken = adminSessionToken || null;
        req.currentUser = userSessionToken ? await getUserBySessionToken(userSessionToken) : null;
        req.currentAdmin = adminSessionToken ? await getUserBySessionToken(adminSessionToken) : null;
        if (req.currentAdmin && req.currentAdmin.role !== 'admin') {
            req.currentAdmin = null;
        }
        next();
    } catch (error) {
        next(error);
    }
});

app.get('/brand-logo-dark.png', (req, res) => {
    res.sendFile(path.join(__dirname, '透明背景黑色logo.png'));
});

app.get('/brand-logo-light.png', (req, res) => {
    res.sendFile(path.join(__dirname, '透明背景白色logo.png'));
});

app.get('/admin', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});

app.get('/admin/login', (req, res) => {
    res.redirect(302, '/admin');
});

function getSafeUser(user) {
    if (!user) {
        return null;
    }

    return {
        id: user.id,
        username: user.username,
        role: user.role,
        is_active: user.is_active,
        created_at: user.created_at,
        membership_plan: user.membership_plan || (user.role === 'admin' ? 'admin' : 'yearly'),
        membership_plan_label: user.membership_plan_label || (user.role === 'admin' ? '管理员' : '年卡会员'),
        membership_duration_days: Number(user.membership_duration_days || 365),
        membership_started_at: user.membership_started_at || user.created_at,
        membership_expires_at: user.membership_expires_at || null,
        membership_active: user.role === 'admin' ? true : Boolean(user.membership_active),
        monitor_limit: Number(user.monitor_limit || (user.role === 'admin' ? 5000 : 800))
    };
}

function buildMembershipMeta(user) {
    const safe = getSafeUser(user);
    return safe
        ? {
            plan: safe.membership_plan,
            planLabel: safe.membership_plan_label,
            expiresAt: safe.membership_expires_at,
            active: safe.membership_active,
            monitorLimit: safe.monitor_limit
        }
        : null;
}

function ensureMembershipActive(user, res) {
    if (!user || user.role === 'admin') {
        return true;
    }

    if (user.membership_active) {
        return true;
    }

    res.status(402).json({
        error: '会员已过期，请输入兑换码进行续费',
        code: 'MEMBERSHIP_EXPIRED',
        membership: buildMembershipMeta(user)
    });
    return false;
}

async function assertMonitorQuotaAvailable(user) {
    if (!user || user.role === 'admin') {
        return;
    }

    const monitorLimit = Number(user.monitor_limit || 0);
    if (!Number.isFinite(monitorLimit) || monitorLimit <= 0) {
        return;
    }

    const currentSelections = await getUserSelectionCount(user.id);
    if (currentSelections >= monitorLimit) {
        throw new Error(`当前套餐最多同时监控 ${monitorLimit} 个商品，已达上限。请先移除部分选品或使用兑换码升级套餐。`);
    }
}

function requireAuth(req, res, next) {
    if (!req.currentUser) {
        return res.status(401).json({ error: '请先登录后再继续操作' });
    }

    if (req.currentUser.role === 'admin') {
        return res.status(403).json({ error: '管理员账号请使用管理后台登录入口' });
    }

    next();
}

function requireAdminAuth(req, res, next) {
    if (!req.currentAdmin) {
        return res.status(401).json({ error: '请先登录后再继续操作' });
    }

    next();
}

function requireAnyAuth(req, res, next) {
    if (req.currentAdmin) {
        req.actorUser = req.currentAdmin;
        return next();
    }

    if (req.currentUser) {
        req.actorUser = req.currentUser;
        return next();
    }

    return res.status(401).json({ error: '请先登录后再继续操作' });
}

function launchBrowser() {
    const executablePath = DEFAULT_BROWSER_CANDIDATES.find(candidate => fs.existsSync(candidate));

    if (!executablePath) {
        throw new Error('未找到可用的 Chrome/Chromium，请设置 PUPPETEER_EXECUTABLE_PATH');
    }

    return puppeteer.launch({
        headless: "new",
        protocolTimeout: 60000,
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--disable-web-security',
            '--disable-features=VizDisplayCompositor',
            '--no-first-run',
            '--no-zygote',
            '--single-process',
            '--disable-extensions',
            '--disable-background-timer-throttling',
            '--disable-backgrounding-occluded-windows',
            '--disable-renderer-backgrounding',
            '--memory-pressure-off'
        ],
        executablePath
    });
}

function trimJobMap(jobMap) {
    const jobs = Array.from(jobMap.values()).sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
    jobs.slice(10).forEach(job => jobMap.delete(job.id));
}

function trimRefreshJobs() {
    trimJobMap(refreshJobs);
}

function trimImportJobs() {
    trimJobMap(importJobs);
}

function buildJobSummary(job) {
    return {
        id: job.id,
        type: job.type,
        status: job.status,
        total: job.total,
        completedCount: job.completedCount,
        successCount: job.successCount,
        failCount: job.failCount,
        progress: job.progress,
        message: job.message,
        startedAt: job.startedAt,
        finishedAt: job.finishedAt
    };
}

function buildRefreshJobSummary(job) {
    return buildJobSummary(job);
}

function buildImportJobSummary(job) {
    return buildJobSummary(job);
}

function createBackgroundJob(jobMap, type, total, source = 'manual', owner = null) {
    const id = `${type}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const job = {
        id,
        type,
        source,
        ownerUserId: owner?.id || null,
        ownerUsername: owner?.username || '',
        status: 'queued',
        total,
        completedCount: 0,
        successCount: 0,
        failCount: 0,
        progress: 0,
        message: '任务已创建',
        startedAt: null,
        finishedAt: null,
        createdAt: new Date().toISOString(),
        results: []
    };

    jobMap.set(id, job);
    return job;
}

function createRefreshJob(total, source = 'manual', owner = null) {
    const job = createBackgroundJob(refreshJobs, 'refresh', total, source, owner);
    trimRefreshJobs();
    return job;
}

function createImportJob(total, source = 'manual', owner = null) {
    const job = createBackgroundJob(importJobs, 'import', total, source, owner);
    trimImportJobs();
    return job;
}

function isUserJobOwner(job, user) {
    return Boolean(job && user && job.ownerUserId && job.ownerUserId === user.id);
}

function getVisibleActiveJob(jobMap, activeJobId, user) {
    const activeJob = activeJobId ? jobMap.get(activeJobId) : null;
    if (!activeJob || (activeJob.status !== 'queued' && activeJob.status !== 'running')) {
        return null;
    }

    if (!isUserJobOwner(activeJob, user)) {
        return null;
    }

    return activeJob;
}

function getActiveUserJob(jobMap, user) {
    if (!user) {
        return null;
    }

    const jobs = Array.from(jobMap.values())
        .filter(job => isUserJobOwner(job, user) && (job.status === 'queued' || job.status === 'running'))
        .sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

    return jobs[0] || null;
}

function markRefreshJobStarted(job) {
    job.status = 'running';
    job.startedAt = new Date().toISOString();
    job.message = `正在刷新 0/${job.total}`;
    activeRefreshJobId = job.id;
}

function markRefreshJobProgress(job, result) {
    job.results.push(result);
    job.completedCount = job.successCount + job.failCount;
    job.progress = job.total === 0 ? 100 : Math.round((job.completedCount / job.total) * 100);
    job.message = `正在刷新 ${job.completedCount}/${job.total}`;
}

function markRefreshJobFinished(job, status = 'completed', errorMessage = '') {
    job.status = status;
    job.finishedAt = new Date().toISOString();
    job.progress = 100;
    job.message = errorMessage || (status === 'failed'
        ? '刷新任务失败'
        : `刷新完成，成功 ${job.successCount} 个，失败 ${job.failCount} 个`);

    if (activeRefreshJobId === job.id) {
        activeRefreshJobId = null;
    }
}

function markImportJobStarted(job) {
    job.status = 'running';
    job.startedAt = new Date().toISOString();
    job.message = `正在导入 0/${job.total}`;
}

function markImportJobProgress(job, result) {
    job.results.push(result);
    job.completedCount = job.successCount + job.failCount;
    job.progress = job.total === 0 ? 100 : Math.round((job.completedCount / job.total) * 100);
    job.message = `正在导入 ${job.completedCount}/${job.total}`;
}

function markImportJobFinished(job, status = 'completed', errorMessage = '') {
    job.status = status;
    job.finishedAt = new Date().toISOString();
    job.progress = 100;
    job.message = errorMessage || (status === 'failed'
        ? '导入任务失败'
        : `导入完成，成功 ${job.successCount} 条，失败 ${job.failCount} 条`);
}

// 使用浏览器解析短链接
async function resolveShortUrl(shortUrl, options = {}) {
    console.log('开始解析短链接:', shortUrl);
    const browser = options.browser || await launchBrowser();
    const shouldCloseBrowser = !options.browser;

    try {
        const page = await browser.newPage();

        // 设置更真实的浏览器环境
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');

        // 设置视口大小
        await page.setViewport({ width: 1366, height: 768 });

        // 设置额外的请求头
        await page.setExtraHTTPHeaders({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        });

        // 隐藏webdriver属性
        await page.evaluateOnNewDocument(() => {
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
        });

        console.log('正在访问短链接...');

        // 尝试多种方式访问短链接
        let finalUrl = shortUrl;

        try {
            // 方法1: 等待网络空闲
            console.log('尝试方法1: 等待网络空闲...');
            await page.goto(shortUrl, {
                waitUntil: 'networkidle2',
                timeout: 12000
            });
            finalUrl = page.url();
            console.log('方法1成功，获取到URL:', finalUrl);
        } catch (error) {
            console.log('方法1失败，尝试方法2...');
            try {
                // 方法2: 等待加载完成
                console.log('尝试方法2: 等待加载完成...');
                await page.goto(shortUrl, {
                    waitUntil: 'load',
                    timeout: 10000
                });
                finalUrl = page.url();
                console.log('方法2成功，获取到URL:', finalUrl);
            } catch (error2) {
                console.log('方法2失败，尝试方法3...');
                try {
                    // 方法3: 不等待，直接获取重定向
                    console.log('尝试方法3: 快速访问...');
                    await page.goto(shortUrl, {
                        waitUntil: 'domcontentloaded',
                        timeout: 8000
                    });
                    // 等待一下让重定向完成
                    await page.waitForTimeout(3000);
                    finalUrl = page.url();
                    console.log('方法3成功，获取到URL:', finalUrl);
                } catch (error3) {
                    console.log('方法3失败，尝试方法4...');
                    try {
                        // 方法4: 最简单的访问方式
                        console.log('尝试方法4: 最简单访问...');
                        await page.goto(shortUrl, { timeout: 6000 });
                        await page.waitForTimeout(2000);
                        finalUrl = page.url();
                        console.log('方法4成功，获取到URL:', finalUrl);
                    } catch (error4) {
                        console.log('所有方法都失败，使用原链接');
                        finalUrl = shortUrl;
                    }
                }
            }
        }

        if (finalUrl !== shortUrl) {
            console.log('短链接解析成功:', shortUrl, '->', finalUrl);
        } else {
            console.log('短链接解析失败，使用原链接');
        }

        return finalUrl;

    } catch (error) {
        console.error('短链接解析过程出错:', error);
        // 解析失败时返回原URL
        return shortUrl;
    } finally {
        if (shouldCloseBrowser) {
            await browser.close().catch(() => {});
        }
    }
}

function normalizeProductUrl(url) {
    const trimmedUrl = (url || '').trim();
    const goodsDetailMatch = trimmedUrl.match(/https?:\/\/www\.xiaohongshu\.com\/goods-detail\/([^/?#\s]+)/i);
    if (goodsDetailMatch) {
        return `https://www.xiaohongshu.com/goods-detail/${goodsDetailMatch[1]}`;
    }

    const goodsMatch = trimmedUrl.match(/https?:\/\/(?:www|pages)\.xiaohongshu\.com\/goods\/([^/?#\s]+)/i);
    if (goodsMatch) {
        return `https://www.xiaohongshu.com/goods-detail/${goodsMatch[1]}`;
    }

    return null;
}

// 提取和处理小红书链接
async function processXhsUrl(inputText, options = {}) {
    console.log('处理输入文本:', inputText);

    // 支持的链接格式
    const urlPatterns = [
        // 完整的小红书商品链接
        /https?:\/\/www\.xiaohongshu\.com\/goods-detail\/[^\s]+/g,
        /https?:\/\/www\.xiaohongshu\.com\/goods\/[^\s]+/g,
        /https?:\/\/pages\.xiaohongshu\.com\/goods\/[^\s]+/g,
        // 小红书短链接
        /https?:\/\/xhslink\.com\/[^\s]+/g,
    ];

    let extractedUrl = null;

    // 尝试提取链接
    for (const pattern of urlPatterns) {
        const matches = inputText.match(pattern);
        if (matches && matches.length > 0) {
            extractedUrl = matches[0].trim();
            break;
        }
    }

    if (!extractedUrl) {
        throw new Error('未找到有效的小红书链接');
    }

    // 如果是短链接，尝试转换为长链接
    if (extractedUrl.includes('xhslink.com')) {
        console.log('检测到短链接，正在转换...');
        extractedUrl = await resolveShortUrl(extractedUrl, { browser: options.browser });
    }

    const normalizedUrl = normalizeProductUrl(extractedUrl);

    // 验证最终链接是否为小红书商品链接
    if (!normalizedUrl) {
        if (extractedUrl.includes('xhslink.com')) {
            throw new Error('短链接解析失败，请手动转换：\n1. 在浏览器中打开短链接\n2. 复制重定向后的长链接\n3. 使用长链接添加商品\n\n或者检查网络连接后重试');
        } else {
            throw new Error('链接不是小红书商品页面');
        }
    }

    console.log('最终处理的链接:', normalizedUrl);
    return normalizedUrl;
}

// 解析销量数字（处理万+格式）
function parseSalesNumber(salesText) {
    if (!salesText) return 0;

    const text = salesText.toString().toLowerCase();

    if (text.includes('万')) {
        const number = parseFloat(text.replace('万', '').replace('+', ''));
        return Math.floor(number * 10000);
    }

    return parseInt(text.replace(/[^\d]/g, '')) || 0;
}

function normalizeTextCandidate(value) {
    return String(value || '').replace(/\s+/g, ' ').trim();
}

function containsLowQualityKeyword(text) {
    return LOW_QUALITY_NAME_KEYWORDS.some(keyword => text.includes(keyword));
}

function isLikelyProductName(name) {
    const normalized = normalizeTextCandidate(name);
    if (!normalized || normalized.length < 4 || normalized.length > 180) {
        return false;
    }
    if (containsLowQualityKeyword(normalized)) {
        return false;
    }
    if (normalized.includes('已售') || normalized.includes('¥')) {
        return false;
    }
    if (/^(未知商品|商品名称|店铺)$/.test(normalized)) {
        return false;
    }
    if (!/[\u4e00-\u9fa5A-Za-z0-9]/.test(normalized)) {
        return false;
    }
    return true;
}

function isLikelyShopName(name) {
    const normalized = normalizeTextCandidate(name);
    if (!normalized || normalized.length < 2 || normalized.length > 60) {
        return false;
    }
    if (containsLowQualityKeyword(normalized)) {
        return false;
    }
    if (normalized.includes('已售') || normalized.includes('¥')) {
        return false;
    }
    if (!/[\u4e00-\u9fa5A-Za-z0-9]/.test(normalized)) {
        return false;
    }
    return true;
}

function choosePreferredValue(candidates, validator, fallback = '') {
    for (const candidate of candidates) {
        const normalized = normalizeTextCandidate(candidate);
        if (validator(normalized)) {
            return normalized;
        }
    }

    for (const candidate of candidates) {
        const normalized = normalizeTextCandidate(candidate);
        if (normalized) {
            return normalized;
        }
    }

    return fallback;
}

function normalizeSalesMetric(value) {
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) {
        return 0;
    }
    return Math.max(0, Math.floor(parsed));
}

async function applySnapshotGuard(productId, productData, contextLabel = 'snapshot', options = {}) {
    const sanitized = {
        ...productData,
        name: choosePreferredValue(
            [productData?.name, options.existingProductName],
            isLikelyProductName,
            '未知商品'
        ),
        shopName: choosePreferredValue(
            [productData?.shopName, options.existingShopName],
            isLikelyShopName,
            '未知店铺'
        ),
        productSales: normalizeSalesMetric(productData?.productSales),
        shopSales: normalizeSalesMetric(productData?.shopSales)
    };

    if (!productId) {
        return sanitized;
    }

    let previousSnapshot = null;
    try {
        const trendSource = await getTrendData(productId);
        const rows = Array.isArray(trendSource?.salesRows) ? trendSource.salesRows : [];
        previousSnapshot = rows.length > 0 ? rows[rows.length - 1] : null;
    } catch (error) {
        console.warn(`[snapshot-guard] 读取历史快照失败 (product=${productId}): ${error.message}`);
        return sanitized;
    }

    if (!previousSnapshot) {
        return sanitized;
    }

    const prevProductSales = normalizeSalesMetric(previousSnapshot.product_sales);
    const prevShopSales = normalizeSalesMetric(previousSnapshot.shop_sales);
    const reasons = [];

    if (sanitized.shopSales === 0 && prevShopSales > 0 && sanitized.productSales > 0) {
        sanitized.shopSales = prevShopSales;
        reasons.push(`shopSales 0 -> ${prevShopSales}`);
    }

    if (prevProductSales > 0) {
        const growth = sanitized.productSales - prevProductSales;
        const drop = prevProductSales - sanitized.productSales;
        const copiedShopPattern = sanitized.productSales > 0
            && Math.abs(sanitized.productSales - sanitized.shopSales) <= Math.max(20, Math.round(sanitized.productSales * 0.02));
        const shopChange = Math.abs(sanitized.shopSales - prevShopSales);
        const shopAlmostStable = prevShopSales > 0
            && shopChange <= Math.max(50, Math.round(prevShopSales * 0.02));
        const hugeUnexpectedSpike = growth > Math.max(
            SNAPSHOT_SPIKE_GUARD_ABS,
            Math.round(prevProductSales * SNAPSHOT_SPIKE_GUARD_RATIO)
        );
        if (hugeUnexpectedSpike && copiedShopPattern && shopAlmostStable) {
            sanitized.productSales = prevProductSales;
            reasons.push(`abnormal spike ${prevProductSales} -> ${prevProductSales + growth}`);
        }

        const abnormalDrop = drop > Math.max(
            SNAPSHOT_DROP_GUARD_ABS,
            Math.round(prevProductSales * SNAPSHOT_DROP_GUARD_RATIO)
        );
        if (abnormalDrop) {
            sanitized.productSales = prevProductSales;
            reasons.push(`abnormal drop ${prevProductSales} -> ${prevProductSales - drop}`);
        }
    }

    if (sanitized.shopSales > 0 && sanitized.productSales > sanitized.shopSales) {
        sanitized.shopSales = Math.max(sanitized.productSales, prevShopSales);
        reasons.push(`shopSales raised to ${sanitized.shopSales}`);
    }

    if (reasons.length > 0) {
        console.warn(
            `[snapshot-guard] ${contextLabel} product=${productId} applied: ${reasons.join('; ')}`
        );
    }

    return sanitized;
}

function normalizeImageUrl(imageUrl) {
    if (!imageUrl) {
        return '';
    }

    const trimmedUrl = imageUrl.trim();
    if (!trimmedUrl) {
        return '';
    }

    if (trimmedUrl.startsWith('//')) {
        return `https:${trimmedUrl}`;
    }

    return trimmedUrl;
}

function resolveInitialProductCategory(productTitle, shopName) {
    return inferProductCategoryFromText(productTitle, shopName);
}

function hasResolvedCategory(category) {
    const normalized = String(category || '').trim();
    return Boolean(normalized && normalized !== '其他');
}

function parseTimestampToMs(value) {
    const timestamp = Date.parse(String(value || ''));
    return Number.isFinite(timestamp) ? timestamp : null;
}

function isFailedCategoryRetryCoolingDown(product) {
    if (AI_FAILED_RETRY_COOLDOWN_MS <= 0 || product?.category_status !== 'failed') {
        return false;
    }

    const lastStatusMs =
        parseTimestampToMs(product.category_status_updated_at) ??
        parseTimestampToMs(product.updated_at) ??
        parseTimestampToMs(product.created_at);
    if (lastStatusMs === null) {
        return false;
    }

    return Date.now() - lastStatusMs < AI_FAILED_RETRY_COOLDOWN_MS;
}

function shouldEnqueueAiCategory(product, options = {}) {
    if (!hasDoubaoConfig() || !product?.id || !product?.name) {
        return false;
    }

    if (product.category_source === 'manual') {
        return false;
    }

    if (options.force) {
        return true;
    }

    if (product.category_source === 'ai' && product.category_status === 'completed' && hasResolvedCategory(product.category)) {
        return false;
    }

    if (product.category_status === 'queued' || product.category_status === 'processing') {
        return false;
    }

    if (isFailedCategoryRetryCoolingDown(product)) {
        return false;
    }

    return true;
}

function enqueueAiCategorySync(product, options = {}) {
    if (!shouldEnqueueAiCategory(product, options)) {
        return false;
    }

    updateProductCategoryState(product.id, {
        categorySource: product.category_source === 'manual' ? 'manual' : 'rule',
        categoryStatus: 'queued'
    }).catch(error => {
        console.error(`更新 AI 队列状态失败 (ID: ${product.id}):`, error.message);
    });

    aiCategoryQueue.set(String(product.id), {
        id: product.id,
        name: product.name,
        shopName: product.shop_name || product.shopName || '',
        currentCategory: product.category || '其他',
        force: Boolean(options.force)
    });
    processAiCategoryQueue().catch(error => {
        console.error('AI 类目队列执行失败:', error.message);
    });
    return true;
}

async function processAiCategoryQueue() {
    if (aiCategoryWorkerRunning || !hasDoubaoConfig() || aiCategoryQueue.size === 0) {
        return;
    }

    aiCategoryWorkerRunning = true;

    try {
        while (aiCategoryQueue.size > 0) {
            const batch = [];
            while (batch.length < AI_CATEGORY_BATCH_SIZE && aiCategoryQueue.size > 0) {
                const [queueKey, task] = aiCategoryQueue.entries().next().value;
                aiCategoryQueue.delete(queueKey);
                batch.push(task);
            }

            try {
                const activeTasks = [];

                for (const task of batch) {
                    const currentProduct = await getProductById(task.id);
                    if (!currentProduct || currentProduct.category_source === 'manual') {
                        continue;
                    }

                    await updateProductCategoryState(task.id, {
                        categoryStatus: 'processing'
                    });
                    activeTasks.push(task);
                }

                if (activeTasks.length === 0) {
                    continue;
                }

                const batchResults = await classifyCategoriesWithDoubaoBatch(activeTasks.map(task => ({
                    id: task.id,
                    title: task.name
                })));
                const resultMap = new Map(batchResults.map(item => [String(item.id), item.category]));

                for (const task of activeTasks) {
                    const aiCategory = resultMap.get(String(task.id));
                    if (!aiCategory) {
                        await updateProductCategoryState(task.id, {
                            categoryStatus: 'failed'
                        });
                        continue;
                    }

                    await updateProductCategory(task.id, aiCategory, {
                        categorySource: 'ai',
                        categoryStatus: 'completed'
                    });
                    console.log(`AI 类目已更新: ${task.name} -> ${aiCategory}`);
                }
            } catch (error) {
                for (const task of batch) {
                    await updateProductCategoryState(task.id, {
                        categoryStatus: 'failed'
                    }).catch(() => {});
                    console.error(`AI 类目补分类失败 (ID: ${task.id}):`, error.message);
                }
            }
        }
    } finally {
        aiCategoryWorkerRunning = false;
        if (aiCategoryQueue.size > 0) {
            setTimeout(() => {
                processAiCategoryQueue().catch(queueError => {
                    console.error('AI 类目队列重试失败:', queueError.message);
                });
            }, 300);
        }
    }
}

async function syncExistingCategoriesWithAi() {
    if (!hasDoubaoConfig()) {
        return;
    }

    try {
        const products = await listProductsWithMetrics();
        let queuedCount = 0;
        for (const product of products) {
            if (product.category_source === 'manual') {
                continue;
            }
            const queued = enqueueAiCategorySync(product, { force: product.category === '其他' });
            if (queued) {
                queuedCount += 1;
            }
        }
        console.log(`${getAiProviderName()} 类目补分类任务已入队 ${queuedCount} 个（总商品 ${products.length} 个）`);
    } catch (error) {
        console.error(`${getAiProviderName()} 类目同步失败:`, error.message);
    }
}

// 爬取商品数据
async function scrapeProductData(url, options = {}) {
    console.log('开始爬取商品数据:', url);
    const browser = options.browser || await launchBrowser();
    const shouldCloseBrowser = !options.browser;
    const shouldCaptureScreenshot = options.captureScreenshot === true;
    let page = null;

    try {
        page = await browser.newPage();

        // 设置用户代理
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36');

        console.log('正在访问页面...');
        try {
            await page.goto(url, { waitUntil: 'networkidle2', timeout: 15000 });
            console.log('页面加载完成');
        } catch (error) {
            console.log('页面加载超时，尝试继续...');
        }

        // 等待页面加载
        console.log('等待页面渲染...');
        await page.waitForTimeout(5000);

        console.log('正在提取数据...');

        if (shouldCaptureScreenshot) {
            await page.screenshot({ path: 'debug_screenshot.png', fullPage: true });
            console.log('页面截图已保存为 debug_screenshot.png');
        }

        const data = await page.evaluate(() => {
            // 获取页面所有文本内容用于调试
            const pageText = document.body.innerText;
            console.log('页面文本内容:', pageText.substring(0, 500));

            // 尝试多种选择器来获取商品信息
            const getTextBySelectors = (selectors, description) => {
                for (const selector of selectors) {
                    const elements = document.querySelectorAll(selector);
                    if (elements.length > 0) {
                        for (const element of elements) {
                            const text = element.textContent.trim();
                            if (text) {
                                console.log(`${description} - 找到 ${selector}: ${text}`);
                                return text;
                            }
                        }
                    }
                }
                console.log(`${description} - 未找到匹配的元素`);
                return '';
            };

            // 商品名称 - 扩展更多选择器
            const name = getTextBySelectors([
                'h1',
                '[class*="title"]',
                '[class*="Title"]',
                '[class*="name"]',
                '[class*="Name"]',
                '.goods-title',
                '.product-title',
                '.item-title',
                '[data-testid*="title"]',
                '[data-testid*="name"]'
            ], '商品名称');

            // 商品价格 - 扩展更多选择器
            const priceText = getTextBySelectors([
                '[class*="price"]',
                '[class*="Price"]',
                '[class*="money"]',
                '[class*="Money"]',
                '[class*="yuan"]',
                '[class*="Yuan"]',
                '.current-price',
                '.sale-price',
                '.price-current',
                '[data-testid*="price"]'
            ], '商品价格');

            // 商品销量 - 扩展更多选择器
            const salesText = getTextBySelectors([
                '[class*="sales"]',
                '[class*="Sales"]',
                '[class*="sold"]',
                '[class*="Sold"]',
                '[class*="sell"]',
                '[class*="Sell"]',
                '[class*="buy"]',
                '[class*="Buy"]',
                '.sales-count',
                '.sold-count',
                '[data-testid*="sales"]',
                '[data-testid*="sold"]'
            ], '商品销量');

            // 店铺名称
            const shopName = getTextBySelectors([
                '[class*="shop"]',
                '[class*="Shop"]',
                '[class*="store"]',
                '[class*="Store"]',
                '[class*="brand"]',
                '[class*="Brand"]',
                '.shop-name',
                '.store-name',
                '[data-testid*="shop"]',
                '[data-testid*="store"]'
            ], '店铺名称');

            // 店铺销量
            const shopSalesText = getTextBySelectors([
                '[class*="shop"][class*="sales"]',
                '[class*="store"][class*="sales"]',
                '.shop-sales',
                '.store-sales'
            ], '店铺销量');

            const imageCandidates = [
                document.querySelector('.carousel-image')?.currentSrc,
                document.querySelector('.carousel-image')?.src,
                document.querySelector('[class*="carousel"] img')?.currentSrc,
                document.querySelector('[class*="carousel"] img')?.src,
                document.querySelector('meta[property="og:image"]')?.content,
                document.querySelector('meta[name="og:image"]')?.content,
                document.querySelector('meta[name="twitter:image"]')?.content,
                document.querySelector('img[src*="sns-webpic-qc"]')?.src,
                document.querySelector('img[src*="xhscdn.com"]')?.currentSrc,
                document.querySelector('img[src*="xhscdn.com"]')?.src,
                document.querySelector('img[src*="xiaohongshu"]')?.src,
                document.querySelector('img')?.src
            ].filter(Boolean);

            const coverImage = imageCandidates.find(src =>
                /^https?:\/\//i.test(src) || src.startsWith('//')
            ) || '';

            // 智能从页面文本中提取信息
            let extractedName = '未知商品';
            let extractedPrice = 0;
            let extractedSales = '0';
            let extractedShopName = '未知店铺';
            let extractedShopSales = '0';

            // 从页面文本中直接查找商品名称
            console.log('页面文本前500字符:', pageText.substring(0, 500));

            // 改进的商品名称提取逻辑
            const namePatterns = [
                // 针对果壳铃商品的特殊格式：【云水】三果33颗果壳摇铃 么几果壳铃 · 草绳33颗
                /【[^】]+】[^\n]*(?:果壳|摇铃|风铃)[^\n]*·[^\n]*/,
                // 匹配已售数字后的商品名称（针对果壳铃的格式）
                /已售\d+[万千]?\n([^\n]{8,100}?)(?=\n(?:保障|跨店铺|已选|发货))/,
                // 匹配包含【】或·符号的商品名称
                /([^\n]*(?:【[^】]+】|·)[^\n]{8,}?)(?=\n(?:保障|已选|发货))/,
                // 匹配特定品牌的商品名称（包含云水、果壳等关键词）
                /((?:花栖|森野植愈|么几果壳铃|自明|小飞基|云水|果壳|摇铃)[^\n]{3,}?)(?=\n(?:保障|已选|发货))/,
                // 匹配包含商品特征词的名称
                /([^\n]*(?:果壳|摇铃|风铃|挂件|手铃|种子|白噪音|瑜伽|冥想|三果|颗)[^\n]{3,}?)(?=\n(?:保障|已选|发货))/,
                // 匹配长商品名称（在关键词前）
                /([^\n¥]{12,80}?)(?=\n(?:保障|已选|发货|跨店铺))/,
                // 匹配包含特殊符号的商品名称
                /([^\n]*[｜·][^\n]{6,}?)(?=\n(?:保障|已选|发货))/,
                // 匹配价格后面的商品名称
                /¥\s*\d+(?:\.\d+)?\n([^\n]{8,80}?)(?=\n(?:保障|已选|发货|跨店铺))/,
                // 新增：匹配包含数字+颗的商品名称（针对果壳铃）
                /([^\n]*\d+颗[^\n]{3,}?)(?=\n(?:保障|已选|发货))/,
                // 新增：匹配草绳相关的商品名称
                /([^\n]*(?:草绳|三果)[^\n]{3,}?)(?=\n(?:保障|已选|发货))/,
                // 新增：专门针对【云水】三果33颗果壳摇铃的模式
                /(【云水】[^\n]*(?:果壳|摇铃)[^\n]*)/,
                // 新增：匹配跨店铺优惠后的商品名称
                /跨店铺[^\n]*\n([^\n]{10,}?)(?=\n(?:保障|已选|发货))/
            ];

            // 如果上述模式都没有匹配到，尝试从页面文本中直接提取商品名称
            if (extractedName === '未知商品') {
                // 从你的日志中可以看到，商品名称通常出现在价格和保障之间
                // 尝试更宽松的匹配模式
                const fallbackPatterns = [
                    // 匹配价格后到保障前的内容，过滤掉跨店铺等信息
                    /¥\s*\d+(?:\.\d+)?[^\n]*\n([^\n]+?)(?=\n(?:保障|跨店铺))/,
                    // 匹配已售后到保障前的内容
                    /已售\d+[万千]?[^\n]*\n([^\n]+?)(?=\n保障)/,
                    // 匹配包含中文字符的较长文本行（可能是商品名称）
                    /([^\n]*[\u4e00-\u9fa5]{5,}[^\n]{10,}?)(?=\n(?:保障|已选|发货))/
                ];

                for (const pattern of fallbackPatterns) {
                    const match = pageText.match(pattern);
                    if (match) {
                        let candidateName = match[1] || match[0];
                        candidateName = candidateName.trim();

                        // 更宽松的过滤条件
                        if (!candidateName.includes('卖家口碑') &&
                            !candidateName.includes('粉丝数') &&
                            !candidateName.includes('进店逛逛') &&
                            !candidateName.includes('已售') &&
                            !candidateName.includes('¥') &&
                            !candidateName.includes('跨店铺') &&
                            candidateName.length > 5 &&
                            candidateName.length < 200) {
                            extractedName = candidateName;
                            console.log('使用备用模式提取到商品名称:', extractedName);
                            break;
                        }
                    }
                }
            }

            for (const pattern of namePatterns) {
                const match = pageText.match(pattern);
                if (match) {
                    let candidateName = match[1] || match[0];
                    candidateName = candidateName.trim();

                    // 过滤掉明显不是商品名称的内容
                    if (!candidateName.includes('卖家口碑') &&
                        !candidateName.includes('粉丝数') &&
                        !candidateName.includes('进店逛逛') &&
                        !candidateName.includes('已售') &&
                        !candidateName.includes('¥') &&
                        candidateName.length > 8 &&
                        candidateName.length < 150) {
                        extractedName = candidateName;
                        console.log('从文本中提取到商品名称:', extractedName);
                        break;
                    }
                }
            }

            // 提取价格（寻找 ¥ 符号后的数字）
            const priceMatch = pageText.match(/¥\s*(\d+(?:\.\d+)?)/);
            if (priceMatch) {
                extractedPrice = parseFloat(priceMatch[1]);
                console.log('从文本中提取到价格:', extractedPrice);
            }

            // 提取商品销量（寻找"已售"后的数字）
            const salesMatch = pageText.match(/已售\s*(\d+(?:\.\d+)?[万千]?)/);
            if (salesMatch) {
                extractedSales = salesMatch[1];
                console.log('从文本中提取到商品销量:', extractedSales);
            }

            // 提取店铺名称（寻找店铺名称模式）
            const shopMatch = pageText.match(/([^¥\n]{3,20}?)(?:的店|店铺)/);
            if (shopMatch) {
                extractedShopName = shopMatch[1].trim();
                console.log('从文本中提取到店铺名称:', extractedShopName);
            }

            // 提取店铺销量（寻找店铺相关的已售数字）
            const shopSalesMatches = pageText.match(/已售\s*(\d+(?:\.\d+)?[万千]?)/g);
            if (shopSalesMatches && shopSalesMatches.length > 1) {
                // 如果有多个"已售"，第二个通常是店铺销量
                const shopSalesMatch = shopSalesMatches[1].match(/(\d+(?:\.\d+)?[万千]?)/);
                if (shopSalesMatch) {
                    extractedShopSales = shopSalesMatch[1];
                    console.log('从文本中提取到店铺销量:', extractedShopSales);
                }
            }

            // 使用提取到的信息，优先使用智能提取的结果
            const finalName = name || extractedName;
            const finalPrice = parseFloat(priceText.replace(/[^\d.]/g, '')) || extractedPrice || 0;
            const finalSales = salesText || extractedSales;
            const finalShopName = shopName || extractedShopName;
            const finalShopSales = shopSalesText || extractedShopSales;

            console.log('最终提取结果:', {
                name: finalName,
                price: finalPrice,
                sales: finalSales,
                shopName: finalShopName,
                shopSales: finalShopSales
            });

            return {
                name: finalName,
                price: finalPrice,
                salesText: finalSales,
                shopName: finalShopName,
                shopSalesText: finalShopSales,
                imageUrl: coverImage,
                // 调试信息
                debug: {
                    originalPriceText: priceText,
                    originalSalesText: salesText,
                    pageTextSample: pageText.substring(0, 300),
                    extractedInfo: {
                        name: extractedName,
                        price: extractedPrice,
                        sales: extractedSales,
                        shopName: extractedShopName,
                        shopSales: extractedShopSales
                    }
                }
            };
        });

        console.log('提取到的原始数据:', data);

        const selectorProductSales = parseSalesNumber(data.salesText);
        const extractedProductSales = parseSalesNumber(data.debug?.extractedInfo?.sales);
        const selectorShopSales = parseSalesNumber(data.shopSalesText);
        const extractedShopSales = parseSalesNumber(data.debug?.extractedInfo?.shopSales);
        const normalizedProductSales = selectorProductSales > 0 ? selectorProductSales : extractedProductSales;
        const normalizedShopSales = selectorShopSales > 0 ? selectorShopSales : extractedShopSales;
        const normalizedPrice = Number.isFinite(Number(data.price)) && Number(data.price) > 0
            ? Number(data.price)
            : Number(data.debug?.extractedInfo?.price || 0);

        const result = {
            name: choosePreferredValue(
                [data.name, data.debug?.extractedInfo?.name],
                isLikelyProductName,
                '未知商品'
            ),
            price: normalizedPrice || 0,
            productSales: normalizedProductSales,
            shopName: choosePreferredValue(
                [data.shopName, data.debug?.extractedInfo?.shopName],
                isLikelyShopName,
                '未知店铺'
            ),
            shopSales: normalizedShopSales,
            imageUrl: normalizeImageUrl(data.imageUrl)
        };

        result.category = resolveInitialProductCategory(result.name, result.shopName);
        result.categorySource = 'rule';
        result.categoryStatus = hasDoubaoConfig() ? 'queued' : 'rule_only';

        console.log('处理后的数据:', result);
        return result;

    } catch (error) {
        console.error('爬取数据失败:', error);
        throw error;
    } finally {
        if (page) {
            await page.close().catch(() => {});
        }
        if (shouldCloseBrowser) {
            await browser.close().catch(() => {});
        }
    }
}

async function refreshProductsBatch(productsToRefresh, options = {}) {
    const concurrency = Math.max(1, Math.min(Number(options.concurrency) || 2, 3));
    const pauseMs = Math.max(0, Number(options.pauseMs) || 150);
    let currentIndex = 0;
    let successCount = 0;
    let failCount = 0;
    const results = [];

    async function runWorker(workerId) {
        const sharedBrowser = await launchBrowser();

        try {
            while (currentIndex < productsToRefresh.length) {
                const product = productsToRefresh[currentIndex];
                currentIndex += 1;

                try {
                    console.log(`正在刷新商品: ${product.name} (ID: ${product.id}, Worker: ${workerId})`);
                    const productData = await scrapeProductData(product.url, {
                        browser: sharedBrowser
                    });
                    const now = new Date();
                    if (product.category_source === 'manual') {
                        productData.category = product.category;
                        productData.categorySource = 'manual';
                        productData.categoryStatus = 'manual';
                    } else if (product.category_source === 'ai' && product.category_status === 'completed' && hasResolvedCategory(product.category)) {
                        productData.category = product.category;
                        productData.categorySource = 'ai';
                        productData.categoryStatus = 'completed';
                    }
                    const sanitizedProductData = await applySnapshotGuard(
                        product.id,
                        productData,
                        `batch-refresh/${workerId}`,
                        {
                            existingProductName: product.name,
                            existingShopName: product.shop_name || product.shopName
                        }
                    );
                    await updateProductSnapshot(product.id, sanitizedProductData, now);
                    await upsertDailySnapshot(product.id, sanitizedProductData, now);
                    if (product.category_source !== 'manual') {
                        enqueueAiCategorySync({
                            id: product.id,
                            name: sanitizedProductData.name,
                            shopName: sanitizedProductData.shopName,
                            category: sanitizedProductData.category,
                            category_source: sanitizedProductData.categorySource,
                            category_status: sanitizedProductData.categoryStatus,
                            category_status_updated_at: product.category_status_updated_at,
                            updated_at: now.toISOString()
                        });
                    }

                    successCount += 1;
                    const result = {
                        id: product.id,
                        success: true,
                        productSales: sanitizedProductData.productSales
                    };
                    results.push(result);
                    if (typeof options.onProgress === 'function') {
                        options.onProgress(result, { successCount, failCount });
                    }

                    if (pauseMs > 0) {
                        await new Promise(resolve => setTimeout(resolve, pauseMs));
                    }
                } catch (error) {
                    failCount += 1;
                    const result = {
                        id: product.id,
                        success: false,
                        error: error.message
                    };
                    results.push(result);
                    if (typeof options.onProgress === 'function') {
                        options.onProgress(result, { successCount, failCount });
                    }
                    console.error(`❌ 商品 ${product.name} 刷新失败:`, error.message);
                }
            }
        } finally {
            await sharedBrowser.close().catch(() => {});
        }
    }

    const workers = Array.from({ length: Math.min(concurrency, productsToRefresh.length) }, (_, index) =>
        runWorker(index + 1)
    );

    await Promise.all(workers);

    return {
        total: productsToRefresh.length,
        successCount,
        failCount,
        results
    };
}

async function addProductsToLibraryForUserBatch(user, rawUrls, options = {}) {
    const concurrency = Math.max(1, Math.min(Number(options.concurrency) || 3, 4));
    let currentIndex = 0;
    let successCount = 0;
    let failCount = 0;
    const results = [];

    async function runWorker(workerId) {
        const sharedBrowser = await launchBrowser();

        try {
            while (currentIndex < rawUrls.length) {
                const rawUrl = rawUrls[currentIndex];
                currentIndex += 1;

                try {
                    console.log(`正在导入商品 (${workerId}): ${rawUrl}`);
                    const result = await addProductToLibraryForUser(user, rawUrl, {
                        browser: sharedBrowser
                    });
                    successCount += 1;
                    const itemResult = {
                        input: rawUrl,
                        success: true,
                        message: result.message,
                        productId: result.product?.id || null,
                        existed: Boolean(result.existed)
                    };
                    results.push(itemResult);

                    if (typeof options.onProgress === 'function') {
                        options.onProgress(itemResult, { successCount, failCount });
                    }
                } catch (error) {
                    failCount += 1;
                    const itemResult = {
                        input: rawUrl,
                        success: false,
                        message: error.message
                    };
                    results.push(itemResult);

                    if (typeof options.onProgress === 'function') {
                        options.onProgress(itemResult, { successCount, failCount });
                    }

                    console.error(`❌ 商品导入失败 (${workerId}):`, error.message);
                }
            }
        } finally {
            await sharedBrowser.close().catch(() => {});
        }
    }

    const workers = Array.from({ length: Math.min(concurrency, rawUrls.length) }, (_, index) =>
        runWorker(index + 1)
    );

    await Promise.all(workers);

    return {
        total: rawUrls.length,
        successCount,
        failCount,
        results
    };
}

async function addProductToLibraryForUser(user, rawUrl, options = {}) {
    const canonicalUrl = await processXhsUrl(rawUrl, { browser: options.browser });
    const adminUser = user?.role === 'admin';

    const ensureSelected = async (product) => {
        const alreadySelected = await isProductSelectedByUser(user.id, product.id);
        if (!alreadySelected) {
            await assertMonitorQuotaAvailable(user);
            await addUserSelection(user.id, product.id);
        }
        return alreadySelected;
    };

    // 1) Public product has highest priority for everyone.
    let existingPublicProduct = await getPublicProductByCanonicalUrl(canonicalUrl);
    if (!existingPublicProduct) {
        // Backward compatibility for old records that have no canonical_url.
        const legacyByUrl = await getProductByUrl(canonicalUrl);
        if (legacyByUrl && legacyByUrl.visibility_scope === 'public') {
            existingPublicProduct = legacyByUrl;
        }
    }
    if (existingPublicProduct) {
        const alreadySelected = await ensureSelected(existingPublicProduct);
        return {
            message: alreadySelected ? '该商品已在商品总库，也已在你的选品中' : '该商品已在商品总库，已加入你的选品',
            product: existingPublicProduct,
            existed: true,
            selected: true
        };
    }

    if (adminUser) {
        const productData = await scrapeProductData(canonicalUrl, {
            browser: options.browser
        });
        const product = await createProduct(canonicalUrl, productData, {
            visibilityScope: 'public',
            ownerUserId: null,
            canonicalUrl
        });
        await upsertDailySnapshot(product.id, productData);
        await ensureSelected(product);
        enqueueAiCategorySync({
            id: product.id,
            name: productData.name,
            shopName: productData.shopName,
            category: productData.category
        });

        return {
            message: '商品已加入总库并同步到你的选品',
            product,
            existed: false,
            selected: true
        };
    }

    // 2) Member can reuse their own private copy (if any), but never reuse others' private copy.
    let ownPrivateProduct = await getUserPrivateProductByCanonicalUrl(user.id, canonicalUrl);
    if (!ownPrivateProduct) {
        const legacyByUrl = await getProductByUrl(canonicalUrl);
        if (legacyByUrl && legacyByUrl.visibility_scope === 'private' && legacyByUrl.owner_user_id === user.id) {
            ownPrivateProduct = legacyByUrl;
        }
    }
    if (ownPrivateProduct) {
        const alreadySelected = await ensureSelected(ownPrivateProduct);
        return {
            message: alreadySelected ? '该商品已在你的选品池中' : '该商品已加入你的选品池（不会展示到商品总库）',
            product: ownPrivateProduct,
            existed: true,
            selected: true
        };
    }

    // 3) Create a dedicated private product record for this user.
    await assertMonitorQuotaAvailable(user);
    const productData = await scrapeProductData(canonicalUrl, {
        browser: options.browser
    });
    const privateStorageUrl = buildPrivateProductStorageUrl(canonicalUrl, user.id);
    let product;
    try {
        product = await createProduct(privateStorageUrl, productData, {
            visibilityScope: 'private',
            ownerUserId: user.id,
            canonicalUrl
        });
    } catch (error) {
        // Concurrency fallback: if another request created the same row first, reuse it.
        const concurrentOwnPrivate = await getUserPrivateProductByCanonicalUrl(user.id, canonicalUrl);
        if (!concurrentOwnPrivate) {
            throw error;
        }
        product = concurrentOwnPrivate;
    }
    await upsertDailySnapshot(product.id, productData);
    await ensureSelected(product);
    enqueueAiCategorySync({
        id: product.id,
        name: productData.name,
        shopName: productData.shopName,
        category: productData.category
    });

    return {
        message: '商品已加入你的选品池（不会展示到商品总库）',
        product,
        existed: false,
        selected: true
    };
}

app.get('/auth/me', (req, res) => {
    if (req.currentUser && req.currentUser.role === 'admin') {
        return res.json({
            authenticated: false,
            user: null
        });
    }

    res.json({
        authenticated: Boolean(req.currentUser),
        user: getSafeUser(req.currentUser)
    });
});

app.post('/auth/register', async (req, res) => {
    try {
        const { username, password, inviteCode } = validateRegistrationInput(
            req.body?.username,
            req.body?.password,
            req.body?.inviteCode
        );

        const user = await createUserWithInvite(username, hashPassword(password), inviteCode);
        const sessionToken = createSessionToken();
        const expiresAt = buildSessionExpiryDate();
        const cookieName = user.role === 'admin' ? ADMIN_SESSION_COOKIE_NAME : SESSION_COOKIE_NAME;

        await createSession(user.id, sessionToken, expiresAt);
        res.setHeader('Set-Cookie', [
            serializeSessionCookie(sessionToken, expiresAt, cookieName),
            serializeClearSessionCookie(user.role === 'admin' ? SESSION_COOKIE_NAME : ADMIN_SESSION_COOKIE_NAME)
        ]);
        res.status(201).json({
            message: user.role === 'admin' ? '管理员账号创建成功，请前往管理后台登录' : '注册成功',
            user: getSafeUser(user),
            portal: user.role === 'admin' ? 'admin' : 'client'
        });
    } catch (error) {
        console.error('注册失败:', error.message);
        res.status(400).json({ error: error.message || '注册失败' });
    }
});

app.post('/auth/login', async (req, res) => {
    try {
        const { username, password } = validateLoginInput(req.body?.username, req.body?.password);
        const user = await getUserByUsername(username);

        if (!user || !verifyPassword(password, user.password_hash)) {
            return res.status(401).json({ error: '用户名或密码错误' });
        }

        if (!user.is_active) {
            return res.status(403).json({ error: '账号已被停用，请联系管理员' });
        }

        if (user.role === 'admin') {
            return res.status(403).json({ error: '管理员账号请使用管理后台登录入口' });
        }

        const sessionToken = createSessionToken();
        const expiresAt = buildSessionExpiryDate();

        await createSession(user.id, sessionToken, expiresAt);
        res.setHeader('Set-Cookie', [
            serializeSessionCookie(sessionToken, expiresAt, SESSION_COOKIE_NAME),
            serializeClearSessionCookie(ADMIN_SESSION_COOKIE_NAME)
        ]);
        res.json({
            message: '登录成功',
            user: getSafeUser(user)
        });
    } catch (error) {
        console.error('登录失败:', error.message);
        res.status(400).json({ error: error.message || '登录失败' });
    }
});

app.post('/auth/logout', async (req, res) => {
    try {
        if (req.sessionToken) {
            await deleteSession(req.sessionToken);
        }

        res.setHeader('Set-Cookie', [
            serializeClearSessionCookie(SESSION_COOKIE_NAME),
            serializeClearSessionCookie(ADMIN_SESSION_COOKIE_NAME)
        ]);
        res.json({ message: '已退出登录' });
    } catch (error) {
        console.error('退出登录失败:', error.message);
        res.status(500).json({ error: '退出登录失败' });
    }
});

app.post('/api/membership/redeem', requireAuth, async (req, res) => {
    try {
        const code = String(req.body?.code || '').trim();
        if (!code) {
            return res.status(400).json({ error: '请输入兑换码' });
        }

        const renewedUser = await redeemInviteCodeForUser(req.currentUser.id, code);
        res.json({
            message: '续费成功，会员权益已恢复',
            user: getSafeUser(renewedUser)
        });
    } catch (error) {
        console.error('会员续费失败:', error.message);
        res.status(400).json({ error: error.message || '续费失败' });
    }
});

app.get('/admin/auth/me', (req, res) => {
    res.json({
        authenticated: Boolean(req.currentAdmin),
        user: getSafeUser(req.currentAdmin)
    });
});

app.post('/admin/auth/login', async (req, res) => {
    try {
        const { username, password } = validateLoginInput(req.body?.username, req.body?.password);
        const user = await getUserByUsername(username);

        if (!user || !verifyPassword(password, user.password_hash)) {
            return res.status(401).json({ error: '用户名或密码错误' });
        }

        if (!user.is_active) {
            return res.status(403).json({ error: '账号已被停用，请联系管理员' });
        }

        if (user.role !== 'admin') {
            return res.status(403).json({ error: '仅管理员可登录管理后台' });
        }

        const sessionToken = createSessionToken();
        const expiresAt = buildSessionExpiryDate();
        await createSession(user.id, sessionToken, expiresAt);
        res.setHeader('Set-Cookie', [
            serializeSessionCookie(sessionToken, expiresAt, ADMIN_SESSION_COOKIE_NAME),
            serializeClearSessionCookie(SESSION_COOKIE_NAME)
        ]);
        res.json({
            message: '登录成功',
            user: getSafeUser(user)
        });
    } catch (error) {
        console.error('管理后台登录失败:', error.message);
        res.status(400).json({ error: error.message || '登录失败' });
    }
});

app.post('/admin/auth/logout', async (req, res) => {
    try {
        if (req.adminSessionToken) {
            await deleteSession(req.adminSessionToken);
        }

        res.setHeader('Set-Cookie', [
            serializeClearSessionCookie(ADMIN_SESSION_COOKIE_NAME),
            serializeClearSessionCookie(SESSION_COOKIE_NAME)
        ]);
        res.json({ message: '已退出登录' });
    } catch (error) {
        console.error('管理后台退出登录失败:', error.message);
        res.status(500).json({ error: '退出登录失败' });
    }
});

app.get('/admin/users', requireAdminAuth, async (req, res) => {
    try {
        res.json(await listUsersWithStats());
    } catch (error) {
        console.error('获取用户列表失败:', error.message);
        res.status(500).json({ error: '获取用户列表失败' });
    }
});

app.patch('/admin/users/:id', requireAdminAuth, async (req, res) => {
    const targetUserId = Number(req.params.id);

    try {
        if (req.body.role !== undefined) {
            await updateUserRole(targetUserId, req.body.role, req.currentAdmin.id);
        }

        if (req.body.isActive !== undefined) {
            await updateUserActiveStatus(targetUserId, Boolean(req.body.isActive), req.currentAdmin.id);
        }

        res.json({ message: '用户信息已更新' });
    } catch (error) {
        console.error('更新用户失败:', error.message);
        res.status(400).json({ error: error.message || '更新用户失败' });
    }
});

app.get('/admin/invite-codes', requireAdminAuth, async (req, res) => {
    try {
        res.json(await listInviteCodes());
    } catch (error) {
        console.error('获取兑换码失败:', error.message);
        res.status(500).json({ error: '获取兑换码失败' });
    }
});

app.post('/admin/invite-codes', requireAdminAuth, async (req, res) => {
    try {
        const inviteCode = await createInviteCode({
            code: req.body?.code,
            description: req.body?.description,
            maxUses: req.body?.maxUses,
            durationDays: req.body?.durationDays
        });
        res.status(201).json({
            message: '兑换码已创建',
            inviteCode
        });
    } catch (error) {
        console.error('创建兑换码失败:', error.message);
        res.status(400).json({ error: error.message || '创建兑换码失败' });
    }
});

app.patch('/admin/invite-codes/:id', requireAdminAuth, async (req, res) => {
    try {
        await updateInviteCode(Number(req.params.id), {
            description: req.body?.description,
            maxUses: req.body?.maxUses,
            durationDays: req.body?.durationDays,
            isActive: req.body?.isActive
        });
        res.json({ message: '兑换码已更新' });
    } catch (error) {
        console.error('更新兑换码失败:', error.message);
        res.status(400).json({ error: error.message || '更新兑换码失败' });
    }
});

app.patch('/admin/products/:id/category', requireAdminAuth, async (req, res) => {
    const productId = Number(req.params.id);
    const category = String(req.body?.category || '').trim();

    if (!productId) {
        return res.status(400).json({ error: '商品 ID 无效' });
    }

    if (!category) {
        return res.status(400).json({ error: '请选择一个商品类目' });
    }

        try {
            const product = await getProductById(productId);
            if (!product) {
                return res.status(404).json({ error: '商品不存在' });
            }

            aiCategoryQueue.delete(String(productId));
            await updateProductCategory(productId, category);
            res.json({ message: '商品类目已更新' });
        } catch (error) {
        console.error('更新商品类目失败:', error.message);
        res.status(400).json({ error: error.message || '更新商品类目失败' });
    }
});

// 添加商品（管理员进入总库，普通用户进入个人私有选品池）
app.post('/api/products', requireAnyAuth, async (req, res) => {
    const { url } = req.body;

    if (!url) {
        return res.status(400).json({ error: '请提供商品链接或分享文本' });
    }

    try {
        if (!ensureMembershipActive(req.actorUser, res)) {
            return;
        }
        console.log('收到添加商品请求:', url);
        const result = await addProductToLibraryForUser(req.actorUser, url);
        console.log('商品添加成功:', result.product);
        res.json(result);

    } catch (error) {
        console.error('添加商品失败:', error);
        const statusCode = error.message && error.message.includes('最多同时监控') ? 400 : 500;
        res.status(statusCode).json({ error: '添加商品失败: ' + error.message });
    }
});

app.post('/api/products/import', requireAnyAuth, async (req, res) => {
    const items = Array.isArray(req.body?.items) ? req.body.items : [];
    const cleanedItems = items
        .map(item => String(item || '').trim())
        .filter(Boolean);

    if (cleanedItems.length === 0) {
        return res.status(400).json({ error: '请提供要导入的商品链接' });
    }

    const uniqueItems = Array.from(new Set(cleanedItems));
    try {
        if (!ensureMembershipActive(req.actorUser, res)) {
            return;
        }
        const activeJob = getActiveUserJob(importJobs, req.actorUser);
        if (activeJob && (activeJob.status === 'queued' || activeJob.status === 'running')) {
            return res.status(202).json({
                message: '已有导入任务进行中',
                job: buildImportJobSummary(activeJob)
            });
        }

        const job = createImportJob(uniqueItems.length, 'manual', req.actorUser);
        markImportJobStarted(job);

        (async () => {
            try {
                await addProductsToLibraryForUserBatch(req.actorUser, uniqueItems, {
                    concurrency: 3,
                    onProgress(result, counts) {
                        job.successCount = counts.successCount;
                        job.failCount = counts.failCount;
                        markImportJobProgress(job, result);
                    }
                });

                markImportJobFinished(job, 'completed');
            } catch (error) {
                console.error('后台批量导入失败:', error);
                markImportJobFinished(job, 'failed', error.message);
            }
        })();

        res.status(202).json({
            message: '批量导入任务已启动',
            job: buildImportJobSummary(job)
        });
    } catch (error) {
        console.error('创建批量导入任务失败:', error);
        res.status(500).json({ error: '创建批量导入任务失败: ' + error.message });
    }
});

// 获取所有商品数据
app.get('/api/products', requireAnyAuth, async (req, res) => {
    try {
        const actor = req.actorUser;
        if (!ensureMembershipActive(actor, res)) {
            return;
        }
        res.json(await queryProductsWithMetrics(actor.id, {
            view: req.query?.view,
            search: req.query?.q,
            categories: req.query?.categories,
            category: req.query?.category,
            minPrice: req.query?.minPrice,
            maxPrice: req.query?.maxPrice,
            minTotalSales: req.query?.minTotalSales,
            maxTotalSales: req.query?.maxTotalSales,
            minDailySales: req.query?.minDailySales,
            maxDailySales: req.query?.maxDailySales,
            sortBy: req.query?.sortBy,
            sortOrder: req.query?.sortOrder,
            page: req.query?.page,
            pageSize: req.query?.pageSize
        }));
    } catch (error) {
        console.error('获取商品数据失败:', error);
        res.status(500).json({ error: '获取数据失败' });
    }
});

// 刷新单个商品数据（仅管理后台）
app.post('/api/products/:id/refresh', requireAdminAuth, async (req, res) => {
    const productId = parseInt(req.params.id);

    const product = await getProductById(productId);
    if (!product) {
        return res.status(404).json({ error: '商品不存在' });
    }

    try {
        console.log('刷新商品数据:', product.url);
        const productData = await scrapeProductData(product.url);
        if (product.category_source === 'manual') {
            productData.category = product.category;
            productData.categorySource = 'manual';
            productData.categoryStatus = 'manual';
        } else if (product.category_source === 'ai' && product.category_status === 'completed' && hasResolvedCategory(product.category)) {
            productData.category = product.category;
            productData.categorySource = 'ai';
            productData.categoryStatus = 'completed';
        }

        const now = new Date();
        const sanitizedProductData = await applySnapshotGuard(productId, productData, 'single-refresh', {
            existingProductName: product.name,
            existingShopName: product.shopName || product.shop_name
        });
        await updateProductSnapshot(productId, sanitizedProductData, now);
        await upsertDailySnapshot(productId, sanitizedProductData, now);
        if (product.category_source !== 'manual') {
            enqueueAiCategorySync({
                id: productId,
                name: sanitizedProductData.name,
                shopName: sanitizedProductData.shopName,
                category: sanitizedProductData.category,
                category_source: sanitizedProductData.categorySource,
                category_status: sanitizedProductData.categoryStatus,
                category_status_updated_at: product.category_status_updated_at,
                updated_at: now.toISOString()
            });
        }

        res.json({ message: '数据刷新成功', data: sanitizedProductData });

    } catch (error) {
        console.error('刷新数据失败:', error);
        res.status(500).json({ error: '刷新数据失败: ' + error.message });
    }
});

// 获取商品销量趋势数据
app.get('/api/products/:id/trend', requireAnyAuth, async (req, res) => {
    const productId = parseInt(req.params.id);

    try {
        if (!ensureMembershipActive(req.actorUser, res)) {
            return;
        }
        const trendSource = await getTrendData(productId);
        if (!trendSource) {
            return res.status(404).json({ error: '商品不存在' });
        }

        const { product, salesRows: productSalesData } = trendSource;

        if (productSalesData.length === 0) {
            return res.json({
                productName: product.name,
                totalSales: product.productSales || 0,
                avgDailySales: 0,
                maxDailySales: 0,
                monitorDays: 0,
                chartData: {
                    dates: [],
                    totalSales: [],
                    dailySales: []
                }
            });
        }

        // 准备图表数据
        const chartData = {
            dates: [],
            totalSales: [],
            dailySales: []
        };

        let previousSales = 0;
        let totalDailySales = 0;
        let maxDailySales = 0;

        productSalesData.forEach((data, index) => {
            const date = new Date(data.crawl_date);
            const dateStr = date.toLocaleDateString('zh-CN', {
                month: 'short',
                day: 'numeric'
            });

            chartData.dates.push(dateStr);
            chartData.totalSales.push(data.product_sales);

            // 计算日销量（首日没有对比基线，返回 null）
            let dailySales = null;
            if (index > 0) {
                dailySales = Math.max(0, data.product_sales - previousSales);
                totalDailySales += dailySales;
                maxDailySales = Math.max(maxDailySales, dailySales);
            }
            chartData.dailySales.push(dailySales);

            previousSales = data.product_sales;
        });

        // 计算统计数据
        const monitorDays = productSalesData.length;
        const avgDailySales = monitorDays > 1 ? Math.round(totalDailySales / (monitorDays - 1)) : 0;
        const currentTotalSales = productSalesData[productSalesData.length - 1].product_sales;

        res.json({
            productName: product.name,
            totalSales: currentTotalSales,
            avgDailySales: avgDailySales,
            maxDailySales: maxDailySales,
            monitorDays: monitorDays,
            chartData: chartData
        });

    } catch (error) {
        console.error('获取趋势数据失败:', error);
        res.status(500).json({ error: '获取趋势数据失败' });
    }
});

// 删除商品
app.delete('/api/products/:id', requireAdminAuth, async (req, res) => {
    const productId = Number(req.params.id);

    try {
        await deleteProduct(productId);
        res.json({ message: '商品已从商品库删除' });
    } catch (error) {
        console.error('删除商品失败:', error);
        res.status(500).json({ error: '删除商品失败' });
    }
});

app.post('/api/products/:id/select', requireAuth, async (req, res) => {
    const productId = parseInt(req.params.id);

    try {
        if (!ensureMembershipActive(req.currentUser, res)) {
            return;
        }
        const product = await getProductById(productId);
        if (!product) {
            return res.status(404).json({ error: '商品不存在' });
        }

        const isPrivateProduct = product.visibility_scope === 'private';
        const isOwner = product.owner_user_id === req.currentUser.id;
        const isAdminUser = req.currentUser.role === 'admin';
        if (isPrivateProduct && !isOwner && !isAdminUser) {
            return res.status(403).json({ error: '无权添加该私有商品到选品' });
        }

        const alreadySelected = await isProductSelectedByUser(req.currentUser.id, productId);
        if (!alreadySelected) {
            await assertMonitorQuotaAvailable(req.currentUser);
            await addUserSelection(req.currentUser.id, productId);
        }
        res.json({ message: '已加入你的选品' });
    } catch (error) {
        console.error('添加选品失败:', error.message);
        const statusCode = error.message && error.message.includes('最多同时监控') ? 400 : 500;
        res.status(statusCode).json({ error: error.message || '添加选品失败' });
    }
});

app.delete('/api/products/:id/select', requireAuth, async (req, res) => {
    const productId = parseInt(req.params.id);

    try {
        if (!ensureMembershipActive(req.currentUser, res)) {
            return;
        }
        await removeUserSelection(req.currentUser.id, productId);
        res.json({ message: '已从你的选品中移除' });
    } catch (error) {
        console.error('移除选品失败:', error.message);
        res.status(500).json({ error: '移除选品失败' });
    }
});

app.post('/api/products/refresh-all', requireAdminAuth, async (req, res) => {
    try {
        const activeJob = activeRefreshJobId ? refreshJobs.get(activeRefreshJobId) : null;
        if (activeJob && (activeJob.status === 'queued' || activeJob.status === 'running')) {
            if (!isUserJobOwner(activeJob, req.currentAdmin)) {
                return res.status(409).json({
                    error: '当前系统已有刷新任务在进行中，请稍后再试'
                });
            }
            return res.status(202).json({
                message: '已有刷新任务进行中',
                job: buildRefreshJobSummary(activeJob)
            });
        }

        const productList = await listProductsWithMetrics();
        if (productList.length === 0) {
            return res.json({
                message: '没有商品需要刷新',
                total: 0,
                successCount: 0,
                failCount: 0,
                results: []
            });
        }

        console.log(`================================`);
        console.log(`开始手动批量刷新 (${new Date().toLocaleString()})`);
        console.log(`需要刷新的商品数量: ${productList.length}`);
        console.log(`================================`);

        const job = createRefreshJob(productList.length, 'manual', req.currentAdmin);
        markRefreshJobStarted(job);

        (async () => {
            try {
                const summary = await refreshProductsBatch(productList, {
                    concurrency: 2,
                    pauseMs: 150,
                    onProgress(result, counts) {
                        job.successCount = counts.successCount;
                        job.failCount = counts.failCount;
                        markRefreshJobProgress(job, result);
                    }
                });

                console.log(`================================`);
                console.log(`手动批量刷新完成 (${new Date().toLocaleString()})`);
                console.log(`成功: ${summary.successCount} 个, 失败: ${summary.failCount} 个`);
                console.log(`================================`);

                markRefreshJobFinished(job, 'completed');
            } catch (error) {
                console.error('后台批量刷新失败:', error);
                markRefreshJobFinished(job, 'failed', error.message);
            }
        })();

        res.status(202).json({
            message: '批量刷新任务已启动',
            job: buildRefreshJobSummary(job)
        });
    } catch (error) {
        console.error('批量刷新失败:', error);
        res.status(500).json({ error: '批量刷新失败: ' + error.message });
    }
});

app.get('/api/refresh-jobs/active', requireAdminAuth, (req, res) => {
    const activeJob = getVisibleActiveJob(refreshJobs, activeRefreshJobId, req.currentAdmin);
    if (!activeJob) {
        return res.status(204).end();
    }

    res.json(buildRefreshJobSummary(activeJob));
});

app.get('/api/refresh-jobs/:id', requireAdminAuth, (req, res) => {
    const job = refreshJobs.get(req.params.id);
    if (!job) {
        return res.status(404).json({ error: '刷新任务不存在' });
    }
    if (!isUserJobOwner(job, req.currentAdmin)) {
        return res.status(403).json({ error: '无权查看该刷新任务' });
    }

    res.json(buildRefreshJobSummary(job));
});

app.get('/api/import-jobs/active', requireAnyAuth, (req, res) => {
    const activeJob = getActiveUserJob(importJobs, req.actorUser);
    if (!activeJob) {
        return res.status(204).end();
    }

    res.json(buildImportJobSummary(activeJob));
});

app.get('/api/import-jobs/:id', requireAnyAuth, (req, res) => {
    const job = importJobs.get(req.params.id);
    if (!job) {
        return res.status(404).json({ error: '导入任务不存在' });
    }
    if (!isUserJobOwner(job, req.actorUser)) {
        return res.status(403).json({ error: '无权查看该导入任务' });
    }

    res.json(buildImportJobSummary(job));
});

// 自动刷新所有商品数据的函数
async function autoRefreshAllProducts() {
    const activeJob = activeRefreshJobId ? refreshJobs.get(activeRefreshJobId) : null;
    if (activeJob && (activeJob.status === 'queued' || activeJob.status === 'running')) {
        console.log('已有刷新任务进行中，跳过本次自动刷新');
        return;
    }

    const products = await listProductsWithMetrics();
    if (products.length === 0) {
        console.log('没有商品需要刷新');
        return;
    }

    console.log(`================================`);
    console.log(`开始自动刷新所有商品数据 (${new Date().toLocaleString()})`);
    console.log(`需要刷新的商品数量: ${products.length}`);
    console.log(`================================`);

    const job = createRefreshJob(products.length, 'auto');
    markRefreshJobStarted(job);

    try {
        const summary = await refreshProductsBatch(products, {
            concurrency: 2,
            pauseMs: 150,
            onProgress(result, counts) {
                job.successCount = counts.successCount;
                job.failCount = counts.failCount;
                markRefreshJobProgress(job, result);
            }
        });

        console.log(`================================`);
        console.log(`自动刷新完成 (${new Date().toLocaleString()})`);
        console.log(`成功: ${summary.successCount} 个, 失败: ${summary.failCount} 个`);
        console.log(`下次自动刷新时间: ${new Date(Date.now() + 60 * 60 * 1000).toLocaleString()}`);
        console.log(`================================`);
        markRefreshJobFinished(job, 'completed');
    } catch (error) {
        markRefreshJobFinished(job, 'failed', error.message);
        throw error;
    }
}

// 设置定时任务：每小时自动刷新所有商品数据
cron.schedule('0 * * * *', async () => {
    console.log('⏰ 定时任务触发：开始自动刷新商品数据...');
    await autoRefreshAllProducts();
}, {
    timezone: "Asia/Shanghai"
});

// 健康检查端点
app.get('/health', async (req, res) => {
    try {
        const products = await listProductsWithMetrics();
        res.status(200).json({
            status: 'healthy',
            storage: STORAGE_DRIVER,
            timestamp: new Date().toISOString(),
            products: products.length,
            uptime: process.uptime()
        });
    } catch (error) {
        res.status(500).json({
            status: 'unhealthy',
            storage: STORAGE_DRIVER,
            timestamp: new Date().toISOString(),
            error: error.message
        });
    }
});

async function startServer() {
    await verifyConnection();
    const products = await listProductsWithMetrics();

    const server = app.listen(PORT, '0.0.0.0', () => {
        console.log(`================================`);
        console.log(`小红书监控系统已启动`);
        console.log(`访问地址: http://localhost:${PORT}`);
        console.log(`================================`);
        console.log(`当前商品数量: ${products.length}`);
        console.log(`数据存储: ${STORAGE_DRIVER}`);
        console.log(`数据库文件: ${DB_PATH}`);
        console.log(`AI类目分类: ${hasDoubaoConfig() ? `${getAiProviderName()} 已启用（后台补分类）` : '未启用，使用本地规则'}`);
        console.log(`================================`);
        console.log('⏰ 自动刷新功能已启用');
        console.log('📅 刷新频率: 每小时一次');
        console.log('🕐 下次刷新时间: 每小时的0分');
        console.log(`================================`);

        // 启动后5分钟执行一次初始刷新（可选）
        setTimeout(async () => {
            console.log('🚀 执行启动后的初始数据刷新...');
            await autoRefreshAllProducts();
        }, 5 * 60 * 1000); // 5分钟后执行

        if (AI_BOOT_SYNC_ENABLED) {
            setTimeout(async () => {
                await syncExistingCategoriesWithAi();
            }, 2000);
        } else {
            console.log('🤖 AI 启动补分类已关闭（AI_BOOT_SYNC_ENABLED=false）');
        }
    });

    return server;
}

startServer().catch(error => {
    console.error('服务启动失败:', error.message);
    process.exit(1);
});
