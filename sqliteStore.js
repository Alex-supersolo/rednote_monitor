const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { DatabaseSync } = require('node:sqlite');
const {
    CATEGORIES,
    normalizeCategory,
    inferProductCategoryFromText
} = require('./productCategory');

const DATA_DIR = path.join(__dirname, 'data');
const DB_PATH = process.env.SQLITE_PATH
    ? path.resolve(process.cwd(), process.env.SQLITE_PATH)
    : path.join(DATA_DIR, 'monitor.db');
const PRODUCTS_JSON_PATH = path.join(DATA_DIR, 'products.json');
const SALES_JSON_PATH = path.join(DATA_DIR, 'sales_data.json');

let db = null;
let initialized = false;

function ensureDataDir() {
    fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
}

function readJsonArray(filePath) {
    if (!fs.existsSync(filePath)) {
        return [];
    }

    try {
        const content = fs.readFileSync(filePath, 'utf8');
        const parsed = JSON.parse(content);
        return Array.isArray(parsed) ? parsed : [];
    } catch (error) {
        console.warn(`读取 ${path.basename(filePath)} 失败，跳过自动导入:`, error.message);
        return [];
    }
}

function formatLocalDate(date = new Date()) {
    const current = new Date(date);
    const year = current.getFullYear();
    const month = String(current.getMonth() + 1).padStart(2, '0');
    const day = String(current.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
}

function normalizeVisibilityScope(value) {
    return value === 'private' ? 'private' : 'public';
}

function toPositiveInteger(value) {
    const parsed = Number(value);
    return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function getDb() {
    if (!db) {
        ensureDataDir();
        db = new DatabaseSync(DB_PATH);
        db.exec('PRAGMA foreign_keys = ON');
        db.exec('PRAGMA busy_timeout = 5000');
        db.exec('PRAGMA journal_mode = WAL');
    }

    if (!initialized) {
        initializeDatabase();
        initialized = true;
    }

    return db;
}

function initializeDatabase() {
    const database = db;

    database.exec(`
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            image_url TEXT,
            category TEXT NOT NULL DEFAULT '其他',
            category_source TEXT NOT NULL DEFAULT 'rule',
            category_status TEXT NOT NULL DEFAULT 'rule_only',
            category_status_updated_at TEXT,
            visibility_scope TEXT NOT NULL DEFAULT 'public',
            owner_user_id INTEGER,
            price REAL NOT NULL DEFAULT 0,
            product_sales INTEGER NOT NULL DEFAULT 0,
            shop_name TEXT NOT NULL DEFAULT '未知店铺',
            shop_sales INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sales_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            product_sales INTEGER NOT NULL DEFAULT 0,
            shop_sales INTEGER NOT NULL DEFAULT 0,
            crawl_date TEXT NOT NULL,
            crawl_time TEXT NOT NULL,
            UNIQUE(product_id, crawl_date),
            FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'member',
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS invite_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,
            description TEXT NOT NULL DEFAULT '',
            max_uses INTEGER,
            used_count INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            expires_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS user_product_selections (
            user_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (user_id, product_id),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_sales_product_date
        ON sales_data(product_id, crawl_date, crawl_time);

        CREATE INDEX IF NOT EXISTS idx_sessions_user_id
        ON sessions(user_id);

        CREATE INDEX IF NOT EXISTS idx_sessions_expires_at
        ON sessions(expires_at);

        CREATE INDEX IF NOT EXISTS idx_selections_product_id
        ON user_product_selections(product_id);
    `);

    ensureProductColumns(database);
    ensureUserColumns(database);
    normalizeInviteCodeLimits(database);
    seedInviteCodes(database);
    bootstrapFromJsonIfNeeded(database);
    bootstrapAdminRole(database);
}

function ensureProductColumns(database) {
    const columns = database.prepare('PRAGMA table_info(products)').all();
    const columnNames = new Set(columns.map(column => column.name));

    if (!columnNames.has('image_url')) {
        database.exec('ALTER TABLE products ADD COLUMN image_url TEXT');
    }

    if (!columnNames.has('category')) {
        database.exec("ALTER TABLE products ADD COLUMN category TEXT NOT NULL DEFAULT '其他'");
    }

    if (!columnNames.has('category_source')) {
        database.exec("ALTER TABLE products ADD COLUMN category_source TEXT NOT NULL DEFAULT 'rule'");
    }

    if (!columnNames.has('category_status')) {
        database.exec("ALTER TABLE products ADD COLUMN category_status TEXT NOT NULL DEFAULT 'rule_only'");
    }

    if (!columnNames.has('category_status_updated_at')) {
        database.exec('ALTER TABLE products ADD COLUMN category_status_updated_at TEXT');
    }

    if (!columnNames.has('visibility_scope')) {
        database.exec("ALTER TABLE products ADD COLUMN visibility_scope TEXT NOT NULL DEFAULT 'public'");
    }

    if (!columnNames.has('owner_user_id')) {
        database.exec('ALTER TABLE products ADD COLUMN owner_user_id INTEGER');
    }

    database.exec(`
        UPDATE products
        SET category_source = COALESCE(NULLIF(category_source, ''), 'rule'),
            category_status = COALESCE(NULLIF(category_status, ''), 'rule_only'),
            category_status_updated_at = COALESCE(category_status_updated_at, updated_at, created_at),
            visibility_scope = CASE
                WHEN visibility_scope = 'private' THEN 'private'
                ELSE 'public'
            END,
            owner_user_id = CASE
                WHEN visibility_scope = 'private' THEN owner_user_id
                ELSE NULL
            END
    `);

    database.exec(`
        CREATE INDEX IF NOT EXISTS idx_products_visibility_scope
        ON products(visibility_scope);

        CREATE INDEX IF NOT EXISTS idx_products_owner_user_id
        ON products(owner_user_id);
    `);
}

function ensureUserColumns(database) {
    const columns = database.prepare('PRAGMA table_info(users)').all();
    const columnNames = new Set(columns.map(column => column.name));

    if (!columnNames.has('role')) {
        database.exec("ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'member'");
    }

    if (!columnNames.has('is_active')) {
        database.exec('ALTER TABLE users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1');
    }
}

function normalizeInviteCodeLimits(database) {
    database.exec(`
        UPDATE invite_codes
        SET max_uses = 1
        WHERE max_uses IS NULL
    `);
}

function seedInviteCodes(database) {
    const rawCodes = (process.env.INVITE_CODES || process.env.INITIAL_INVITE_CODES || '')
        .split(/[\n,]/)
        .map(code => code.trim())
        .filter(Boolean);

    if (rawCodes.length === 0) {
        return;
    }

    const insertInviteCode = database.prepare(`
        INSERT INTO invite_codes (code, description, max_uses, created_at)
        VALUES (?, ?, 1, ?)
        ON CONFLICT(code) DO NOTHING
    `);
    const nowIso = new Date().toISOString();

    database.exec('BEGIN');
    try {
        for (const code of rawCodes) {
            insertInviteCode.run(code, '环境变量预置邀请码', nowIso);
        }
        database.exec('COMMIT');
    } catch (error) {
        database.exec('ROLLBACK');
        throw error;
    }
}

function bootstrapFromJsonIfNeeded(database) {
    const productCount = database.prepare('SELECT COUNT(*) AS count FROM products').get().count;
    const salesCount = database.prepare('SELECT COUNT(*) AS count FROM sales_data').get().count;

    if (productCount > 0 || salesCount > 0) {
        return;
    }

    const products = readJsonArray(PRODUCTS_JSON_PATH);
    const salesRows = readJsonArray(SALES_JSON_PATH);

    if (products.length === 0 && salesRows.length === 0) {
        return;
    }

    const insertProduct = database.prepare(`
        INSERT INTO products (
            id, url, name, image_url, category, category_source, category_status, category_status_updated_at, visibility_scope, owner_user_id, price, product_sales, shop_name, shop_sales, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const insertSales = database.prepare(`
        INSERT INTO sales_data (
            product_id, product_sales, shop_sales, crawl_date, crawl_time
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(product_id, crawl_date) DO UPDATE SET
            product_sales = excluded.product_sales,
            shop_sales = excluded.shop_sales,
            crawl_time = excluded.crawl_time
    `);

    database.exec('BEGIN');
    try {
        for (const product of products) {
            const createdAt = product.created_at || new Date().toISOString();
            insertProduct.run(
                product.id,
                product.url,
                product.name || '未命名商品',
                product.imageUrl || product.image_url || '',
                normalizeCategory(product.category || inferProductCategoryFromText(product.name, product.shopName)),
                'rule',
                'rule_only',
                product.updated_at || createdAt,
                'public',
                null,
                Number(product.price || 0),
                Number(product.productSales || 0),
                product.shopName || '未知店铺',
                Number(product.shopSales || 0),
                createdAt,
                product.updated_at || createdAt
            );
        }

        for (const row of salesRows) {
            insertSales.run(
                row.product_id,
                Number(row.product_sales || 0),
                Number(row.shop_sales || 0),
                row.crawl_date,
                row.crawl_time || new Date().toISOString()
            );
        }

        database.exec('COMMIT');
    } catch (error) {
        database.exec('ROLLBACK');
        throw error;
    }

    console.log(`已从 JSON 导入 SQLite: ${products.length} 个商品，${salesRows.length} 条销量记录`);
}

function bootstrapAdminRole(database) {
    const ownerUsernames = (process.env.ADMIN_USERNAMES || process.env.OWNER_USERNAMES || '')
        .split(/[\n,]/)
        .map(item => item.trim())
        .filter(Boolean);

    if (ownerUsernames.length > 0) {
        const placeholders = ownerUsernames.map(() => '?').join(', ');
        database.prepare(`
            UPDATE users
            SET role = 'admin', is_active = 1
            WHERE username IN (${placeholders})
        `).run(...ownerUsernames);
    }

    const adminCount = database.prepare(`
        SELECT COUNT(*) AS count
        FROM users
        WHERE role = 'admin' AND is_active = 1
    `).get().count;

    if (adminCount > 0) {
        return;
    }

    const firstUser = database.prepare(`
        SELECT id
        FROM users
        ORDER BY id ASC
        LIMIT 1
    `).get();

    if (firstUser) {
        database.prepare(`
            UPDATE users
            SET role = 'admin'
            WHERE id = ?
        `).run(firstUser.id);
    }
}

function mapProductRow(row) {
    if (!row) {
        return null;
    }

    const inferredCategory = inferProductCategoryFromText(row.name, row.shop_name);
    const category = row.category && row.category !== '其他'
        ? normalizeCategory(row.category)
        : inferredCategory;

    return {
        id: row.id,
        url: row.url,
        name: row.name,
        imageUrl: row.image_url || '',
        category,
        category_source: row.category_source || 'rule',
        category_status: row.category_status || 'rule_only',
        category_status_updated_at: row.category_status_updated_at || row.updated_at || row.created_at,
        visibility_scope: normalizeVisibilityScope(row.visibility_scope),
        owner_user_id: toPositiveInteger(row.owner_user_id),
        price: Number(row.price || 0),
        productSales: row.product_sales || 0,
        shopName: row.shop_name || '未知店铺',
        shopSales: row.shop_sales || 0,
        created_at: row.created_at,
        updated_at: row.updated_at
    };
}

function mapUserRow(row) {
    if (!row) {
        return null;
    }

    return {
        id: row.id,
        username: row.username,
        role: row.role || 'member',
        is_active: Boolean(row.is_active),
        created_at: row.created_at
    };
}

function getInviteCodeRow(code) {
    return getDb().prepare(`
        SELECT *
        FROM invite_codes
        WHERE code = ? AND is_active = 1
    `).get(String(code || '').trim());
}

function generateInviteCode() {
    return 'TU-' + crypto.randomUUID().replace(/-/g, '').slice(0, 10).toUpperCase();
}

async function verifyConnection() {
    getDb().prepare('SELECT 1').get();
}

async function getProductById(productId) {
    const row = getDb().prepare('SELECT * FROM products WHERE id = ?').get(productId);
    return mapProductRow(row);
}

async function getProductByUrl(url) {
    const row = getDb().prepare('SELECT * FROM products WHERE url = ?').get(url);
    return mapProductRow(row);
}

async function createProduct(url, productData, options = {}) {
    const nowIso = new Date().toISOString();
    const visibilityScope = normalizeVisibilityScope(options.visibilityScope);
    const ownerUserId = visibilityScope === 'private' ? toPositiveInteger(options.ownerUserId) : null;
    const row = getDb().prepare(`
        INSERT INTO products (
            url, name, image_url, category, category_source, category_status, category_status_updated_at, visibility_scope, owner_user_id, price, product_sales, shop_name, shop_sales, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING *
    `).get(
        url,
        productData.name,
        productData.imageUrl || '',
        normalizeCategory(productData.category || inferProductCategoryFromText(productData.name, productData.shopName)),
        String(productData.categorySource || 'rule'),
        String(productData.categoryStatus || 'rule_only'),
        nowIso,
        visibilityScope,
        ownerUserId,
        Number(productData.price || 0),
        Number(productData.productSales || 0),
        productData.shopName || '未知店铺',
        Number(productData.shopSales || 0),
        nowIso,
        nowIso
    );

    return mapProductRow(row);
}

async function updateProductSnapshot(productId, productData, now = new Date()) {
    const nowIso = now.toISOString();
    const row = getDb().prepare(`
        UPDATE products
        SET
            name = ?,
            image_url = ?,
            category = ?,
            category_source = ?,
            category_status = ?,
            category_status_updated_at = ?,
            price = ?,
            product_sales = ?,
            shop_name = ?,
            shop_sales = ?,
            updated_at = ?
        WHERE id = ?
        RETURNING *
    `).get(
        productData.name,
        productData.imageUrl || '',
        normalizeCategory(productData.category || inferProductCategoryFromText(productData.name, productData.shopName)),
        String(productData.categorySource || 'rule'),
        String(productData.categoryStatus || 'rule_only'),
        nowIso,
        Number(productData.price || 0),
        Number(productData.productSales || 0),
        productData.shopName || '未知店铺',
        Number(productData.shopSales || 0),
        nowIso,
        productId
    );

    return mapProductRow(row);
}

async function upsertDailySnapshot(productId, productData, now = new Date()) {
    getDb().prepare(`
        INSERT INTO sales_data (
            product_id, product_sales, shop_sales, crawl_date, crawl_time
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(product_id, crawl_date) DO UPDATE SET
            product_sales = excluded.product_sales,
            shop_sales = excluded.shop_sales,
            crawl_time = excluded.crawl_time
    `).run(
        productId,
        Number(productData.productSales || 0),
        Number(productData.shopSales || 0),
        formatLocalDate(now),
        now.toISOString()
    );
}

function escapeLikeValue(value) {
    return String(value || '').replace(/[\\%_]/g, '\\$&');
}

function parseOptionalNumber(value) {
    if (value === undefined || value === null || value === '') {
        return null;
    }

    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
}

function normalizeCategoryList(value) {
    const items = Array.isArray(value)
        ? value
        : String(value || '')
            .split(',')
            .map(item => item.trim())
            .filter(Boolean);

    return Array.from(new Set(items.map(normalizeCategory).filter(Boolean)));
}

function sortMetricProducts(items, sortBy = 'id', sortOrder = 'desc') {
    return [...items].sort((a, b) => {
        let valueA = 0;
        let valueB = 0;

        switch (sortBy) {
            case 'price':
                valueA = a.price || 0;
                valueB = b.price || 0;
                break;
            case 'product_total_sales':
                valueA = a.product_total_sales || 0;
                valueB = b.product_total_sales || 0;
                break;
            case 'daily_product_sales':
                valueA = a.daily_product_sales || 0;
                valueB = b.daily_product_sales || 0;
                break;
            case 'daily_gmv':
                valueA = a.daily_gmv || 0;
                valueB = b.daily_gmv || 0;
                break;
            case 'daily_product_sales_growth':
                valueA = Number.isFinite(a.daily_product_sales_growth) ? a.daily_product_sales_growth : Number.NEGATIVE_INFINITY;
                valueB = Number.isFinite(b.daily_product_sales_growth) ? b.daily_product_sales_growth : Number.NEGATIVE_INFINITY;
                break;
            case 'shop_total_sales':
                valueA = a.shop_total_sales || 0;
                valueB = b.shop_total_sales || 0;
                break;
            case 'daily_shop_sales':
                valueA = a.daily_shop_sales || 0;
                valueB = b.daily_shop_sales || 0;
                break;
            default:
                valueA = a.id || 0;
                valueB = b.id || 0;
        }

        return sortOrder === 'asc' ? valueA - valueB : valueB - valueA;
    });
}

async function listProductsWithMetrics(userId = null, options = {}) {
    const search = String(options.search || '').trim();
    const category = String(options.category || '').trim();
    const view = options.view === 'selected' ? 'selected' : 'library';
    const visibility = ['public', 'accessible'].includes(options.visibility)
        ? options.visibility
        : 'all';
    const minPrice = parseOptionalNumber(options.minPrice);
    const maxPrice = parseOptionalNumber(options.maxPrice);
    const minTotalSales = parseOptionalNumber(options.minTotalSales);
    const maxTotalSales = parseOptionalNumber(options.maxTotalSales);
    const minDailySales = parseOptionalNumber(options.minDailySales);
    const maxDailySales = parseOptionalNumber(options.maxDailySales);
    const whereClauses = [];
    const productParams = [];

    if (visibility === 'public') {
        whereClauses.push("visibility_scope = 'public'");
    } else if (visibility === 'accessible') {
        if (userId) {
            whereClauses.push("(visibility_scope = 'public' OR (visibility_scope = 'private' AND owner_user_id = ?))");
            productParams.push(userId);
        } else {
            whereClauses.push("visibility_scope = 'public'");
        }
    }

    if (search) {
        whereClauses.push('name LIKE ? ESCAPE \'\\\' COLLATE NOCASE');
        productParams.push(`%${escapeLikeValue(search)}%`);
    }

    if (category && category !== 'all') {
        whereClauses.push('category = ?');
        productParams.push(normalizeCategory(category));
    }

    if (view === 'selected' && userId) {
        whereClauses.push('id IN (SELECT product_id FROM user_product_selections WHERE user_id = ?)');
        productParams.push(userId);
    }

    if (minPrice !== null) {
        whereClauses.push('price >= ?');
        productParams.push(minPrice);
    }

    if (maxPrice !== null) {
        whereClauses.push('price <= ?');
        productParams.push(maxPrice);
    }

    const productQuery = `
        SELECT * FROM products
        ${whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : ''}
        ORDER BY id DESC
    `;
    const products = getDb()
        .prepare(productQuery)
        .all(...productParams)
        .map(mapProductRow);

    if (products.length === 0) {
        return [];
    }

    const productIds = products.map(product => product.id);
    const salesPlaceholders = productIds.map(() => '?').join(', ');
    const salesRows = getDb().prepare(`
        SELECT product_id, product_sales, shop_sales, crawl_date, crawl_time
        FROM sales_data
        WHERE product_id IN (${salesPlaceholders})
    `).all(...productIds);

    const selectedIds = new Set();
    if (userId) {
        getDb().prepare(`
            SELECT product_id
            FROM user_product_selections
            WHERE user_id = ?
        `).all(userId).forEach(row => selectedIds.add(row.product_id));
    }

    const byProduct = new Map();

    for (const row of salesRows) {
        const rows = byProduct.get(row.product_id) || [];
        rows.push(row);
        byProduct.set(row.product_id, rows);
    }

    const metricProducts = products.map(product => {
        const productSalesRows = byProduct.get(product.id) || [];
        const orderedRows = [...productSalesRows].sort((a, b) => new Date(a.crawl_time) - new Date(b.crawl_time));
        const latestSnapshot = orderedRows.length > 0 ? orderedRows[orderedRows.length - 1] : null;
        const previousSnapshot = orderedRows.length > 1 ? orderedRows[orderedRows.length - 2] : null;
        const prePreviousSnapshot = orderedRows.length > 2 ? orderedRows[orderedRows.length - 3] : null;
        const productTotalSales = latestSnapshot ? latestSnapshot.product_sales : product.productSales;
        const shopTotalSales = latestSnapshot ? latestSnapshot.shop_sales : product.shopSales;
        const hasDailyProductBaseline = Boolean(latestSnapshot && previousSnapshot);
        const hasDailyShopBaseline = Boolean(latestSnapshot && previousSnapshot);
        const dailyProductSales = hasDailyProductBaseline
            ? Math.max(0, latestSnapshot.product_sales - previousSnapshot.product_sales)
            : 0;
        const previousDailyProductSales = previousSnapshot && prePreviousSnapshot
            ? Math.max(0, previousSnapshot.product_sales - prePreviousSnapshot.product_sales)
            : 0;
        const dailyShopSales = hasDailyShopBaseline
            ? Math.max(0, latestSnapshot.shop_sales - previousSnapshot.shop_sales)
            : 0;
        const dailyProductSalesGrowth = previousDailyProductSales > 0
            ? ((dailyProductSales - previousDailyProductSales) / previousDailyProductSales) * 100
            : null;

        return {
            ...product,
            product_total_sales: productTotalSales,
            shop_total_sales: shopTotalSales,
            daily_product_sales: dailyProductSales,
            daily_product_sales_ready: hasDailyProductBaseline,
            previous_daily_product_sales: previousDailyProductSales,
            daily_product_sales_growth: dailyProductSalesGrowth,
            daily_shop_sales: dailyShopSales,
            daily_shop_sales_ready: hasDailyShopBaseline,
            daily_gmv: dailyProductSales * (product.price || 0),
            last_update: latestSnapshot ? latestSnapshot.crawl_time : (product.updated_at || product.created_at),
            shop_name: product.shopName || '未知店铺',
            is_selected: userId ? selectedIds.has(product.id) : false
        };
    }).filter(product => {
        if (minTotalSales !== null && product.product_total_sales < minTotalSales) {
            return false;
        }
        if (maxTotalSales !== null && product.product_total_sales > maxTotalSales) {
            return false;
        }
        if (minDailySales !== null && product.daily_product_sales < minDailySales) {
            return false;
        }
        if (maxDailySales !== null && product.daily_product_sales > maxDailySales) {
            return false;
        }
        return true;
    });

    return sortMetricProducts(metricProducts, options.sortBy, options.sortOrder);
}

async function queryProductsWithMetrics(userId = null, options = {}) {
    const view = options.view === 'selected' ? 'selected' : 'library';
    const visibility = ['public', 'accessible', 'all'].includes(options.visibility)
        ? options.visibility
        : (view === 'library' ? 'public' : 'accessible');
    const categories = normalizeCategoryList(options.categories || options.category);
    const requestedPage = Math.max(1, parseInt(options.page, 10) || 1);
    const pageSize = Math.min(200, Math.max(1, parseInt(options.pageSize, 10) || 20));
    const libraryCount = getDb().prepare(`
        SELECT COUNT(*) AS count
        FROM products
        WHERE visibility_scope = 'public'
    `).get().count;
    const selectedCount = userId
        ? getDb().prepare('SELECT COUNT(*) AS count FROM user_product_selections WHERE user_id = ?').get(userId).count
        : 0;
    const categoryStatusSummaryParams = [];
    const categoryStatusSummaryWhere = visibility === 'public'
        ? "WHERE visibility_scope = 'public'"
        : (visibility === 'accessible'
            ? (userId
                ? "WHERE visibility_scope = 'public' OR (visibility_scope = 'private' AND owner_user_id = ?)"
                : "WHERE visibility_scope = 'public'")
            : '');
    if (visibility === 'accessible' && userId) {
        categoryStatusSummaryParams.push(userId);
    }
    const categoryStatusSummaryRows = getDb().prepare(`
        SELECT category_status, COUNT(*) AS count
        FROM products
        ${categoryStatusSummaryWhere}
        GROUP BY category_status
    `).all(...categoryStatusSummaryParams);
    const categoryStatusSummary = {
        queued: 0,
        processing: 0,
        completed: 0,
        manual: 0,
        failed: 0,
        rule_only: 0
    };
    categoryStatusSummaryRows.forEach(row => {
        const key = row.category_status || 'rule_only';
        if (Object.prototype.hasOwnProperty.call(categoryStatusSummary, key)) {
            categoryStatusSummary[key] = Number(row.count || 0);
        }
    });

    const baseProducts = await listProductsWithMetrics(userId, {
        ...options,
        view,
        visibility,
        category: ''
    });
    const availableCategories = [...CATEGORIES];
    const finalProducts = categories.length > 0
        ? baseProducts.filter(product => categories.includes(product.category))
        : baseProducts;
    const filteredTotal = finalProducts.length;
    const totalPages = filteredTotal === 0 ? 1 : Math.ceil(filteredTotal / pageSize);
    const page = Math.min(requestedPage, totalPages);
    const startIndex = (page - 1) * pageSize;
    const items = finalProducts.slice(startIndex, startIndex + pageSize);

    return {
        items,
        availableCategories,
        filteredTotal,
        page,
        pageSize,
        totalPages,
        libraryCount,
        selectedCount,
        categoryStatusSummary
    };
}

async function getTrendData(productId) {
    const product = await getProductById(productId);
    if (!product) {
        return null;
    }

    const salesRows = getDb().prepare(`
        SELECT product_sales, crawl_date, crawl_time
        FROM sales_data
        WHERE product_id = ?
        ORDER BY crawl_date ASC, crawl_time ASC
    `).all(productId);

    const groupedByDate = new Map();
    for (const row of salesRows) {
        groupedByDate.set(row.crawl_date, row);
    }

    return {
        product,
        salesRows: Array.from(groupedByDate.values())
    };
}

async function deleteProduct(productId) {
    getDb().prepare('DELETE FROM products WHERE id = ?').run(productId);
}

async function updateProductCategory(productId, category, metadata = {}) {
    const nextSource = String(metadata.categorySource || 'manual');
    const nextStatus = String(metadata.categoryStatus || 'manual');
    const nowIso = new Date().toISOString();
    getDb().prepare(`
        UPDATE products
        SET category = ?, category_source = ?, category_status = ?, category_status_updated_at = ?, updated_at = ?
        WHERE id = ?
    `).run(normalizeCategory(category), nextSource, nextStatus, nowIso, nowIso, productId);
}

async function updateProductCategoryState(productId, metadata = {}) {
    const fields = [];
    const values = [];

    if (metadata.categorySource !== undefined) {
        fields.push('category_source = ?');
        values.push(String(metadata.categorySource || 'rule'));
    }

    if (metadata.categoryStatus !== undefined) {
        fields.push('category_status = ?');
        values.push(String(metadata.categoryStatus || 'rule_only'));
    }

    if (fields.length === 0) {
        return;
    }

    const nowIso = new Date().toISOString();
    fields.push('category_status_updated_at = ?');
    values.push(nowIso);
    fields.push('updated_at = ?');
    values.push(nowIso);
    values.push(productId);

    getDb().prepare(`
        UPDATE products
        SET ${fields.join(', ')}
        WHERE id = ?
    `).run(...values);
}

async function updateProductVisibility(productId, visibilityScope = 'public', ownerUserId = null) {
    const nextVisibilityScope = normalizeVisibilityScope(visibilityScope);
    const nextOwnerUserId = nextVisibilityScope === 'private' ? toPositiveInteger(ownerUserId) : null;
    const nowIso = new Date().toISOString();
    getDb().prepare(`
        UPDATE products
        SET visibility_scope = ?, owner_user_id = ?, updated_at = ?
        WHERE id = ?
    `).run(nextVisibilityScope, nextOwnerUserId, nowIso, productId);
}

async function getUserById(userId) {
    const row = getDb().prepare(`
        SELECT id, username, role, is_active, created_at
        FROM users
        WHERE id = ?
    `).get(userId);

    return mapUserRow(row);
}

async function getUserByUsername(username) {
    const row = getDb().prepare(`
        SELECT *
        FROM users
        WHERE username = ?
    `).get(String(username || '').trim());

    return row
        ? {
            ...mapUserRow(row),
            password_hash: row.password_hash
        }
        : null;
}

async function createUserWithInvite(username, passwordHash, inviteCode) {
    const database = getDb();
    const normalizedUsername = String(username || '').trim();
    const normalizedInviteCode = String(inviteCode || '').trim();
    const nowIso = new Date().toISOString();

    database.exec('BEGIN IMMEDIATE');
    try {
        const inviteCodeCount = database.prepare('SELECT COUNT(*) AS count FROM invite_codes WHERE is_active = 1').get().count;
        if (inviteCodeCount === 0) {
            throw new Error('当前系统还未配置邀请码，请联系管理员');
        }

        const existingUser = database.prepare(`
            SELECT id
            FROM users
            WHERE username = ?
        `).get(normalizedUsername);

        if (existingUser) {
            throw new Error('用户名已存在');
        }

        const inviteRow = getInviteCodeRow(normalizedInviteCode);
        if (!inviteRow) {
            throw new Error('邀请码无效或已停用');
        }

        if (inviteRow.max_uses !== null && inviteRow.used_count >= inviteRow.max_uses) {
            throw new Error('邀请码已达到使用上限');
        }

        const adminCount = database.prepare(`
            SELECT COUNT(*) AS count
            FROM users
            WHERE role = 'admin' AND is_active = 1
        `).get().count;
        const role = adminCount === 0 ? 'admin' : 'member';

        const userRow = database.prepare(`
            INSERT INTO users (username, password_hash, role, is_active, created_at)
            VALUES (?, ?, ?, 1, ?)
            RETURNING id, username, role, is_active, created_at
        `).get(normalizedUsername, passwordHash, role, nowIso);

        database.prepare(`
            UPDATE invite_codes
            SET used_count = used_count + 1
            WHERE id = ?
        `).run(inviteRow.id);

        database.exec('COMMIT');
        return mapUserRow(userRow);
    } catch (error) {
        database.exec('ROLLBACK');
        throw error;
    }
}

async function createSession(userId, token, expiresAt) {
    const nowIso = new Date().toISOString();
    const expiresIso = expiresAt instanceof Date ? expiresAt.toISOString() : new Date(expiresAt).toISOString();

    getDb().prepare(`
        INSERT INTO sessions (token, user_id, expires_at, created_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(token) DO UPDATE SET
            user_id = excluded.user_id,
            expires_at = excluded.expires_at
    `).run(token, userId, expiresIso, nowIso);
}

async function getUserBySessionToken(token) {
    if (!token) {
        return null;
    }

    const database = getDb();
    const row = database.prepare(`
        SELECT s.token, s.expires_at, u.id, u.username, u.role, u.is_active, u.created_at
        FROM sessions s
        JOIN users u ON u.id = s.user_id
        WHERE s.token = ?
    `).get(token);

    if (!row) {
        return null;
    }

    if (!row.is_active || new Date(row.expires_at) <= new Date()) {
        database.prepare('DELETE FROM sessions WHERE token = ?').run(token);
        return null;
    }

    return mapUserRow(row);
}

async function deleteSession(token) {
    if (!token) {
        return;
    }

    getDb().prepare('DELETE FROM sessions WHERE token = ?').run(token);
}

async function deleteUserSessions(userId) {
    getDb().prepare('DELETE FROM sessions WHERE user_id = ?').run(userId);
}

async function addUserSelection(userId, productId) {
    getDb().prepare(`
        INSERT INTO user_product_selections (user_id, product_id, created_at)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, product_id) DO NOTHING
    `).run(userId, productId, new Date().toISOString());
}

async function removeUserSelection(userId, productId) {
    getDb().prepare(`
        DELETE FROM user_product_selections
        WHERE user_id = ? AND product_id = ?
    `).run(userId, productId);
}

async function isProductSelectedByUser(userId, productId) {
    const row = getDb().prepare(`
        SELECT 1 AS selected
        FROM user_product_selections
        WHERE user_id = ? AND product_id = ?
    `).get(userId, productId);

    return Boolean(row);
}

async function listUsersWithStats() {
    return getDb().prepare(`
        SELECT
            u.id,
            u.username,
            u.role,
            u.is_active,
            u.created_at,
            COUNT(ups.product_id) AS selection_count
        FROM users u
        LEFT JOIN user_product_selections ups ON ups.user_id = u.id
        GROUP BY u.id
        ORDER BY u.created_at ASC
    `).all().map(row => ({
        ...mapUserRow(row),
        selection_count: Number(row.selection_count || 0)
    }));
}

async function updateUserRole(targetUserId, role, actorUserId) {
    const database = getDb();
    const normalizedRole = role === 'admin' ? 'admin' : 'member';

    database.exec('BEGIN IMMEDIATE');
    try {
        const targetUser = database.prepare(`
            SELECT id, role, is_active
            FROM users
            WHERE id = ?
        `).get(targetUserId);

        if (!targetUser) {
            throw new Error('用户不存在');
        }

        if (targetUser.id === actorUserId && targetUser.role === 'admin' && normalizedRole !== 'admin') {
            const adminCount = database.prepare(`
                SELECT COUNT(*) AS count
                FROM users
                WHERE role = 'admin' AND is_active = 1
            `).get().count;

            if (adminCount <= 1) {
                throw new Error('系统至少需要保留一个启用中的管理员');
            }
        }

        if (targetUser.role === 'admin' && normalizedRole !== 'admin' && targetUser.is_active) {
            const adminCount = database.prepare(`
                SELECT COUNT(*) AS count
                FROM users
                WHERE role = 'admin' AND is_active = 1
            `).get().count;

            if (adminCount <= 1) {
                throw new Error('系统至少需要保留一个启用中的管理员');
            }
        }

        database.prepare(`
            UPDATE users
            SET role = ?
            WHERE id = ?
        `).run(normalizedRole, targetUserId);

        database.exec('COMMIT');
    } catch (error) {
        database.exec('ROLLBACK');
        throw error;
    }
}

async function updateUserActiveStatus(targetUserId, isActive, actorUserId) {
    const database = getDb();
    const activeFlag = isActive ? 1 : 0;

    database.exec('BEGIN IMMEDIATE');
    try {
        const targetUser = database.prepare(`
            SELECT id, role, is_active
            FROM users
            WHERE id = ?
        `).get(targetUserId);

        if (!targetUser) {
            throw new Error('用户不存在');
        }

        if (targetUser.role === 'admin' && targetUser.is_active && !activeFlag) {
            const adminCount = database.prepare(`
                SELECT COUNT(*) AS count
                FROM users
                WHERE role = 'admin' AND is_active = 1
            `).get().count;

            if (adminCount <= 1) {
                throw new Error('系统至少需要保留一个启用中的管理员');
            }
        }

        if (targetUser.id === actorUserId && !activeFlag) {
            throw new Error('不能停用当前登录账号');
        }

        database.prepare(`
            UPDATE users
            SET is_active = ?
            WHERE id = ?
        `).run(activeFlag, targetUserId);

        if (!activeFlag) {
            database.prepare('DELETE FROM sessions WHERE user_id = ?').run(targetUserId);
        }

        database.exec('COMMIT');
    } catch (error) {
        database.exec('ROLLBACK');
        throw error;
    }
}

async function listInviteCodes() {
    return getDb().prepare(`
        SELECT id, code, description, max_uses, used_count, is_active, created_at
        FROM invite_codes
        ORDER BY created_at DESC
    `).all().map(row => ({
        id: row.id,
        code: row.code,
        description: row.description || '',
        max_uses: row.max_uses === null ? null : Number(row.max_uses),
        used_count: Number(row.used_count || 0),
        is_active: Boolean(row.is_active),
        created_at: row.created_at
    }));
}

async function createInviteCode({ code, description, maxUses }) {
    const inviteCode = String(code || '').trim() || generateInviteCode();
    const normalizedMaxUses = maxUses === '' || maxUses === null || maxUses === undefined
        ? 1
        : Math.max(1, Number(maxUses));
    const row = getDb().prepare(`
        INSERT INTO invite_codes (code, description, max_uses, is_active, created_at)
        VALUES (?, ?, ?, 1, ?)
        RETURNING id, code, description, max_uses, used_count, is_active, created_at
    `).get(inviteCode, String(description || '').trim(), normalizedMaxUses, new Date().toISOString());

    return {
        id: row.id,
        code: row.code,
        description: row.description || '',
        max_uses: row.max_uses === null ? null : Number(row.max_uses),
        used_count: Number(row.used_count || 0),
        is_active: Boolean(row.is_active),
        created_at: row.created_at
    };
}

async function updateInviteCode(inviteCodeId, updates = {}) {
    const database = getDb();
    const current = database.prepare(`
        SELECT *
        FROM invite_codes
        WHERE id = ?
    `).get(inviteCodeId);

    if (!current) {
        throw new Error('邀请码不存在');
    }

    const nextDescription = updates.description !== undefined
        ? String(updates.description || '').trim()
        : current.description;
    const nextMaxUses = updates.maxUses !== undefined
        ? (updates.maxUses === '' || updates.maxUses === null ? 1 : Math.max(1, Number(updates.maxUses)))
        : current.max_uses;
    const nextIsActive = updates.isActive !== undefined
        ? (updates.isActive ? 1 : 0)
        : current.is_active;

    database.prepare(`
        UPDATE invite_codes
        SET description = ?, max_uses = ?, is_active = ?
        WHERE id = ?
    `).run(nextDescription, nextMaxUses, nextIsActive, inviteCodeId);
}

module.exports = {
    DB_PATH,
    formatLocalDate,
    verifyConnection,
    getProductById,
    getProductByUrl,
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
    getUserById,
    getUserByUsername,
    createUserWithInvite,
    createSession,
    getUserBySessionToken,
    deleteSession,
    deleteUserSessions,
    addUserSelection,
    removeUserSelection,
    isProductSelectedByUser,
    listUsersWithStats,
    updateUserRole,
    updateUserActiveStatus,
    listInviteCodes,
    createInviteCode,
    updateInviteCode
};
