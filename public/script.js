let products = [];
let productMeta = {
    availableCategories: [],
    filteredTotal: 0,
    libraryCount: 0,
    selectedCount: 0,
    categoryStatusSummary: {},
    page: 1,
    pageSize: 20,
    totalPages: 1
};
let currentUser = null;
let trendChart = null;
let loadingTimer = null;
let chartJsLoader = null;
let activeRefreshJobId = null;
let refreshJobPollTimer = null;
let lastRefreshCompletedCount = 0;
let activeImportJobId = null;
let importJobPollTimer = null;
let lastImportCompletedCount = 0;
let lastImportSuccessPrefix = '批量导入完成';
let currentSearchTerm = '';
let searchRequestTimer = null;
let productsRequestSeq = 0;
let currentPage = 1;
let currentPageSize = 20;
let currentCategorySelections = [];
let activeWorkspaceView = 'library';
let adminUsers = [];
let adminInviteCodes = [];
let adminCategoryDrafts = {};
let portalMode = 'client';

const PRODUCTS_CACHE_KEY = 'xhs-monitor-products-cache-v4';
const COLUMN_VISIBILITY_KEY = 'xhs-monitor-column-visibility-v1';
const TABLE_IMPORT_URL_REGEX = /https?:\/\/(?:www\.xiaohongshu\.com\/goods-detail\/[^\s\t\r\n]+|(?:www|pages)\.xiaohongshu\.com\/goods\/[^\s\t\r\n]+|xhslink\.com\/[^\s\t\r\n]+)/ig;
const PRODUCT_COLUMN_DEFS = [
    { key: 'product_name', label: '商品名称', locked: true, defaultVisible: true },
    { key: 'category', label: '商品类目', defaultVisible: true },
    { key: 'price', label: '价格', defaultVisible: true },
    { key: 'product_total_sales', label: '商品总销量', defaultVisible: true },
    { key: 'daily_product_sales', label: '商品日销量', defaultVisible: true },
    { key: 'daily_product_sales_growth', label: '销量日增幅', defaultVisible: true },
    { key: 'daily_gmv', label: '商品日GMV', defaultVisible: true },
    { key: 'shop_total_sales', label: '店铺总销量', defaultVisible: true },
    { key: 'daily_shop_sales', label: '店铺日销量', defaultVisible: true },
    { key: 'created_at', label: '添加时间', defaultVisible: true },
    { key: 'last_update', label: '最后更新时间', defaultVisible: true },
    { key: 'actions', label: '操作', locked: true, defaultVisible: true }
];
let visibleProductColumns = getDefaultVisibleProductColumns();

document.addEventListener('DOMContentLoaded', function() {
    portalMode = detectPortalMode();
    configurePortalAuthShell();
    initializeApp();
});

document.addEventListener('click', handleDocumentClick);

async function initializeApp() {
    try {
        const response = await fetch(getAuthApiBase() + '/me');
        const data = await response.json();

        if (data.authenticated && data.user) {
            applyAuthenticatedState(data.user);
            hydrateProductsFromCache();
            await loadProducts();
            if (isAdminRoute() && isAdmin()) {
                await loadAdminData();
                resumeActiveRefreshJob();
                resumeActiveImportJob();
            }
            return;
        }
    } catch (error) {
        console.warn('获取登录状态失败:', error);
    }

    showAuthShell();
}

function isAdmin() {
    return currentUser && currentUser.role === 'admin';
}

function isAdminRoute() {
    return portalMode === 'admin';
}

function detectPortalMode() {
    const fromDataset = document.body && document.body.dataset ? document.body.dataset.portal : '';
    if (fromDataset === 'admin' || fromDataset === 'client') {
        return fromDataset;
    }
    return window.location.pathname.startsWith('/admin') ? 'admin' : 'client';
}

function getAuthApiBase() {
    return isAdminRoute() ? '/admin/auth' : '/auth';
}

function configurePortalAuthShell() {
    if (!isAdminRoute()) {
        return;
    }

    const tabs = document.querySelector('.auth-tabs');
    if (tabs) {
        const registerTab = tabs.querySelector('button[onclick*=\"register\"]');
        if (registerTab) {
            registerTab.hidden = true;
        }
    }

    const registerPanel = document.getElementById('registerPanel');
    if (registerPanel) {
        registerPanel.hidden = true;
        registerPanel.classList.remove('active');
    }
}

function switchInputTab(tabName, buttonEl) {
    document.querySelectorAll('.input-tabs .tab-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    if (buttonEl) {
        buttonEl.classList.add('active');
    }

    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
    });
    document.getElementById(tabName + 'Tab').classList.add('active');
}

function switchAuthMode(mode, buttonEl) {
    document.querySelectorAll('.auth-tab-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    if (buttonEl) {
        buttonEl.classList.add('active');
    }

    document.querySelectorAll('.auth-panel').forEach(panel => {
        panel.classList.remove('active');
    });
    const targetPanel = document.getElementById(mode + 'Panel');
    if (targetPanel) {
        targetPanel.classList.add('active');
    }
}

async function handleLogin(event) {
    event.preventDefault();

    const username = document.getElementById('loginUsername').value.trim();
    const password = document.getElementById('loginPassword').value;

    try {
        const response = await fetch(getAuthApiBase() + '/login', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password })
        });
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || '登录失败');
        }

        document.getElementById('loginPassword').value = '';
        await enterWorkspace(data.user, data.message || '登录成功');
    } catch (error) {
        showMessage(error.message, 'error');
    }
}

async function handleRegister(event) {
    event.preventDefault();

    if (isAdminRoute()) {
        showMessage('管理后台不开放注册，请使用管理员账号登录', 'warning');
        return;
    }

    const username = document.getElementById('registerUsername').value.trim();
    const password = document.getElementById('registerPassword').value;
    const inviteCode = document.getElementById('registerInviteCode').value.trim();

    try {
        const response = await fetch('/auth/register', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password, inviteCode })
        });
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || '注册失败');
        }

        document.getElementById('registerPassword').value = '';
        document.getElementById('registerInviteCode').value = '';
        if (data.portal === 'admin' || (data.user && data.user.role === 'admin')) {
            showMessage(data.message || '管理员账号创建成功，请前往管理后台登录', 'success');
            window.location.assign('/admin');
            return;
        }
        await enterWorkspace(data.user, data.message || '注册成功');
    } catch (error) {
        showMessage(error.message, 'error');
    }
}

async function enterWorkspace(user, messageText) {
    applyAuthenticatedState(user);
    hydrateProductsFromCache();
    await loadProducts();
    if (isAdminRoute() && isAdmin()) {
        await loadAdminData();
        resumeActiveRefreshJob();
        resumeActiveImportJob();
    }
    showMessage(messageText, 'success');
}

async function logout() {
    try {
        await fetch(getAuthApiBase() + '/logout', { method: 'POST' });
    } catch (error) {
        console.warn('退出请求失败:', error);
    }

    clearAuthenticatedState();
    showAuthShell();
    showMessage('已退出登录', 'info');
}

function applyAuthenticatedState(user) {
    currentUser = user;
    if (isAdminRoute() && !isAdmin()) {
        clearAuthenticatedState();
        showAuthShell();
        showMessage('仅管理员可登录管理后台', 'error');
        return;
    }
    activeWorkspaceView = isAdminRoute() ? 'library' : 'library';
    visibleProductColumns = loadColumnVisibilityState();
    document.getElementById('authShell').hidden = true;
    document.getElementById('appShell').hidden = false;
    setText('currentUsername', user.username);
    setText('currentUserRole', user.role || 'member');
    renderColumnSelector();
    updateWorkspaceChrome();
}

function clearAuthenticatedState() {
    currentUser = null;
    products = [];
    productMeta = {
        availableCategories: [],
        filteredTotal: 0,
        libraryCount: 0,
        selectedCount: 0,
        categoryStatusSummary: {},
        page: 1,
        pageSize: 20,
        totalPages: 1
    };
    adminUsers = [];
    adminInviteCodes = [];
    activeWorkspaceView = 'library';
    currentSearchTerm = '';
    currentPage = 1;
    currentPageSize = 20;
    currentCategorySelections = [];
    visibleProductColumns = getDefaultVisibleProductColumns();
    adminCategoryDrafts = {};
    if (searchRequestTimer) {
        clearTimeout(searchRequestTimer);
        searchRequestTimer = null;
    }
    setText('currentUsername', '-');
    setText('currentUserRole', 'member');
    stopRefreshJobPolling();
    stopImportJobPolling();
    clearProductsCache();
    const searchInput = document.getElementById('productSearch');
    if (searchInput) {
        searchInput.value = '';
    }
    updateSearchUi();
}

function getDefaultVisibleProductColumns() {
    return PRODUCT_COLUMN_DEFS
        .filter(column => column.defaultVisible)
        .map(column => column.key);
}

function getColumnVisibilityStorageKey() {
    return currentUser ? `${COLUMN_VISIBILITY_KEY}:${currentUser.id}` : COLUMN_VISIBILITY_KEY;
}

function loadColumnVisibilityState() {
    const defaultColumns = getDefaultVisibleProductColumns();
    try {
        const raw = localStorage.getItem(getColumnVisibilityStorageKey());
        if (!raw) {
            return defaultColumns;
        }

        const parsed = JSON.parse(raw);
        if (!Array.isArray(parsed)) {
            return defaultColumns;
        }

        const allowed = new Set(PRODUCT_COLUMN_DEFS.map(column => column.key));
        const next = parsed.filter(key => allowed.has(key));
        return ensureLockedColumns(next);
    } catch (error) {
        console.warn('读取显示列配置失败:', error);
        return defaultColumns;
    }
}

function persistColumnVisibilityState() {
    try {
        localStorage.setItem(getColumnVisibilityStorageKey(), JSON.stringify(visibleProductColumns));
    } catch (error) {
        console.warn('写入显示列配置失败:', error);
    }
}

function ensureLockedColumns(columns) {
    const lockedKeys = PRODUCT_COLUMN_DEFS.filter(column => column.locked).map(column => column.key);
    const nextSet = new Set(columns);
    lockedKeys.forEach(key => nextSet.add(key));
    return PRODUCT_COLUMN_DEFS.map(column => column.key).filter(key => nextSet.has(key));
}

function isColumnVisible(columnKey) {
    return visibleProductColumns.includes(columnKey);
}

function showAuthShell() {
    document.getElementById('authShell').hidden = false;
    document.getElementById('appShell').hidden = true;
}

async function apiFetch(url, options) {
    const response = await fetch(url, options);
    if (response.status === 401) {
        clearAuthenticatedState();
        showAuthShell();
        throw new Error('登录状态已失效，请重新登录');
    }

    return response;
}

async function addProduct() {
    if (!isAdminRoute()) {
        showMessage('新增商品功能仅在管理后台开放', 'warning');
        return;
    }

    const urlInput = document.getElementById('productUrl');
    const url = urlInput.value.trim();

    if (!url) {
        showMessage('请输入商品链接', 'error');
        return;
    }

    showLoading(true);
    try {
        const response = await apiFetch('/api/products', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ url })
        });
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || '添加失败');
        }

        urlInput.value = '';
        showMessage(data.message || '商品添加成功', 'success');
        await loadProducts();
    } catch (error) {
        showMessage('添加失败: ' + error.message, 'error');
    } finally {
        showLoading(false);
    }
}

async function importBatchUrls() {
    if (!isAdminRoute()) {
        showMessage('批量导入功能仅在管理后台开放', 'warning');
        return;
    }

    const textarea = document.getElementById('batchUrls');
    const items = textarea.value.trim().split('\n').map(item => item.trim()).filter(Boolean);

    if (items.length === 0) {
        showMessage('请输入商品链接', 'error');
        return;
    }

    await importProducts(items, '批量导入完成');
    textarea.value = '';
}

function clearBatchInput() {
    document.getElementById('batchUrls').value = '';
}

function previewTableImport() {
    const links = extractUrlsFromText(document.getElementById('tableImportText').value);
    renderTableImportPreview(links);

    if (links.length === 0) {
        showMessage('没有从表格内容中识别到有效的小红书商品链接', 'warning');
    }
}

async function importTableData() {
    if (!isAdminRoute()) {
        showMessage('表格导入功能仅在管理后台开放', 'warning');
        return;
    }

    const links = extractUrlsFromText(document.getElementById('tableImportText').value);
    renderTableImportPreview(links);

    if (links.length === 0) {
        showMessage('请先粘贴表格内容，或确认表格里包含小红书商品链接', 'error');
        return;
    }

    await importProducts(links, '表格导入完成');
}

function extractUrlsFromText(text) {
    const matches = String(text || '').match(TABLE_IMPORT_URL_REGEX) || [];
    return Array.from(new Set(matches.map(item => item.trim()).filter(Boolean)));
}

function renderTableImportPreview(links) {
    const preview = document.getElementById('tableImportPreview');
    const list = document.getElementById('tableImportList');
    setText('tableImportCount', String(links.length));

    if (links.length === 0) {
        preview.hidden = true;
        list.innerHTML = '';
        return;
    }

    preview.hidden = false;
    list.innerHTML = links.slice(0, 8).map(link => '<li>' + escapeHtml(link) + '</li>').join('');
}

async function importProducts(items, successPrefix) {
    if (!isAdminRoute()) {
        showMessage('导入功能仅在管理后台开放', 'warning');
        return;
    }

    showBatchProgress(true);
    updateProgress(8, '正在创建后台导入任务...');

    try {
        const response = await apiFetch('/api/products/import', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ items })
        });
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || '导入失败');
        }

        if (!data.job || !data.job.id) {
            throw new Error('导入任务创建失败');
        }

        showMessage(data.message || '批量导入任务已启动', 'info');
        startImportJobPolling(data.job, successPrefix);
    } catch (error) {
        showBatchProgress(false);
        showMessage('导入失败: ' + error.message, 'error');
    }
}

async function refreshAllData() {
    if (!isAdminRoute()) {
        showMessage('刷新商品数据已迁移到管理后台，请前往 /admin 操作', 'warning');
        return;
    }

    if (products.length === 0) {
        showMessage('没有商品需要刷新', 'warning');
        return;
    }

    try {
        const response = await apiFetch('/api/products/refresh-all', { method: 'POST' });
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || '批量刷新失败');
        }

        if (!data.job || !data.job.id) {
            throw new Error('刷新任务创建失败');
        }

        showMessage(data.message || '批量刷新任务已启动', 'info');
        startRefreshJobPolling(data.job);
    } catch (error) {
        showMessage('刷新失败: ' + error.message, 'error');
    }
}

async function loadProducts() {
    if (!currentUser) {
        return;
    }

    try {
        const requestSeq = ++productsRequestSeq;
        const params = buildProductsQueryParams();

        const response = await apiFetch('/api/products' + (params.toString() ? ('?' + params.toString()) : ''));
        if (!response.ok) {
            throw new Error('获取数据失败');
        }

        const payload = await response.json();
        if (requestSeq !== productsRequestSeq) {
            return;
        }

        products = Array.isArray(payload) ? payload : (payload.items || []);
        productMeta = {
            availableCategories: payload.availableCategories || [],
            filteredTotal: payload.filteredTotal || 0,
            libraryCount: payload.libraryCount || 0,
            selectedCount: payload.selectedCount || 0,
            categoryStatusSummary: payload.categoryStatusSummary || {},
            page: payload.page || currentPage,
            pageSize: payload.pageSize || currentPageSize,
            totalPages: payload.totalPages || 1
        };
        currentPage = productMeta.page;
        currentPageSize = productMeta.pageSize || 20;
        cacheProducts(products);
        populateCategoryFilter();
        updateLastSyncFromProducts();
        renderWorkspace();
    } catch (error) {
        showMessage('加载商品失败: ' + error.message, 'error');
    }
}

async function loadAdminData() {
    if (!isAdminRoute() || !isAdmin()) {
        return;
    }

    try {
        const [usersResponse, codesResponse] = await Promise.all([
            apiFetch('/admin/users'),
            apiFetch('/admin/invite-codes')
        ]);

        const usersData = await usersResponse.json();
        const codesData = await codesResponse.json();

        if (!usersResponse.ok) {
            throw new Error(usersData.error || '获取用户失败');
        }
        if (!codesResponse.ok) {
            throw new Error(codesData.error || '获取邀请码失败');
        }

        adminUsers = usersData;
        adminInviteCodes = codesData;
        renderAdminBoard();
    } catch (error) {
        showMessage('加载管理后台失败: ' + error.message, 'error');
    }
}

function switchWorkspaceView(view, buttonEl) {
    activeWorkspaceView = view;
    currentPage = 1;
    document.querySelectorAll('.board-tabs .library-tab-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    if (buttonEl) {
        buttonEl.classList.add('active');
    }
    renderWorkspace();
    loadProducts();
}

function renderWorkspace() {
    updateWorkspaceChrome();

    if (isAdminRoute()) {
        document.getElementById('productsBoard').hidden = false;
        document.getElementById('adminBoard').hidden = false;
        renderProducts();
        renderAdminBoard();
        return;
    }

    document.getElementById('productsBoard').hidden = false;
    document.getElementById('adminBoard').hidden = true;
    renderProducts();
}

function updateWorkspaceChrome() {
    const adminPortal = isAdminRoute();
    const titles = {
        library: '商品总库',
        selected: '我的选品池',
        admin: '管理后台'
    };
    const notes = {
        library: '商品总库展示所有已进入监控池的商品，你可以把其中感兴趣的商品加入自己的选品。',
        selected: '这里展示你已经加入“我的选品”的商品数据，更适合持续跟踪自己的候选池。',
        admin: '管理员可以在这里管理邀请码、用户角色和账号状态。'
    };

    const viewKey = adminPortal ? 'admin' : activeWorkspaceView;
    setText('workspaceTitle', titles[viewKey] || '商品总库');
    setText('toolbarNote', notes[viewKey] || notes.library);
    document.getElementById('workspaceToolbar').hidden = false;
    document.getElementById('sortControls').hidden = false;
    document.getElementById('refreshAllButton').hidden = !adminPortal;
    document.getElementById('refreshAllButton').disabled = !adminPortal;
    document.getElementById('dashboardSidebar').hidden = !adminPortal;
    document.getElementById('adminEntryButton').hidden = true;
    document.getElementById('workspaceEntryButton').hidden = true;
    const boardTabs = document.querySelector('.board-tabs');
    if (boardTabs) {
        boardTabs.hidden = adminPortal;
    }
    if (adminPortal) {
        setText('workspaceTitle', '商品管理后台');
        setText('toolbarNote', '在此统一管理商品库、导入监控对象、刷新监控数据和维护类目。');
        activeWorkspaceView = 'library';
        updateAddProductHelper();
    }

    const searchInput = document.getElementById('productSearch');
    if (searchInput && searchInput.value !== currentSearchTerm) {
        searchInput.value = currentSearchTerm;
    }
    updateSearchUi();
}

function updateAddProductHelper() {
    const helper = document.getElementById('addProductHelper');
    const addButton = document.getElementById('singleAddButton');
    if (!helper || !addButton) {
        return;
    }

    helper.textContent = '新增商品会进入商品总库并自动加入你的个人选品，用于后续统一监控。';
    addButton.textContent = '加入商品库';
}

function getBaseProductsForView() {
    return products;
}

function getFilteredProducts() {
    return getBaseProductsForView();
}

function renderProducts() {
    const tbody = document.getElementById('productsTableBody');
    const baseProducts = getBaseProductsForView();
    const filteredProducts = getFilteredProducts();

    updateViewSummary();
    updateFilterFeedback();
    renderPagination();

    if (productMeta.filteredTotal === 0) {
        const emptyText = activeWorkspaceView === 'selected'
            ? '你还没有添加任何选品，先去商品总库选择感兴趣的商品。'
            : (isAdminRoute() ? '暂无监控样本，先在左侧添加商品进入商品总库。' : '商品总库暂无样本，请联系管理员先完成监控商品配置。');
        tbody.innerHTML = '<tr><td colspan="12" class="table-empty">' + emptyText + '</td></tr>';
        applyColumnVisibility();
        return;
    }

    if (baseProducts.length === 0 || filteredProducts.length === 0) {
        tbody.innerHTML = '<tr><td colspan="12" class="table-empty">当前页没有数据，请切换分页或调整筛选条件。</td></tr>';
        applyColumnVisibility();
        return;
    }

    tbody.innerHTML = filteredProducts.map(renderProductRow).join('');
    applyColumnVisibility();
}

function renderProductRow(product) {
    return '<tr>' +
        '<td data-col="product_name">' +
            '<div class="product-cell">' +
                renderProductThumb(product) +
                '<div class="product-copy">' +
                    '<a class="product-link" href="' + escapeHtml(product.url) + '" target="_blank" rel="noopener noreferrer" title="' + escapeHtml(product.name) + '">' + escapeHtml(product.name) + '</a>' +
                    '<div class="product-meta">' + escapeHtml(product.shop_name || product.shopName || '未知店铺') + '</div>' +
                '</div>' +
            '</div>' +
        '</td>' +
        '<td data-col="category"><span class="category-pill">' + escapeHtml(getProductCategory(product)) + '</span></td>' +
        '<td data-col="price">¥' + (product.price || 0).toFixed(2) + '</td>' +
        '<td data-col="product_total_sales">' + formatNumber(product.product_total_sales) + '</td>' +
        '<td data-col="daily_product_sales" class="' + getDeltaClass(product.daily_product_sales_ready ? product.daily_product_sales : null) + '">' + renderDailyProductSales(product) + '</td>' +
        '<td data-col="daily_product_sales_growth" class="' + getGrowthClass(product.daily_product_sales_growth) + '">' + formatDailyGrowth(product.daily_product_sales_growth, product.previous_daily_product_sales, product.daily_product_sales, product.daily_product_sales_ready) + '</td>' +
        '<td data-col="daily_gmv">¥' + formatCurrency(product.daily_gmv || 0) + '</td>' +
        '<td data-col="shop_total_sales">' + formatNumber(product.shop_total_sales) + '</td>' +
        '<td data-col="daily_shop_sales" class="' + getDeltaClass(product.daily_shop_sales) + '">' + formatNumber(product.daily_shop_sales) + '</td>' +
        '<td data-col="created_at">' + renderTimeCell(product.created_at) + '</td>' +
        '<td data-col="last_update">' + renderTimeCell(product.last_update || product.created_at) + '</td>' +
        '<td data-col="actions"><div class="action-buttons">' + renderProductActions(product) + '</div></td>' +
    '</tr>';
}

function renderProductActions(product) {
    const actions = [];

    if (isAdminRoute()) {
        actions.push('<button onclick="refreshProductItem(' + product.id + ')" class="btn-small table-action-btn btn-secondary">刷新</button>');
        actions.push('<button onclick="showTrend(' + product.id + ')" class="btn-small table-action-btn btn-info">趋势</button>');
        actions.push('<button onclick="deleteProductItem(' + product.id + ')" class="btn-small table-action-btn btn-danger">删库</button>');
        return actions.join('');
    }

    if (activeWorkspaceView === 'selected') {
        actions.push('<button onclick="toggleSelection(' + product.id + ', false)" class="btn-small table-action-btn btn-danger">移出选品</button>');
    } else if (product.is_selected) {
        actions.push('<button class="btn-small table-action-btn btn-selected" disabled>已在选品</button>');
    } else {
        actions.push('<button onclick="toggleSelection(' + product.id + ', true)" class="btn-small table-action-btn btn-primary-soft">添加选品</button>');
    }

    actions.push('<button onclick="showTrend(' + product.id + ')" class="btn-small table-action-btn btn-info">趋势</button>');

    if (isAdmin() && activeWorkspaceView === 'library') {
        actions.push('<button onclick="deleteProductItem(' + product.id + ')" class="btn-small table-action-btn btn-danger">删库</button>');
    }

    return actions.join('');
}

async function refreshProductItem(productId) {
    if (!isAdminRoute()) {
        showMessage('刷新功能仅在管理后台开放', 'warning');
        return;
    }

    try {
        const response = await apiFetch('/api/products/' + productId + '/refresh', { method: 'POST' });
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || '刷新失败');
        }

        showMessage(data.message || '商品刷新成功', 'success');
        await loadProducts();
    } catch (error) {
        showMessage('刷新失败: ' + error.message, 'error');
    }
}

async function toggleSelection(productId, shouldSelect) {
    try {
        const response = await apiFetch('/api/products/' + productId + '/select', {
            method: shouldSelect ? 'POST' : 'DELETE'
        });
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || '选品操作失败');
        }

        showMessage(data.message || (shouldSelect ? '已加入选品' : '已移出选品'), 'success');
        await loadProducts();
    } catch (error) {
        showMessage(error.message, 'error');
    }
}

async function deleteProductItem(productId) {
    if (!confirm('确定要从商品总库删除这个商品吗？所有用户的选品关联也会一起删除。')) {
        return;
    }

    try {
        const response = await apiFetch('/api/products/' + productId, { method: 'DELETE' });
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || '删除失败');
        }

        showMessage(data.message || '商品已删除', 'success');
        await loadProducts();
    } catch (error) {
        showMessage(error.message, 'error');
    }
}

async function createInviteCodeItem() {
    try {
        const response = await apiFetch('/admin/invite-codes', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                code: document.getElementById('newInviteCode').value.trim(),
                description: document.getElementById('newInviteDescription').value.trim(),
                maxUses: document.getElementById('newInviteMaxUses').value.trim()
            })
        });
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || '创建邀请码失败');
        }

        document.getElementById('newInviteCode').value = '';
        document.getElementById('newInviteDescription').value = '';
        document.getElementById('newInviteMaxUses').value = '1';
        showMessage(data.message || '邀请码已创建', 'success');
        await loadAdminData();
    } catch (error) {
        showMessage(error.message, 'error');
    }
}

async function toggleInviteCode(inviteCodeId, nextActive) {
    try {
        const response = await apiFetch('/admin/invite-codes/' + inviteCodeId, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ isActive: nextActive })
        });
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || '更新邀请码失败');
        }

        showMessage(data.message || '邀请码已更新', 'success');
        await loadAdminData();
    } catch (error) {
        showMessage(error.message, 'error');
    }
}

async function changeUserRole(userId, role) {
    try {
        const response = await apiFetch('/admin/users/' + userId, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ role })
        });
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || '更新角色失败');
        }

        showMessage(data.message || '用户角色已更新', 'success');
        await refreshCurrentUserState();
        await loadAdminData();
    } catch (error) {
        showMessage(error.message, 'error');
    }
}

async function toggleUserActive(userId, nextActive) {
    try {
        const response = await apiFetch('/admin/users/' + userId, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ isActive: nextActive })
        });
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || '更新用户状态失败');
        }

        showMessage(data.message || '用户状态已更新', 'success');
        await loadAdminData();
    } catch (error) {
        showMessage(error.message, 'error');
    }
}

function renderAdminBoard() {
    renderInviteCodes();
    renderUsers();
    renderAdminProductCategories();
}

function renderInviteCodes() {
    const tbody = document.getElementById('inviteCodesTableBody');
    if (!tbody) {
        return;
    }

    if (adminInviteCodes.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="table-empty">暂无邀请码。</td></tr>';
        return;
    }

    tbody.innerHTML = adminInviteCodes.map(item => {
        const usageLimit = item.max_uses || 1;
        const usage = item.used_count + '/' + usageLimit;
        return '<tr>' +
            '<td><code class="inline-code">' + escapeHtml(item.code) + '</code></td>' +
            '<td>' + escapeHtml(item.description || '-') + '</td>' +
            '<td>' + usage + '</td>' +
            '<td><span class="status-pill ' + (item.is_active ? 'status-active' : 'status-paused') + '">' + (item.is_active ? '启用中' : '已停用') + '</span></td>' +
            '<td><button class="btn-small ' + (item.is_active ? 'btn-danger' : 'btn-primary-soft') + '" onclick="toggleInviteCode(' + item.id + ', ' + (!item.is_active) + ')">' + (item.is_active ? '停用' : '启用') + '</button></td>' +
        '</tr>';
    }).join('');
}

function renderUsers() {
    const tbody = document.getElementById('usersTableBody');
    if (!tbody) {
        return;
    }

    if (adminUsers.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="table-empty">暂无用户。</td></tr>';
        return;
    }

    tbody.innerHTML = adminUsers.map(user => {
        const roleButton = user.role === 'admin'
            ? '<button class="btn-small btn-secondary" onclick="changeUserRole(' + user.id + ', \'member\')">改为成员</button>'
            : '<button class="btn-small btn-primary-soft" onclick="changeUserRole(' + user.id + ', \'admin\')">设为管理员</button>';
        const activeButton = user.is_active
            ? '<button class="btn-small btn-danger" onclick="toggleUserActive(' + user.id + ', false)">停用</button>'
            : '<button class="btn-small btn-primary-soft" onclick="toggleUserActive(' + user.id + ', true)">启用</button>';
        const currentUserMark = currentUser && currentUser.id === user.id ? '<span class="current-user-tag">当前</span>' : '';

        return '<tr>' +
            '<td>' + escapeHtml(user.username) + currentUserMark + '</td>' +
            '<td><span class="status-pill ' + (user.role === 'admin' ? 'status-admin' : 'status-member') + '">' + (user.role === 'admin' ? 'admin' : 'member') + '</span></td>' +
            '<td><span class="status-pill ' + (user.is_active ? 'status-active' : 'status-paused') + '">' + (user.is_active ? '启用' : '停用') + '</span></td>' +
            '<td>' + formatNumber(user.selection_count || 0) + '</td>' +
            '<td>' + renderTimeCell(user.created_at) + '</td>' +
            '<td><div class="action-buttons">' + roleButton + activeButton + '</div></td>' +
        '</tr>';
    }).join('');
}

function renderAdminProductCategories() {
    const tbody = document.getElementById('adminProductCategoriesTableBody');
    if (!tbody) {
        return;
    }

    renderAdminCategoryStatusSummary();

    if (products.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="table-empty">当前没有可编辑的商品，先在工作台里加入商品后再来管理类目。</td></tr>';
        return;
    }

    const categories = productMeta.availableCategories || [];
    tbody.innerHTML = products.map(product => {
        const currentCategory = getProductCategory(product);
        const draftValue = adminCategoryDrafts[product.id] || currentCategory;
        const options = categories.map(category =>
            '<option value="' + escapeHtml(category) + '"' + (draftValue === category ? ' selected' : '') + '>' + escapeHtml(category) + '</option>'
        ).join('');

        return '<tr>' +
            '<td>' +
                '<div class="product-cell admin-product-cell">' +
                    renderProductThumb(product) +
                    '<div class="product-copy">' +
                        '<a class="product-link" href="' + escapeHtml(product.url) + '" target="_blank" rel="noopener noreferrer" title="' + escapeHtml(product.name) + '">' + escapeHtml(product.name) + '</a>' +
                        '<div class="product-meta">' + escapeHtml(product.shop_name || product.shopName || '未知店铺') + '</div>' +
                    '</div>' +
                '</div>' +
            '</td>' +
            '<td><span class="category-pill">' + escapeHtml(currentCategory) + '</span></td>' +
            '<td>' + renderCategoryStatusBadge(product) + '</td>' +
            '<td>' +
                '<select id="adminCategorySelect-' + product.id + '" class="admin-category-select" aria-label="修改商品类目" onchange="setAdminCategoryDraft(' + product.id + ', this.value)">' +
                    options +
                '</select>' +
            '</td>' +
            '<td><button class="btn-small btn-primary-soft" type="button" onclick="updateAdminProductCategory(' + product.id + ')">保存类目</button></td>' +
        '</tr>';
    }).join('');
}

function renderAdminCategoryStatusSummary() {
    const summary = document.getElementById('adminCategoryStatusSummary');
    if (!summary) {
        return;
    }

    const counts = productMeta.categoryStatusSummary || {};
    const items = [
        { key: 'queued', label: '待豆包' },
        { key: 'processing', label: '处理中' },
        { key: 'completed', label: '已完成' },
        { key: 'manual', label: '人工修正' },
        { key: 'failed', label: '失败' },
        { key: 'rule_only', label: '规则初判' }
    ].filter(item => Number(counts[item.key] || 0) > 0);

    if (items.length === 0) {
        summary.innerHTML = '<span class="admin-summary-pill">暂无类目状态数据</span>';
        return;
    }

    summary.innerHTML = items.map(item =>
        '<span class="admin-summary-pill">' + item.label + ' ' + formatNumber(counts[item.key] || 0) + '</span>'
    ).join('');
}

function renderCategoryStatusBadge(product) {
    const mapping = {
        queued: { label: '待豆包', className: 'status-queued' },
        processing: { label: '处理中', className: 'status-processing' },
        completed: { label: '豆包已完成', className: 'status-completed' },
        manual: { label: '人工修正', className: 'status-manual' },
        failed: { label: '补分类失败', className: 'status-failed' },
        rule_only: { label: '规则初判', className: 'status-rule-only' }
    };
    const status = mapping[product.category_status] || mapping.rule_only;
    return '<span class="status-pill ' + status.className + '">' + status.label + '</span>';
}

function setAdminCategoryDraft(productId, category) {
    adminCategoryDrafts[productId] = category;
}

async function updateAdminProductCategory(productId) {
    const select = document.getElementById('adminCategorySelect-' + productId);
    if (!select) {
        return;
    }

    try {
        const response = await apiFetch('/admin/products/' + productId + '/category', {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ category: select.value })
        });
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || '更新商品类目失败');
        }

        adminCategoryDrafts[productId] = select.value;
        showMessage(data.message || '商品类目已更新', 'success');
        await loadProducts();
    } catch (error) {
        showMessage(error.message, 'error');
    }
}

function sortTable() {
    currentPage = 1;
    loadProducts();
}

function populateCategoryFilter() {
    const menu = document.getElementById('categoryFilterOptions');
    if (!menu) {
        return;
    }

    const categories = [...(productMeta.availableCategories || [])];
    currentCategorySelections = currentCategorySelections.filter(category => categories.includes(category));
    menu.innerHTML = categories.map(category => {
        const checked = currentCategorySelections.includes(category);
        return '<label class="multi-select-option">' +
            '<input type="checkbox" value="' + escapeHtml(category) + '"' + (checked ? ' checked' : '') + ' onchange="toggleCategorySelection(\'' + escapeJsString(category) + '\')">' +
            '<span>' + escapeHtml(category) + '</span>' +
        '</label>';
    }).join('');
    syncCategoryFilterUi();
}

async function showTrend(id) {
    const modal = document.getElementById('trendModal');
    modal.style.display = 'flex';

    try {
        await ensureChartJsLoaded();
        const response = await apiFetch('/api/products/' + id + '/trend');
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || '获取趋势数据失败');
        }

        setText('trendTitle', data.productName + ' - 销量趋势');
        setText('totalSales', formatNumber(data.totalSales));
        setText('avgDailySales', formatNumber(data.avgDailySales));
        setText('maxDailySales', formatNumber(data.maxDailySales));
        const hasFirstDayWithoutBaseline = Array.isArray(data.chartData?.dailySales)
            && data.chartData.dailySales.length > 0
            && data.chartData.dailySales[0] === null;
        setText('monitorDays', hasFirstDayWithoutBaseline ? `${data.monitorDays} 天（首日无对比基线）` : `${data.monitorDays} 天`);
        renderTrendChart(data.chartData);
    } catch (error) {
        showMessage('获取趋势数据失败: ' + error.message, 'error');
        modal.style.display = 'none';
    }
}

function renderTrendChart(chartData) {
    const ctx = document.getElementById('trendChart').getContext('2d');

    if (trendChart) {
        trendChart.destroy();
    }

    trendChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: chartData.dates,
            datasets: [
                {
                    label: '总销量',
                    data: chartData.totalSales,
                    borderColor: '#1e40af',
                    backgroundColor: 'rgba(30, 64, 175, 0.08)',
                    tension: 0.4,
                    fill: true,
                    yAxisID: 'y'
                },
                {
                    label: '日销量',
                    data: chartData.dailySales,
                    borderColor: '#f59e0b',
                    backgroundColor: 'rgba(245, 158, 11, 0.14)',
                    tension: 0.4,
                    fill: true,
                    spanGaps: true,
                    yAxisID: 'y1'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: 'index',
                intersect: false
            },
            scales: {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    ticks: { color: '#4b5563' },
                    grid: { color: 'rgba(203, 213, 225, 0.65)' },
                    title: { display: true, text: '总销量', color: '#6b7280' }
                },
                y1: {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    ticks: { color: '#4b5563' },
                    title: { display: true, text: '日销量', color: '#6b7280' },
                    grid: { drawOnChartArea: false }
                },
                x: {
                    ticks: { color: '#4b5563' },
                    grid: { color: 'rgba(226, 232, 240, 0.72)' }
                }
            },
            plugins: {
                legend: {
                    position: 'top',
                    labels: { color: '#374151' }
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    backgroundColor: 'rgba(255, 255, 255, 0.98)',
                    titleColor: '#111827',
                    bodyColor: '#4b5563',
                    borderColor: 'rgba(203, 213, 225, 0.95)',
                    borderWidth: 1,
                    callbacks: {
                        label(context) {
                            const value = context.raw;
                            if (context.dataset.label === '日销量' && (value === null || value === undefined)) {
                                return '日销量: 首日（暂无对比基线）';
                            }
                            if (typeof value === 'number') {
                                return `${context.dataset.label}: ${formatNumber(value)}`;
                            }
                            return `${context.dataset.label}: --`;
                        }
                    }
                }
            }
        }
    });
}

function closeTrendModal() {
    document.getElementById('trendModal').style.display = 'none';
}

function showLoading(show) {
    const loadingEl = document.getElementById('loading');
    if (show) {
        if (loadingTimer) {
            clearTimeout(loadingTimer);
        }
        loadingEl.style.display = 'flex';
        return;
    }

    loadingTimer = setTimeout(function() {
        loadingEl.style.display = 'none';
    }, 120);
}

function showBatchProgress(show) {
    document.getElementById('batchProgress').style.display = show ? 'block' : 'none';
}

function updateProgress(percent, text) {
    document.getElementById('progressFill').style.width = percent + '%';
    document.getElementById('progressText').textContent = text;
}

function showMessage(text, type) {
    const messageEl = document.getElementById('message');
    messageEl.textContent = text;
    messageEl.className = 'message ' + (type || 'info');
    messageEl.style.display = 'block';

    setTimeout(function() {
        messageEl.style.display = 'none';
    }, 3200);
}

function updateViewSummary() {
    setText('libraryCountBadge', String(productMeta.libraryCount || 0));
    setText('selectionCountBadge', String(productMeta.selectedCount || 0));
}

function updateLastSyncFromProducts() {
    let latestSync = null;
    products.forEach(product => {
        const candidate = product.last_update || product.created_at;
        if (!candidate) {
            return;
        }

        if (!latestSync || new Date(candidate) > new Date(latestSync)) {
            latestSync = candidate;
        }
    });

    setText('heroLastSync', latestSync ? formatDate(latestSync) : '等待数据');
}

async function refreshCurrentUserState() {
    const response = await fetch(getAuthApiBase() + '/me');
    const data = await response.json();
    if (!data.authenticated || !data.user) {
        clearAuthenticatedState();
        showAuthShell();
        return;
    }

    currentUser = data.user;
    setText('currentUsername', currentUser.username);
    setText('currentUserRole', currentUser.role || 'member');

    if (isAdminRoute() && !isAdmin()) {
        clearAuthenticatedState();
        showAuthShell();
        showMessage('仅管理员可登录管理后台', 'error');
        return;
    }

    updateWorkspaceChrome();
    renderWorkspace();
}

function formatNumber(num) {
    if (!num || num === 0) {
        return '0';
    }

    if (num >= 10000) {
        return (num / 10000).toFixed(1) + '万';
    }

    return Number(num).toLocaleString();
}

function formatCurrency(num) {
    if (!num || num === 0) {
        return '0';
    }

    if (num >= 10000) {
        return (num / 10000).toFixed(1) + '万';
    }

    return Math.round(num).toLocaleString();
}

function formatDailyGrowth(growthValue, previousDailySales, currentDailySales, hasDailyBaseline = true) {
    if (!hasDailyBaseline) {
        return '首日';
    }

    if (Number.isFinite(growthValue)) {
        const prefix = growthValue > 0 ? '+' : '';
        return prefix + growthValue.toFixed(1) + '%';
    }

    if ((previousDailySales || 0) === 0 && (currentDailySales || 0) > 0) {
        return '新增';
    }

    return '--';
}

function formatAbsoluteDate(dateStr) {
    if (!dateStr) {
        return '-';
    }

    const date = new Date(dateStr);
    if (Number.isNaN(date.getTime())) {
        return '-';
    }

    return date.toLocaleString('zh-CN', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false
    });
}

function formatDate(dateStr) {
    if (!dateStr) {
        return '-';
    }

    const date = new Date(dateStr);
    if (Number.isNaN(date.getTime())) {
        return '-';
    }

    const diff = Date.now() - date.getTime();

    if (diff < 60000) {
        return '刚刚';
    }
    if (diff < 3600000) {
        return Math.floor(diff / 60000) + '分钟前';
    }
    if (diff < 86400000) {
        return Math.floor(diff / 3600000) + '小时前';
    }
    if (diff < 604800000) {
        return Math.floor(diff / 86400000) + '天前';
    }

    return date.toLocaleDateString('zh-CN', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
    });
}

function renderTimeCell(dateStr) {
    const relative = formatDate(dateStr);
    const absolute = formatAbsoluteDate(dateStr);
    return '<span title="' + escapeHtml(absolute) + '">' + escapeHtml(relative) + '</span>';
}

function getDeltaClass(value) {
    if (!Number.isFinite(value)) {
        return '';
    }

    if (value > 0) {
        return 'positive';
    }
    if (value < 0) {
        return 'negative';
    }
    return '';
}

function renderDailyProductSales(product) {
    if (!product.daily_product_sales_ready) {
        return '<span class="metric-pending" title="首日监控暂无昨日对比基线，暂不展示精确日销量。">首日</span>';
    }

    return formatNumber(product.daily_product_sales);
}

function getGrowthClass(value) {
    if (!Number.isFinite(value)) {
        return 'muted';
    }

    if (value > 0) {
        return 'positive';
    }

    if (value < 0) {
        return 'negative';
    }

    return '';
}

function setText(id, value) {
    const element = document.getElementById(id);
    if (element) {
        element.textContent = value;
    }
}

function normalizeSearchText(value) {
    return String(value || '')
        .toLowerCase()
        .replace(/\s+/g, '')
        .trim();
}

function readNumberInputValue(id) {
    const element = document.getElementById(id);
    if (!element) {
        return null;
    }

    const rawValue = String(element.value || '').trim();
    if (!rawValue) {
        return null;
    }

    const parsed = Number(rawValue);
    return Number.isFinite(parsed) ? parsed : null;
}

function getToolbarFilterState() {
    return {
        categories: [...currentCategorySelections],
        minPrice: readNumberInputValue('minPriceFilter'),
        maxPrice: readNumberInputValue('maxPriceFilter'),
        minTotalSales: readNumberInputValue('minTotalSalesFilter'),
        maxTotalSales: readNumberInputValue('maxTotalSalesFilter'),
        minDailySales: readNumberInputValue('minDailySalesFilter'),
        maxDailySales: readNumberInputValue('maxDailySalesFilter')
    };
}

function buildProductsQueryParams() {
    const params = new URLSearchParams();
    const adminMode = isAdminRoute();
    const filterState = getToolbarFilterState();

    params.set('view', adminMode ? 'library' : activeWorkspaceView);
    params.set('page', String(currentPage));
    params.set('pageSize', String(currentPageSize));

    if (currentSearchTerm.trim()) {
        params.set('q', currentSearchTerm.trim());
    }

    if (filterState.categories.length > 0) {
        params.set('categories', filterState.categories.join(','));
    }

    if (filterState.minPrice !== null) {
        params.set('minPrice', String(filterState.minPrice));
    }
    if (filterState.maxPrice !== null) {
        params.set('maxPrice', String(filterState.maxPrice));
    }
    if (filterState.minTotalSales !== null) {
        params.set('minTotalSales', String(filterState.minTotalSales));
    }
    if (filterState.maxTotalSales !== null) {
        params.set('maxTotalSales', String(filterState.maxTotalSales));
    }
    if (filterState.minDailySales !== null) {
        params.set('minDailySales', String(filterState.minDailySales));
    }
    if (filterState.maxDailySales !== null) {
        params.set('maxDailySales', String(filterState.maxDailySales));
    }

    const sortBy = document.getElementById('sortBy');
    const sortOrder = document.getElementById('sortOrder');
    if (sortBy && sortBy.value) {
        params.set('sortBy', sortBy.value);
    }
    if (sortOrder && sortOrder.value) {
        params.set('sortOrder', sortOrder.value);
    }

    return params;
}

function matchesRange(value, minValue, maxValue) {
    if (minValue !== null && value < minValue) {
        return false;
    }
    if (maxValue !== null && value > maxValue) {
        return false;
    }
    return true;
}

function scheduleProductsReload(delay = 180) {
    if (searchRequestTimer) {
        clearTimeout(searchRequestTimer);
    }

    searchRequestTimer = setTimeout(function() {
        loadProducts();
    }, delay);
}

function handleProductSearchInput(event) {
    currentSearchTerm = event.target.value || '';
    currentPage = 1;
    updateSearchUi();
    scheduleProductsReload(220);
}

function handleToolbarFilterChange() {
    currentPage = 1;
    scheduleProductsReload(120);
}

function toggleCategoryFilterMenu(forceOpen) {
    const wrap = document.getElementById('categoryFilterWrap');
    const menu = document.getElementById('categoryFilterMenu');
    const trigger = document.getElementById('categoryFilterTrigger');
    if (!wrap || !menu || !trigger) {
        return;
    }

    const nextOpen = typeof forceOpen === 'boolean'
        ? forceOpen
        : !wrap.classList.contains('is-open');

    wrap.classList.toggle('is-open', nextOpen);
    menu.hidden = !nextOpen;
    trigger.setAttribute('aria-expanded', nextOpen ? 'true' : 'false');
}

function toggleCategorySelection(category) {
    const nextSelections = currentCategorySelections.includes(category)
        ? currentCategorySelections.filter(item => item !== category)
        : [...currentCategorySelections, category];

    currentCategorySelections = nextSelections;
    syncCategoryFilterUi();
    handleToolbarFilterChange();
}

function clearCategorySelections() {
    currentCategorySelections = [];
    syncCategoryFilterUi();
    handleToolbarFilterChange();
}

function renderColumnSelector() {
    const optionsContainer = document.getElementById('columnSelectorOptions');
    if (!optionsContainer) {
        return;
    }

    optionsContainer.innerHTML = PRODUCT_COLUMN_DEFS.filter(column => !column.locked).map(column => {
        const checked = isColumnVisible(column.key) ? ' checked' : '';
        return '<label class="column-selector-option">' +
            '<input type="checkbox" value="' + escapeHtml(column.key) + '"' + checked + ' onchange="toggleColumnVisibility(\'' + escapeJsString(column.key) + '\')">' +
            '<span>' + escapeHtml(column.label) + '</span>' +
        '</label>';
    }).join('');

    updateColumnSelectorSummary();
    applyColumnVisibility();
}

function toggleColumnSelectorMenu(forceOpen) {
    const wrap = document.getElementById('columnSelectorWrap');
    const menu = document.getElementById('columnSelectorMenu');
    const trigger = document.getElementById('columnSelectorTrigger');
    if (!wrap || !menu || !trigger) {
        return;
    }

    const nextOpen = typeof forceOpen === 'boolean'
        ? forceOpen
        : !wrap.classList.contains('is-open');

    wrap.classList.toggle('is-open', nextOpen);
    menu.hidden = !nextOpen;
    trigger.setAttribute('aria-expanded', nextOpen ? 'true' : 'false');
}

function updateColumnSelectorSummary() {
    const countEl = document.getElementById('columnSelectorCount');
    if (!countEl) {
        return;
    }

    const toggleableColumns = PRODUCT_COLUMN_DEFS.filter(column => !column.locked);
    const visibleCount = toggleableColumns.filter(column => isColumnVisible(column.key)).length;
    countEl.textContent = String(visibleCount);
}

function toggleColumnVisibility(columnKey) {
    const columnDef = PRODUCT_COLUMN_DEFS.find(column => column.key === columnKey);
    if (!columnDef || columnDef.locked) {
        return;
    }

    if (isColumnVisible(columnKey)) {
        visibleProductColumns = visibleProductColumns.filter(key => key !== columnKey);
    } else {
        visibleProductColumns = ensureLockedColumns([...visibleProductColumns, columnKey]);
    }

    persistColumnVisibilityState();
    updateColumnSelectorSummary();
    applyColumnVisibility();
}

function resetColumnVisibility() {
    visibleProductColumns = getDefaultVisibleProductColumns();
    persistColumnVisibilityState();
    renderColumnSelector();
}

function applyColumnVisibility() {
    const visibleSet = new Set(ensureLockedColumns(visibleProductColumns));
    const columnKeys = PRODUCT_COLUMN_DEFS.map(column => column.key);

    columnKeys.forEach(columnKey => {
        const isVisible = visibleSet.has(columnKey);
        document.querySelectorAll('[data-col="' + columnKey + '"]').forEach(node => {
            node.classList.toggle('is-column-hidden', !isVisible);
        });
    });
}

function syncCategoryFilterUi() {
    const summary = document.getElementById('categoryFilterSummary');
    const countBadge = document.getElementById('categoryFilterCount');
    const wrap = document.getElementById('categoryFilterWrap');
    if (!summary || !countBadge || !wrap) {
        return;
    }

    const hasSelections = currentCategorySelections.length > 0;
    wrap.classList.toggle('has-selection', hasSelections);
    summary.textContent = hasSelections
        ? (currentCategorySelections.length <= 2
            ? currentCategorySelections.join(' / ')
            : `已选 ${currentCategorySelections.length} 个类目`)
        : '全部类目';
    countBadge.hidden = !hasSelections;
    countBadge.textContent = String(currentCategorySelections.length);

    const checkboxes = document.querySelectorAll('#categoryFilterOptions input[type="checkbox"]');
    checkboxes.forEach(input => {
        input.checked = currentCategorySelections.includes(input.value);
    });
}

function clearProductSearch() {
    const input = document.getElementById('productSearch');
    currentSearchTerm = '';
    if (searchRequestTimer) {
        clearTimeout(searchRequestTimer);
        searchRequestTimer = null;
    }
    if (input) {
        input.value = '';
    }

    updateSearchUi();
    loadProducts();
}

function resetToolbarFilters() {
    currentSearchTerm = '';
    currentPage = 1;
    currentPageSize = 20;
    currentCategorySelections = [];
    if (searchRequestTimer) {
        clearTimeout(searchRequestTimer);
        searchRequestTimer = null;
    }

    const searchInput = document.getElementById('productSearch');
    const sortBy = document.getElementById('sortBy');
    const sortOrder = document.getElementById('sortOrder');
    const pageSizeSelect = document.getElementById('pageSizeSelect');
    const numericInputs = [
        'minPriceFilter',
        'maxPriceFilter',
        'minTotalSalesFilter',
        'maxTotalSalesFilter',
        'minDailySalesFilter',
        'maxDailySalesFilter'
    ];

    if (searchInput) {
        searchInput.value = '';
    }
    if (sortBy) {
        sortBy.value = 'id';
    }
    if (sortOrder) {
        sortOrder.value = 'desc';
    }
    if (pageSizeSelect) {
        pageSizeSelect.value = '20';
    }
    numericInputs.forEach(id => {
        const element = document.getElementById(id);
        if (element) {
            element.value = '';
        }
    });

    updateSearchUi();
    syncCategoryFilterUi();
    loadProducts();
}

function updateSearchUi() {
    const searchShell = document.getElementById('searchShell');
    const clearButton = document.getElementById('clearSearchButton');
    const hasSearch = Boolean(currentSearchTerm.trim());

    if (searchShell) {
        searchShell.classList.toggle('search-shell-active', hasSearch);
    }

    if (clearButton) {
        clearButton.hidden = !hasSearch;
    }
}

function updateFilterFeedback() {
    const filterState = getToolbarFilterState();
    const activeFilters = [];

    if (currentSearchTerm.trim()) {
        activeFilters.push('关键词');
    }
    if (filterState.categories.length > 0) {
        activeFilters.push(`类目：${filterState.categories.join(' / ')}`);
    }
    if (filterState.minPrice !== null || filterState.maxPrice !== null) {
        activeFilters.push('价格区间');
    }
    if (filterState.minTotalSales !== null || filterState.maxTotalSales !== null) {
        activeFilters.push('商品总销量区间');
    }
    if (filterState.minDailySales !== null || filterState.maxDailySales !== null) {
        activeFilters.push('商品日销量区间');
    }

    const count = productMeta.filteredTotal || 0;
    const currentCount = products.length;
    const scopeCount = activeWorkspaceView === 'selected'
        ? (productMeta.selectedCount || 0)
        : (productMeta.libraryCount || 0);

    setText('resultsSummary', `当前展示 ${currentCount} 条，共命中 ${count} 条`);
    setText(
        'activeFilterSummary',
        activeFilters.length > 0
            ? `已生效：${activeFilters.join(' / ')}，当前范围内共 ${scopeCount} 条`
            : `未启用筛选条件，当前范围内共 ${scopeCount} 条`
    );
}

function handleDocumentClick(event) {
    const categoryWrap = document.getElementById('categoryFilterWrap');
    if (categoryWrap && categoryWrap.classList.contains('is-open') && !categoryWrap.contains(event.target)) {
        toggleCategoryFilterMenu(false);
    }

    const columnWrap = document.getElementById('columnSelectorWrap');
    if (columnWrap && columnWrap.classList.contains('is-open') && !columnWrap.contains(event.target)) {
        toggleColumnSelectorMenu(false);
    }
}

function renderPagination() {
    const bar = document.getElementById('paginationBar');
    const totalPages = productMeta.totalPages || 1;
    const current = productMeta.page || 1;
    const filteredTotal = productMeta.filteredTotal || 0;

    if (!bar) {
        return;
    }

    bar.hidden = filteredTotal === 0;
    setText('paginationStatus', `第 ${current} / ${totalPages} 页`);
    setText('paginationDetail', `共 ${filteredTotal} 条，每页 ${currentPageSize} 条`);
    document.getElementById('prevPageButton').disabled = current <= 1;
    document.getElementById('nextPageButton').disabled = current >= totalPages;
    const pageSizeSelect = document.getElementById('pageSizeSelect');
    if (pageSizeSelect) {
        pageSizeSelect.value = String(currentPageSize);
    }
}

function changePage(step) {
    const nextPage = currentPage + step;
    if (nextPage < 1 || nextPage > (productMeta.totalPages || 1)) {
        return;
    }

    currentPage = nextPage;
    loadProducts();
}

function changePageSize(nextPageSize) {
    const parsed = Number(nextPageSize);
    if (!Number.isFinite(parsed)) {
        return;
    }

    currentPageSize = Math.min(200, Math.max(20, parsed));
    currentPage = 1;
    loadProducts();
}

async function resumeActiveRefreshJob() {
    if (!currentUser) {
        return;
    }

    try {
        const response = await apiFetch('/api/refresh-jobs/active');
        if (response.status === 204) {
            return;
        }
        if (!response.ok) {
            return;
        }

        const job = await response.json();
        startRefreshJobPolling(job);
    } catch (error) {
        console.warn('恢复刷新任务状态失败:', error);
    }
}

async function resumeActiveImportJob() {
    if (!currentUser) {
        return;
    }

    try {
        const response = await apiFetch('/api/import-jobs/active');
        if (response.status === 204) {
            return;
        }
        if (!response.ok) {
            return;
        }

        const job = await response.json();
        startImportJobPolling(job);
    } catch (error) {
        console.warn('恢复导入任务状态失败:', error);
    }
}

function startRefreshJobPolling(job) {
    activeRefreshJobId = job.id;
    lastRefreshCompletedCount = job.completedCount || 0;
    updateRefreshJobBanner(job);
    setRefreshButtonState(true);

    if (refreshJobPollTimer) {
        clearTimeout(refreshJobPollTimer);
    }

    pollRefreshJob(job.id);
}

async function pollRefreshJob(jobId) {
    try {
        const response = await apiFetch('/api/refresh-jobs/' + jobId);
        const job = await response.json();

        if (!response.ok) {
            throw new Error(job.error || '获取刷新任务状态失败');
        }

        updateRefreshJobBanner(job);

        if ((job.completedCount || 0) > lastRefreshCompletedCount) {
            lastRefreshCompletedCount = job.completedCount || 0;
            loadProducts();
        }

        if (job.status === 'queued' || job.status === 'running') {
            refreshJobPollTimer = setTimeout(function() {
                pollRefreshJob(jobId);
            }, 1200);
            return;
        }

        setRefreshButtonState(false);
        activeRefreshJobId = null;
        refreshJobPollTimer = null;
        hideRefreshJobBannerAfterDelay();

        if (job.status === 'completed') {
            await loadProducts();
            showMessage('后台刷新完成', job.failCount > 0 ? 'warning' : 'success');
        } else if (job.status === 'failed') {
            showMessage('后台刷新失败: ' + (job.message || '请稍后重试'), 'error');
        }
    } catch (error) {
        setRefreshButtonState(false);
        showMessage('获取刷新进度失败: ' + error.message, 'error');
    }
}

function stopRefreshJobPolling() {
    activeRefreshJobId = null;
    lastRefreshCompletedCount = 0;
    if (refreshJobPollTimer) {
        clearTimeout(refreshJobPollTimer);
        refreshJobPollTimer = null;
    }

    const banner = document.getElementById('refreshJobBar');
    if (banner) {
        banner.hidden = true;
    }
}

function startImportJobPolling(job, successPrefix) {
    activeImportJobId = job.id;
    lastImportCompletedCount = job.completedCount || 0;
    updateImportJobBanner(job);
    updateImportJobProgress(job);
    setImportButtonsState(true);
    showBatchProgress(true);
    lastImportSuccessPrefix = successPrefix || '批量导入完成';

    if (importJobPollTimer) {
        clearTimeout(importJobPollTimer);
    }

    pollImportJob(job.id);
}

async function pollImportJob(jobId) {
    try {
        const response = await apiFetch('/api/import-jobs/' + jobId);
        const job = await response.json();

        if (!response.ok) {
            throw new Error(job.error || '获取导入任务状态失败');
        }

        updateImportJobBanner(job);
        updateImportJobProgress(job);

        if ((job.completedCount || 0) > lastImportCompletedCount) {
            lastImportCompletedCount = job.completedCount || 0;
            loadProducts();
        }

        if (job.status === 'queued' || job.status === 'running') {
            importJobPollTimer = setTimeout(function() {
                pollImportJob(jobId);
            }, 1200);
            return;
        }

        setImportButtonsState(false);
        activeImportJobId = null;
        importJobPollTimer = null;
        hideImportJobBannerAfterDelay();

        if (job.status === 'completed') {
            await loadProducts();
            const prefix = lastImportSuccessPrefix || '批量导入完成';
            showMessage(prefix + '，成功 ' + (job.successCount || 0) + ' 条，失败 ' + (job.failCount || 0) + ' 条', (job.failCount || 0) > 0 ? 'warning' : 'success');
        } else if (job.status === 'failed') {
            showMessage('后台导入失败: ' + (job.message || '请稍后重试'), 'error');
        }
    } catch (error) {
        setImportButtonsState(false);
        showBatchProgress(false);
        showMessage('获取导入进度失败: ' + error.message, 'error');
    }
}

function stopImportJobPolling() {
    activeImportJobId = null;
    lastImportCompletedCount = 0;
    if (importJobPollTimer) {
        clearTimeout(importJobPollTimer);
        importJobPollTimer = null;
    }

    const banner = document.getElementById('importJobBar');
    if (banner) {
        banner.hidden = true;
    }
    showBatchProgress(false);
}

function updateRefreshJobBanner(job) {
    const banner = document.getElementById('refreshJobBar');
    const progress = Math.max(0, Math.min(100, job.progress || 0));
    banner.hidden = false;
    setText('refreshJobTitle', job.status === 'completed' ? '后台刷新完成' : '后台刷新中');
    setText('refreshJobText', job.message || '正在后台刷新商品数据，你可以继续浏览和操作页面。');
    setText('refreshJobPercent', progress + '%');
    document.getElementById('refreshJobFill').style.width = progress + '%';
}

function hideRefreshJobBannerAfterDelay() {
    setTimeout(function() {
        if (activeRefreshJobId) {
            return;
        }

        const banner = document.getElementById('refreshJobBar');
        banner.hidden = true;
        document.getElementById('refreshJobFill').style.width = '0%';
    }, 2500);
}

function updateImportJobBanner(job) {
    const banner = document.getElementById('importJobBar');
    const progress = Math.max(0, Math.min(100, job.progress || 0));
    banner.hidden = false;
    setText('importJobTitle', job.status === 'completed' ? '后台导入完成' : '后台导入中');
    setText('importJobText', job.message || '正在后台导入商品，你可以继续浏览和操作页面。');
    setText('importJobPercent', progress + '%');
    document.getElementById('importJobFill').style.width = progress + '%';
}

function updateImportJobProgress(job) {
    updateProgress(Math.max(0, Math.min(100, job.progress || 0)), job.message || '正在后台导入商品...');
}

function hideImportJobBannerAfterDelay() {
    setTimeout(function() {
        if (activeImportJobId) {
            return;
        }

        const banner = document.getElementById('importJobBar');
        if (banner) {
            banner.hidden = true;
        }
        const fill = document.getElementById('importJobFill');
        if (fill) {
            fill.style.width = '0%';
        }
        showBatchProgress(false);
    }, 2500);
}

function setRefreshButtonState(isRunning) {
    const refreshButton = document.getElementById('refreshAllButton');
    if (!refreshButton) {
        return;
    }

    refreshButton.disabled = isRunning || !isAdminRoute();
    refreshButton.textContent = isRunning ? '后台刷新中...' : '刷新全部数据';
}

function setImportButtonsState(isRunning) {
    const batchButton = document.getElementById('batchImportButton');
    const tableButton = document.getElementById('tableImportButton');

    if (batchButton) {
        batchButton.disabled = isRunning;
        batchButton.textContent = isRunning ? '后台导入中...' : '开始导入';
    }

    if (tableButton) {
        tableButton.disabled = isRunning;
        tableButton.textContent = isRunning ? '后台导入中...' : '导入表格商品';
    }
}

function goToAdmin() {
    if (!isAdmin()) {
        return;
    }

    window.location.assign('/admin');
}

function goToWorkspace() {
    window.location.assign('/');
}

function getProductsCacheKey() {
    return currentUser ? PRODUCTS_CACHE_KEY + ':' + currentUser.id : PRODUCTS_CACHE_KEY;
}

function hydrateProductsFromCache() {
    try {
        const cached = localStorage.getItem(getProductsCacheKey());
        if (!cached) {
            return;
        }

        const parsed = JSON.parse(cached);
        const cachedItems = Array.isArray(parsed) ? parsed : parsed.items;
        if (!Array.isArray(cachedItems) || cachedItems.length === 0) {
            return;
        }

        products = cachedItems;
        if (parsed && !Array.isArray(parsed) && parsed.meta) {
            productMeta = {
                ...productMeta,
                ...parsed.meta
            };
            currentPage = productMeta.page || 1;
            currentPageSize = productMeta.pageSize || 20;
        }
        populateCategoryFilter();
        updateLastSyncFromProducts();
        renderWorkspace();
    } catch (error) {
        console.warn('读取本地缓存失败:', error);
    }
}

function cacheProducts(items) {
    try {
        localStorage.setItem(getProductsCacheKey(), JSON.stringify({
            items,
            meta: productMeta
        }));
    } catch (error) {
        console.warn('写入本地缓存失败:', error);
    }
}

function clearProductsCache() {
    try {
        if (currentUser) {
            localStorage.removeItem(getProductsCacheKey());
            return;
        }

        Object.keys(localStorage)
            .filter(key => key.startsWith(PRODUCTS_CACHE_KEY))
            .forEach(key => localStorage.removeItem(key));
    } catch (error) {
        console.warn('清理本地缓存失败:', error);
    }
}

function getProductCategory(product) {
    return product.category || '其他';
}

function buildThumbnailUrl(imageUrl) {
    if (!imageUrl) {
        return '';
    }

    const baseUrl = imageUrl.split('?')[0];
    return baseUrl + '?imageView2/2/w/160/q/80/format/webp';
}

function ensureChartJsLoaded() {
    if (window.Chart) {
        return Promise.resolve();
    }

    if (chartJsLoader) {
        return chartJsLoader;
    }

    chartJsLoader = new Promise((resolve, reject) => {
        const script = document.createElement('script');
        script.src = 'https://cdn.jsdelivr.net/npm/chart.js';
        script.async = true;
        script.onload = function() {
            resolve();
        };
        script.onerror = function() {
            reject(new Error('图表库加载失败'));
        };
        document.head.appendChild(script);
    });

    return chartJsLoader;
}

function renderProductThumb(product) {
    const productUrl = escapeHtml(product.url || '#');
    const productName = escapeHtml(product.name || '未命名商品');
    const imageUrl = escapeHtml(buildThumbnailUrl(product.imageUrl || ''));

    if (imageUrl) {
        return '<a class="product-thumb-link" href="' + productUrl + '" target="_blank" rel="noopener noreferrer" aria-label="打开商品链接：' + productName + '">' +
            '<img class="product-thumb" src="' + imageUrl + '" alt="' + productName + '" loading="lazy" decoding="async" fetchpriority="low">' +
        '</a>';
    }

    return '<a class="product-thumb-link product-thumb-fallback" href="' + productUrl + '" target="_blank" rel="noopener noreferrer" aria-label="打开商品链接：' + productName + '">' +
        '<span>' + productName.slice(0, 1) + '</span>' +
    '</a>';
}

function escapeHtml(text) {
    if (!text) {
        return '';
    }

    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function escapeJsString(text) {
    return String(text || '')
        .replace(/\\/g, '\\\\')
        .replace(/'/g, "\\'");
}

window.onclick = function(event) {
    const modal = document.getElementById('trendModal');
    if (event.target === modal) {
        closeTrendModal();
    }
};
