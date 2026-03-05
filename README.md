# 小红书商品销量监控系统 v2.0

一个面向日常运营的本地监控工具，用来跟踪小红书商品销量、店铺销量和日趋势变化。

当前版本的真实状态是：
- 前端可用，适合本地单机日常使用
- 后端支持手动刷新和每小时自动刷新
- 数据存储已切换为本地 SQLite
- 首次启动时会自动导入旧的 JSON 数据
- 还不适合直接部署到 Vercel 这类无状态平台

## 当前能力

- 自动解析小红书商品链接和分享文本
- 抓取商品名、价格、商品总销量、店铺名、店铺总销量
- 支持手动刷新单个商品 / 刷新全部
- 每小时自动刷新一次全部商品
- 展示商品总销量、店铺总销量、商品日销量、店铺日销量、日 GMV
- 提供趋势图接口和健康检查接口

## 技术栈

- 后端: Node.js + Express
- 爬取: Puppeteer
- 定时任务: node-cron
- 前端: HTML + CSS + JavaScript
- 存储: SQLite

## 运行要求

- Node.js `>= 24.0.0`
- 本机可用的 Chrome 或 Chromium

说明：
- 项目当前使用 Node 24 自带的 `node:sqlite`
- 启动时可能会看到 SQLite experimental warning，这是当前实现方式的正常现象

## 本地启动

```bash
npm install
npm start
```

默认访问地址：

```text
http://localhost:3000
```

## 线上发布 SOP（xhsmonitor.supersolo.me）

当前线上环境（已验证）：

- 服务器：`47.107.177.223`
- 项目目录：`/www/wwwroot/rednote_monitor`
- 发布分支：`main`
- PM2 进程名：`xiaohongshu-monitor`
- 服务端口：`3010`
- 对外域名：`http://xhsmonitor.supersolo.me`

标准发布步骤（在服务器终端执行）：

```bash
cd /www/wwwroot/rednote_monitor

# 首次可能遇到 git 安全目录报错，先执行一次
git config --global --add safe.directory /www/wwwroot/rednote_monitor

git fetch origin
git pull --ff-only origin main
git rev-parse --short HEAD

# 生产依赖安装（推荐 --omit=dev）
npm install --omit=dev

# 重启并加载最新环境变量
pm2 restart xiaohongshu-monitor --update-env || npx pm2 restart xiaohongshu-monitor --update-env
pm2 list || npx pm2 list
```

发布后验证：

```bash
# 本机健康检查
curl -s http://127.0.0.1:3010/health

# 本机页面内容检查（示例：检查新前端标记）
curl -s http://127.0.0.1:3010/ | grep -n "addProductHelper"

# 域名检查（确认反代已指向当前服务）
curl -s http://xhsmonitor.supersolo.me/ | grep -n "addProductHelper"
```

常见问题：

- `fatal: detected dubious ownership`：
  - 执行 `git config --global --add safe.directory /www/wwwroot/rednote_monitor`
- `pm2: command not found`：
  - 改用 `npx pm2 ...`
- 本机已更新但域名仍旧版本：
  - 检查反向代理配置：
  - `grep -R "xhsmonitor.supersolo.me" /www/server/panel/vhost/nginx`
  - `grep -R "proxy_pass" /www/server/panel/vhost/nginx`

## 环境变量

可以按 [.env.example](/Users/yalin/projects/xiaohongshu-monitor2/.env.example) 创建 `.env`：

```env
STORAGE_DRIVER=sqlite
SQLITE_PATH=./data/monitor.db
PUPPETEER_EXECUTABLE_PATH=/Applications/Google Chrome.app/Contents/MacOS/Google Chrome
PORT=3000
```

说明：
- `STORAGE_DRIVER` 当前只实现了 `sqlite`
- `SQLITE_PATH` 不填时默认使用 `./data/monitor.db`
- `PUPPETEER_EXECUTABLE_PATH` 用于指定本机 Chrome 路径
- `PORT` 默认为 `3000`

## 数据存储

当前主存储：

- [data/monitor.db](/Users/yalin/projects/xiaohongshu-monitor2/data/monitor.db)

兼容导入来源：

- [data/products.json](/Users/yalin/projects/xiaohongshu-monitor2/data/products.json)
- [data/sales_data.json](/Users/yalin/projects/xiaohongshu-monitor2/data/sales_data.json)
- [data/config.json](/Users/yalin/projects/xiaohongshu-monitor2/data/config.json)

启动逻辑：

1. 服务启动时创建 SQLite 表
2. 如果数据库为空，会自动把旧 JSON 数据导入 SQLite
3. 导入完成后，后续读写都只走 SQLite

## 数据含义

- `商品总销量`: 当前抓取到的商品累计销量
- `店铺总销量`: 当前抓取到的店铺累计销量
- `商品日销量`: 今天商品总销量减去昨天商品总销量
- `店铺日销量`: 今天店铺总销量减去昨天店铺总销量
- `日 GMV`: 商品日销量乘以当前价格

说明：
- 这不是“距离上次刷新新增多少”
- 而是“今天相对昨天的累计差值”

## 自动刷新

- 定时任务：每小时 `0` 分执行一次
- 时区：`Asia/Shanghai`
- 服务启动后 5 分钟会额外执行一次初始刷新

如果当天重复刷新：
- 商品主表会更新到最新快照
- 每个商品当天只保留一条日快照记录
- 自动刷新和手动刷新会覆盖当天快照，不会重复追加多条同日记录

## 接口

主要接口：

- `POST /api/products` 添加商品
- `GET /api/products` 获取商品列表
- `POST /api/products/:id/refresh` 刷新单个商品
- `GET /api/products/:id/trend` 获取趋势数据
- `DELETE /api/products/:id` 删除商品
- `GET /health` 健康检查

健康检查示例：

```bash
curl http://localhost:3000/health
```

## 项目结构

```text
xiaohongshu-monitor2/
├── server_simple.js
├── store.js
├── sqliteStore.js
├── public/
├── data/
│   ├── monitor.db
│   ├── products.json
│   ├── sales_data.json
│   └── config.json
├── package.json
└── README.md
```

## 存储层设计

当前项目已经把存储逻辑收口到统一入口：

- [store.js](/Users/yalin/projects/xiaohongshu-monitor2/store.js)

当前 SQLite 实现：

- [sqliteStore.js](/Users/yalin/projects/xiaohongshu-monitor2/sqliteStore.js)

业务层只依赖这些接口：

- `verifyConnection`
- `getProductById`
- `getProductByUrl`
- `createProduct`
- `updateProductSnapshot`
- `upsertDailySnapshot`
- `listProductsWithMetrics`
- `getTrendData`
- `deleteProduct`

这意味着以后要切 PostgreSQL，原则上只需要：

1. 新增一个 PostgreSQL 版 store
2. 在 [store.js](/Users/yalin/projects/xiaohongshu-monitor2/store.js) 里切换驱动
3. 不改前端和主要路由

## 当前限制

- 更适合单机运行，不适合多实例并发部署
- 不适合直接部署到 Vercel 这类无状态环境
- Chrome 路径目前默认写的是 macOS 本地路径
- 还没有正式测试体系
- 仓库里有历史备份文件，工程化还没完全收尾

## 后续上线建议

如果是你自己本地长期使用：
- 现在这版 SQLite 已经够用

如果后续要正式上线：
- 优先改到 PostgreSQL / MySQL 这类服务端数据库
- 再考虑部署到云服务器或容器平台

## 免责声明

本工具仅供学习和研究使用，请遵守相关网站的使用条款和法律法规。
