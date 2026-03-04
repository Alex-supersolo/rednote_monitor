module.exports = {
    apps: [
        {
            name: 'xiaohongshu-monitor',
            script: './server_simple.js',
            cwd: __dirname,
            instances: 1,
            exec_mode: 'fork',
            autorestart: true,
            watch: false,
            max_memory_restart: '500M',
            env: {
                NODE_ENV: 'production',
                STORAGE_DRIVER: process.env.STORAGE_DRIVER || 'sqlite',
                SQLITE_PATH: process.env.SQLITE_PATH || './data/monitor.db',
                PUPPETEER_EXECUTABLE_PATH: process.env.PUPPETEER_EXECUTABLE_PATH || '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
                PORT: process.env.PORT || 3010
            }
        }
    ]
};
