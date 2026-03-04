const STORAGE_DRIVER = process.env.STORAGE_DRIVER || 'sqlite';

const drivers = {
    sqlite: require('./sqliteStore')
};

const store = drivers[STORAGE_DRIVER];

if (!store) {
    throw new Error(`不支持的存储驱动: ${STORAGE_DRIVER}`);
}

module.exports = {
    STORAGE_DRIVER,
    ...store
};
