const crypto = require('crypto');

const SESSION_COOKIE_NAME = 'xhs_monitor_session';
const SESSION_MAX_AGE_DAYS = 30;
const SESSION_MAX_AGE_SECONDS = SESSION_MAX_AGE_DAYS * 24 * 60 * 60;

function hashPassword(password) {
    const salt = crypto.randomBytes(16).toString('hex');
    const derivedKey = crypto.scryptSync(password, salt, 64).toString('hex');
    return `${salt}:${derivedKey}`;
}

function verifyPassword(password, storedHash) {
    if (!password || !storedHash || !storedHash.includes(':')) {
        return false;
    }

    const [salt, expectedHash] = storedHash.split(':');
    const actualHash = crypto.scryptSync(password, salt, 64).toString('hex');
    const expectedBuffer = Buffer.from(expectedHash, 'hex');
    const actualBuffer = Buffer.from(actualHash, 'hex');

    if (expectedBuffer.length !== actualBuffer.length) {
        return false;
    }

    return crypto.timingSafeEqual(expectedBuffer, actualBuffer);
}

function createSessionToken() {
    return crypto.randomBytes(32).toString('hex');
}

function buildSessionExpiryDate(now = new Date()) {
    return new Date(now.getTime() + SESSION_MAX_AGE_SECONDS * 1000);
}

function parseCookies(cookieHeader = '') {
    return cookieHeader.split(';').reduce((cookies, pair) => {
        const [rawName, ...rawValue] = pair.split('=');
        const name = rawName ? rawName.trim() : '';
        if (!name) {
            return cookies;
        }

        cookies[name] = decodeURIComponent(rawValue.join('=').trim());
        return cookies;
    }, {});
}

function serializeSessionCookie(token, expiresAt) {
    const expires = expiresAt instanceof Date ? expiresAt : new Date(expiresAt);
    return [
        `${SESSION_COOKIE_NAME}=${encodeURIComponent(token)}`,
        'Path=/',
        'HttpOnly',
        'SameSite=Lax',
        `Max-Age=${SESSION_MAX_AGE_SECONDS}`,
        `Expires=${expires.toUTCString()}`
    ].join('; ');
}

function serializeClearSessionCookie() {
    return [
        `${SESSION_COOKIE_NAME}=`,
        'Path=/',
        'HttpOnly',
        'SameSite=Lax',
        'Max-Age=0',
        'Expires=Thu, 01 Jan 1970 00:00:00 GMT'
    ].join('; ');
}

function normalizeUsername(username) {
    return String(username || '').trim();
}

function validateRegistrationInput(username, password, inviteCode) {
    const normalizedUsername = normalizeUsername(username);
    const normalizedInviteCode = String(inviteCode || '').trim();

    if (!/^[\u4e00-\u9fa5a-zA-Z0-9_-]{2,24}$/.test(normalizedUsername)) {
        throw new Error('用户名需为 2-24 位，可包含中文、字母、数字、下划线或中划线');
    }

    if (String(password || '').length < 6) {
        throw new Error('密码长度至少需要 6 位');
    }

    if (!normalizedInviteCode) {
        throw new Error('请输入邀请码');
    }

    return {
        username: normalizedUsername,
        password: String(password),
        inviteCode: normalizedInviteCode
    };
}

function validateLoginInput(username, password) {
    const normalizedUsername = normalizeUsername(username);
    if (!normalizedUsername) {
        throw new Error('请输入用户名');
    }

    if (!password) {
        throw new Error('请输入密码');
    }

    return {
        username: normalizedUsername,
        password: String(password)
    };
}

module.exports = {
    SESSION_COOKIE_NAME,
    SESSION_MAX_AGE_SECONDS,
    buildSessionExpiryDate,
    createSessionToken,
    hashPassword,
    parseCookies,
    serializeClearSessionCookie,
    serializeSessionCookie,
    validateLoginInput,
    validateRegistrationInput,
    verifyPassword
};
