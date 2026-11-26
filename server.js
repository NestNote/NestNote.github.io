/**
 * NestNote — Render & Local Web Server
 * Zero external dependencies: pure Node.js built-in modules (http, fs, path, crypto, zlib)
 * Supports Family Multi-Access (Co-Parents) & Real-time SSE Sync
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const zlib = require('zlib');

const PORT = process.env.PORT || 3000;
const DB_FILE = path.join(__dirname, 'nestnote_data.json');
const LEGACY_DB_FILE = path.join(__dirname, 'chrono_data.json');
const SALT = 'chrono_clinical_salt_2026';

// -------------------------------------------------------------
// REAL-TIME SSE BROADCASTER (CO-PARENT & MULTI-DEVICE SYNC)
// -------------------------------------------------------------
const sseClients = new Map(); // userId -> Set<res>

function addSseClient(userId, res) {
    if (!sseClients.has(userId)) sseClients.set(userId, new Set());
    sseClients.get(userId).add(res);
}

function removeSseClient(userId, res) {
    const set = sseClients.get(userId);
    if (!set) return;
    set.delete(res);
    if (set.size === 0) sseClients.delete(userId);
}

function getChildUserIds(child) {
    if (!child) return [];
    const ids = [child.userId];
    if (Array.isArray(child.familyMembers)) {
        child.familyMembers.forEach(m => ids.push(typeof m === 'string' ? m : m.userId));
    }
    return ids.filter(Boolean);
}

function broadcastToUsers(userIds, payload) {
    const msg = `data: ${JSON.stringify(payload)}\n\n`;
    const unique = new Set((userIds || []).filter(Boolean));
    for (const uid of unique) {
        const set = sseClients.get(uid);
        if (!set) continue;
        for (const clientRes of set) {
            try { clientRes.write(msg); } catch {}
        }
    }
}

// -------------------------------------------------------------
// DATA STORE (CLEAN DATABASE WITHOUT SAMPLE DATA)
// -------------------------------------------------------------
let db = {
    users: {},           // userId -> user
    usersByUsername: {}, // username -> userId
    sessions: {},        // token -> userId
    children: {},        // childId -> child
    childrenByUser: {}   // userId -> [childId]
};

function hashPassword(pass) {
    return crypto.createHash('sha256').update(SALT + pass).digest('hex');
}

function loadDatabase() {
    try {
        const sourceFile = fs.existsSync(DB_FILE)
            ? DB_FILE
            : (fs.existsSync(LEGACY_DB_FILE) ? LEGACY_DB_FILE : null);
        if (sourceFile) {
            const raw = fs.readFileSync(sourceFile, 'utf-8');
            db = JSON.parse(raw);
            console.log('Database loaded from', sourceFile);
            if (sourceFile !== DB_FILE) saveDatabase();
            return;
        }
    } catch (e) {
        console.error('Failed to read DB, initializing a fresh clean database:', e.message);
    }

    db = {
        users: {},
        usersByUsername: {},
        sessions: {},
        children: {},
        childrenByUser: {}
    };
    saveDatabase();
    console.log('Initialized clean NestNote database without sample data');
}

function saveDatabase() {
    try {
        fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2), 'utf-8');
    } catch (e) {
        console.error('Failed to save DB:', e.message);
    }
}

loadDatabase();

// -------------------------------------------------------------
// ACCESS CHECK FOR CHILD PROFILE (CO-PARENTS)
// -------------------------------------------------------------
function hasChildAccess(child, userId) {
    if (!child || !userId) return false;
    if (child.userId === userId) return true;
    if (Array.isArray(child.familyMembers)) {
        return child.familyMembers.some(m => (typeof m === 'string' ? m === userId : m.userId === userId));
    }
    return false;
}

function generateFamilyCode() {
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
    let code = 'NEST-';
    for (let i = 0; i < 4; i++) {
        code += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return code;
}

function dailyWaterGoalMl(ageYr, weightKg) {
    const a = Number(ageYr);
    if (a > 0) {
        if (a < 0.5) return 700;
        if (a < 1) return 800;
        if (a < 2) return 1000;
        if (a < 4) return 1200;
        if (a < 6) return 1400;
        if (a < 9) return 1600;
        if (a < 12) return 1900;
        if (a < 14) return 2100;
        return 2400;
    }
    const w = Number(weightKg) || 12;
    return w <= 10 ? Math.round(w * 100) : Math.round(1000 + (w - 10) * 50);
}

// -------------------------------------------------------------
// HTTP REQUEST HELPERS
// -------------------------------------------------------------
function sendJson(res, statusCode, data) {
    res.writeHead(statusCode, {
        'Content-Type': 'application/json; charset=utf-8',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS'
    });
    res.end(JSON.stringify(data));
}

function parseJsonBody(req) {
    return new Promise((resolve, reject) => {
        let body = '';
        req.on('data', chunk => {
            body += chunk;
            if (body.length > 5 * 1024 * 1024) {
                req.destroy();
                reject(new Error('Payload too large'));
            }
        });
        req.on('end', () => {
            if (!body.trim()) return resolve({});
            try {
                resolve(JSON.parse(body));
            } catch (err) {
                reject(err);
            }
        });
        req.on('error', reject);
    });
}

function getAuthUserId(req, parsedUrl) {
    const header = req.headers['authorization'];
    if (header && header.startsWith('Bearer ')) {
        const token = header.slice(7).trim();
        return db.sessions[token] || null;
    }
    if (parsedUrl) {
        const qTok = parsedUrl.searchParams.get('token');
        if (qTok) return db.sessions[qTok.trim()] || null;
    }
    return null;
}

// -------------------------------------------------------------
// MAIN HTTP SERVER
// -------------------------------------------------------------
const server = http.createServer(async (req, res) => {
    if (req.method === 'OPTIONS') {
        res.writeHead(204, {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS'
        });
        return res.end();
    }

    const parsedUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const pathname = parsedUrl.pathname;
    const method = req.method.toUpperCase();

    // Health check endpoint (for Render monitoring)
    if (pathname === '/api/health' || pathname === '/healthz') {
        return sendJson(res, 200, { status: 'ok', service: 'nestnote', uptime: process.uptime() });
    }

    // Real-time Server-Sent Events stream (instant co-parent & multi-device sync)
    if (pathname === '/api/stream' && method === 'GET') {
        const authUserId = getAuthUserId(req, parsedUrl);
        if (!authUserId) {
            return sendJson(res, 401, { success: false, message: 'Authentication required' });
        }
        res.writeHead(200, {
            'Content-Type': 'text/event-stream; charset=utf-8',
            'Cache-Control': 'no-cache, no-transform',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Origin': '*',
            'X-Accel-Buffering': 'no'
        });
        res.write(`data: ${JSON.stringify({ type: 'connected', ts: Date.now() })}\n\n`);
        addSseClient(authUserId, res);

        const pingTimer = setInterval(() => {
            try { res.write(': ping\n\n'); } catch {}
        }, 25000);

        req.on('close', () => {
            clearInterval(pingTimer);
            removeSseClient(authUserId, res);
        });
        return;
    }

    // ---------------- API ROUTES ----------------
    if (pathname.startsWith('/api/')) {
        try {
            // POST /api/register
            if (pathname === '/api/register' && method === 'POST') {
                const body = await parseJsonBody(req);
                const username = (body.username || '').trim().toLowerCase();
                const password = body.password || '';
                const name = (body.name || '').trim();

                if (!username || !password || !name) {
                    return sendJson(res, 400, { success: false, message: 'Please fill in all fields' });
                }

                if (password.length < 6) {
                    return sendJson(res, 400, { success: false, message: 'Password must be at least 6 characters' });
                }

                if (db.usersByUsername[username]) {
                    return sendJson(res, 409, { success: false, message: 'An account with this email or username already exists' });
                }

                const userId = 'u_' + Date.now() + '_' + crypto.randomBytes(2).toString('hex');
                const user = {
                    id: userId,
                    username,
                    name,
                    passwordHash: hashPassword(password),
                    theme: 'cerulean',
                    unit: 'metric',
                    alarmOn: true,
                    createdAt: new Date().toISOString()
                };

                const token = 'nn_' + crypto.randomBytes(20).toString('hex');
                db.users[userId] = user;
                db.usersByUsername[username] = userId;
                db.sessions[token] = userId;
                db.childrenByUser[userId] = [];
                saveDatabase();

                const safeUser = { ...user };
                delete safeUser.passwordHash;

                return sendJson(res, 200, {
                    success: true,
                    data: { token, user: safeUser, children: [] }
                });
            }

            // POST /api/login
            if (pathname === '/api/login' && method === 'POST') {
                const body = await parseJsonBody(req);
                const username = (body.username || '').trim().toLowerCase();
                const password = body.password || '';

                if (!username || !password) {
                    return sendJson(res, 400, { success: false, message: 'Please enter both username and password' });
                }

                const userId = db.usersByUsername[username];
                if (!userId || !db.users[userId]) {
                    return sendJson(res, 401, { success: false, message: 'Invalid email/username or password' });
                }

                const user = db.users[userId];
                if (user.passwordHash !== hashPassword(password)) {
                    return sendJson(res, 401, { success: false, message: 'Invalid email/username or password' });
                }

                const token = 'nn_' + crypto.randomBytes(20).toString('hex');
                db.sessions[token] = userId;
                saveDatabase();

                const childIds = db.childrenByUser[userId] || [];
                const userChildren = childIds.map(cid => db.children[cid]).filter(Boolean);

                const safeUser = { ...user };
                delete safeUser.passwordHash;

                return sendJson(res, 200, {
                    success: true,
                    data: { token, user: safeUser, children: userChildren }
                });
            }

            // POST /api/logout
            if (pathname === '/api/logout' && method === 'POST') {
                const header = req.headers['authorization'];
                if (header && header.startsWith('Bearer ')) {
                    const token = header.slice(7).trim();
                    delete db.sessions[token];
                    saveDatabase();
                }
                return sendJson(res, 200, { success: true, message: 'Signed out successfully' });
            }

            // Authenticated routes
            const authUserId = getAuthUserId(req, parsedUrl);
            if (!authUserId) {
                return sendJson(res, 401, { success: false, message: 'Authentication required' });
            }
            const activeUser = db.users[authUserId];

            // GET /api/me (Sync user profile and all family children)
            if (pathname === '/api/me' && method === 'GET') {
                if (!activeUser) return sendJson(res, 404, { success: false, message: 'User not found' });

                const childIds = db.childrenByUser[authUserId] || [];
                const userChildren = childIds.map(cid => db.children[cid]).filter(Boolean);

                const safeUser = { ...activeUser };
                delete safeUser.passwordHash;
                return sendJson(res, 200, { success: true, data: { user: safeUser, children: userChildren } });
            }

            // PUT /api/user/theme
            if (pathname === '/api/user/theme' && method === 'PUT') {
                const body = await parseJsonBody(req);
                const theme = body.theme || 'cerulean';
                if (db.users[authUserId]) {
                    db.users[authUserId].theme = theme;
                    saveDatabase();
                    broadcastToUsers([authUserId], { type: 'prefs', ts: Date.now() });
                }
                return sendJson(res, 200, { success: true, data: theme });
            }

            // PUT /api/user/prefs (Full user preference sync: theme, unit, alarmOn)
            if (pathname === '/api/user/prefs' && method === 'PUT') {
                const body = await parseJsonBody(req);
                if (db.users[authUserId]) {
                    if (body.theme) db.users[authUserId].theme = String(body.theme);
                    if (body.unit === 'metric' || body.unit === 'imperial') db.users[authUserId].unit = body.unit;
                    if (typeof body.alarmOn === 'boolean') db.users[authUserId].alarmOn = body.alarmOn;
                    saveDatabase();
                    broadcastToUsers([authUserId], { type: 'prefs', ts: Date.now() });
                }
                const safeUser = { ...db.users[authUserId] };
                delete safeUser.passwordHash;
                return sendJson(res, 200, { success: true, data: safeUser });
            }

            // POST /api/children (Create new child profile)
            if (pathname === '/api/children' && method === 'POST') {
                const body = await parseJsonBody(req);
                const name = (body.name || '').trim();
                const age = parseFloat(body.age);
                const weight = parseFloat(body.weight);

                if (!name || !(age > 0) || !(weight > 0)) {
                    return sendJson(res, 400, { success: false, message: 'Child name, age, and weight are required' });
                }

                const childId = 'c_' + Date.now() + '_' + crypto.randomBytes(2).toString('hex');
                const requestedCode = String(body.familyCode || '').trim().toUpperCase();
                const codeInUse = requestedCode && Object.values(db.children).some(c => (c.familyCode || '').toUpperCase() === requestedCode);
                const familyCode = (/^(NEST|FAM)-[A-Z0-9]{4,8}$/.test(requestedCode) && !codeInUse) ? requestedCode : generateFamilyCode();

                const newChild = {
                    id: childId,
                    userId: authUserId,
                    familyCode,
                    familyMembers: [
                        { userId: authUserId, name: activeUser.name, role: 'Profile creator' }
                    ],
                    name,
                    age,
                    weight,
                    height: parseFloat(body.height) || 0,
                    allergies: (body.allergies || 'None').trim() || 'None',
                    doctorPhone: (body.doctorPhone || '').trim(),
                    episode: (body.episode || 'Active diary').trim(),
                    points48h: Array.isArray(body.points48h) ? body.points48h.slice(-200) : [],
                    reminders: Array.isArray(body.reminders) ? body.reminders.slice(0, 50) : [],
                    savedMeds: Array.isArray(body.savedMeds) ? body.savedMeds.slice(0, 10) : [],
                    urineLogs: Array.isArray(body.urineLogs) ? body.urineLogs.slice(-100) : [],
                    archivedEpisodes: Array.isArray(body.archivedEpisodes) ? body.archivedEpisodes.slice(-30) : [],
                    createdAt: new Date().toISOString()
                };

                db.children[childId] = newChild;
                db.childrenByUser[authUserId] = db.childrenByUser[authUserId] || [];
                db.childrenByUser[authUserId].push(childId);
                saveDatabase();
                broadcastToUsers([authUserId], { type: 'child_added', childId, ts: Date.now() });

                return sendJson(res, 200, { success: true, data: newChild });
            }

            // POST /api/family/join (Family Multi-Access: Co-Parents)
            if (pathname === '/api/family/join' && method === 'POST') {
                const body = await parseJsonBody(req);
                const rawCode = (body.code || '').trim().toUpperCase().replace(/\s+/g, '');

                if (!rawCode) {
                    return sendJson(res, 400, { success: false, message: 'Please enter a family invite code' });
                }

                const candidates = [rawCode];
                if (/^[A-Z0-9]{4}$/.test(rawCode)) {
                    candidates.push('NEST-' + rawCode, 'FAM-' + rawCode);
                }

                const matchedChild = Object.values(db.children).find(c =>
                    candidates.includes((c.familyCode || '').toUpperCase())
                );

                if (!matchedChild) {
                    return sendJson(res, 404, { success: false, message: 'Family code not found. Please check the code (e.g. NEST-7X9K)' });
                }

                matchedChild.familyMembers = matchedChild.familyMembers || [];
                const alreadyMember = matchedChild.familyMembers.some(m => (typeof m === 'string' ? m === authUserId : m.userId === authUserId));

                if (!alreadyMember) {
                    matchedChild.familyMembers.push({
                        userId: authUserId,
                        name: activeUser.name,
                        role: 'Co-parent'
                    });
                }

                db.childrenByUser[authUserId] = db.childrenByUser[authUserId] || [];
                if (!db.childrenByUser[authUserId].includes(matchedChild.id)) {
                    db.childrenByUser[authUserId].push(matchedChild.id);
                }

                saveDatabase();
                broadcastToUsers(getChildUserIds(matchedChild), {
                    type: 'family_joined',
                    childId: matchedChild.id,
                    member: activeUser.name,
                    ts: Date.now()
                });

                return sendJson(res, 200, {
                    success: true,
                    message: `Family access connected! Profile "${matchedChild.name}" is now synced.`,
                    data: matchedChild
                });
            }

            // DELETE /api/children/:id
            const childDelMatch = pathname.match(/^\/api\/children\/([^/]+)$/);
            if (childDelMatch && method === 'DELETE') {
                const childId = childDelMatch[1];
                const child = db.children[childId];
                if (!child || !hasChildAccess(child, authUserId)) {
                    return sendJson(res, 404, { success: false, message: 'Profile not found or access denied' });
                }

                const affectedUsers = getChildUserIds(child);
                delete db.children[childId];
                Object.keys(db.childrenByUser).forEach(uid => {
                    db.childrenByUser[uid] = db.childrenByUser[uid].filter(id => id !== childId);
                });
                saveDatabase();
                broadcastToUsers(affectedUsers, { type: 'child_deleted', childId, ts: Date.now() });
                return sendJson(res, 200, { success: true, message: 'Profile deleted' });
            }

            // POST /api/children/:id/entries (Add temperature, medicine, or water entry)
            const entryAddMatch = pathname.match(/^\/api\/children\/([^/]+)\/entries$/);
            if (entryAddMatch && method === 'POST') {
                const childId = entryAddMatch[1];
                const child = db.children[childId];
                if (!child || !hasChildAccess(child, authUserId)) {
                    return sendJson(res, 404, { success: false, message: 'Child not found or access denied' });
                }

                const body = await parseJsonBody(req);
                const nowMs = body.createdAtMs || Date.now();
                const VALID_TYPES = ['temp', 'med', 'water', 'note', 'sleep', 'food', 'diaper', 'breathing', 'pulse', 'custom'];
                const entryType = VALID_TYPES.includes(body.type)
                    ? body.type
                    : (body.med ? 'med' : (body.drink || body.ml ? 'water' : (body.skin || body.comment ? 'note' : 'temp')));

                const parsedTemp = body.temp !== undefined && body.temp !== null ? parseFloat(body.temp) : NaN;
                const tempVal = entryType === 'temp' ? (Number.isNaN(parsedTemp) ? 37.0 : parsedTemp) : null;

                child.points48h = Array.isArray(child.points48h) ? child.points48h : [];

                const finalMl = entryType === 'water'
                    ? Math.max(1, Number(body.ml) || 0)
                    : (body.ml !== undefined && body.ml !== null ? Number(body.ml) : null);

                const skinList = Array.isArray(body.skin)
                    ? body.skin.map(s => String(s || '').trim()).filter(Boolean).slice(0, 12)
                    : (body.skin ? [String(body.skin).trim()].filter(Boolean) : []);
                const breathingTags = Array.isArray(body.breathingTags)
                    ? body.breathingTags.map(s => String(s || '').trim()).filter(Boolean).slice(0, 12)
                    : [];
                const commentStr = body.comment ? String(body.comment).trim().slice(0, 800) : '';
                const rawPhoto = typeof body.photo === 'string' ? body.photo.trim() : '';
                const safePhoto = (rawPhoto.startsWith('data:image/') && rawPhoto.length <= 450000) ? rawPhoto : '';

                child.points48h.forEach(p => {
                    if (p.createdAtMs) p.hours_ago = +((nowMs - p.createdAtMs) / 3600000).toFixed(2);
                    else p.hours_ago = Math.min(48, (p.hours_ago || 0) + 1);
                });

                const newEntry = {
                    id: 'e_' + nowMs + '_' + crypto.randomBytes(2).toString('hex'),
                    type: entryType,
                    temp: tempVal,
                    title: body.title ? String(body.title).trim().slice(0, 140) : null,
                    dose: body.dose ? String(body.dose).trim().slice(0, 60) : null,
                    ml: finalMl,
                    wet: !!body.wet,
                    skin: skinList,
                    comment: commentStr,
                    photo: safePhoto,
                    hours: body.hours !== undefined && body.hours !== null && !Number.isNaN(Number(body.hours)) ? Number(body.hours) : null,
                    quality: body.quality ? String(body.quality).trim().slice(0, 60) : '',
                    diaperType: body.diaperType ? String(body.diaperType).trim().slice(0, 20) : '',
                    stool: body.stool ? String(body.stool).trim().slice(0, 60) : '',
                    breathingTags,
                    respRate: body.respRate !== undefined && body.respRate !== null && !Number.isNaN(Number(body.respRate)) ? Number(body.respRate) : null,
                    bpm: body.bpm !== undefined && body.bpm !== null && !Number.isNaN(Number(body.bpm)) ? Number(body.bpm) : null,
                    customName: body.customName ? String(body.customName).trim().slice(0, 80) : '',
                    customVal: body.customVal !== undefined && body.customVal !== null ? String(body.customVal).trim().slice(0, 60) : '',
                    customUnit: body.customUnit ? String(body.customUnit).trim().slice(0, 30) : '',
                    drink: entryType === 'water' ? `Water ${finalMl} ml` : (body.drink ? String(body.drink).trim() : null),
                    med: body.med ? String(body.med).trim() : null,
                    note: (body.note || (entryType === 'temp' ? 'Reading' : entryType === 'med' ? '' : entryType === 'water' ? 'Hydration' : '')).trim(),
                    time: body.time ? String(body.time).trim() : null,
                    author: activeUser.name || 'Parent',
                    hours_ago: 0,
                    createdAtMs: nowMs,
                    timestamp: body.time ? String(body.time).trim() : 'Just now'
                };

                child.points48h.push(newEntry);
                saveDatabase();
                broadcastToUsers(getChildUserIds(child), {
                    type: 'entry_added',
                    childId,
                    entryType,
                    author: newEntry.author,
                    ts: Date.now()
                });

                return sendJson(res, 200, { success: true, data: newEntry });
            }

            // PUT /api/children/:id/meta (Update child profile details)
            const childMetaMatch = pathname.match(/^\/api\/children\/([^/]+)\/meta$/);
            if (childMetaMatch && method === 'PUT') {
                const childId = childMetaMatch[1];
                const child = db.children[childId];
                if (!child || !hasChildAccess(child, authUserId)) {
                    return sendJson(res, 404, { success: false, message: 'Child not found or access denied' });
                }

                const body = await parseJsonBody(req);
                if (body.name !== undefined && String(body.name).trim()) child.name = String(body.name).trim();
                if (body.weight !== undefined) child.weight = parseFloat(body.weight) || child.weight;
                if (body.height !== undefined) child.height = parseFloat(body.height) || child.height;
                if (body.age !== undefined) child.age = parseFloat(body.age) || child.age;
                if (body.allergies !== undefined) child.allergies = String(body.allergies).trim() || 'None';
                if (body.doctorPhone !== undefined) child.doctorPhone = String(body.doctorPhone).trim();
                if (body.episode !== undefined) child.episode = String(body.episode).trim();
                if (body.lastUrineMs !== undefined) child.lastUrineMs = body.lastUrineMs;
                if (Array.isArray(body.urineLogs)) child.urineLogs = body.urineLogs;
                if (Array.isArray(body.vaccines)) child.vaccines = body.vaccines;
                if (Array.isArray(body.archivedEpisodes)) child.archivedEpisodes = body.archivedEpisodes;
                if (Array.isArray(body.savedMeds)) child.savedMeds = body.savedMeds;

                saveDatabase();
                broadcastToUsers(getChildUserIds(child), { type: 'child_updated', childId, ts: Date.now() });
                return sendJson(res, 200, { success: true, data: child });
            }

            // DELETE /api/children/:id/entries/:entryId
            const entryDelMatch = pathname.match(/^\/api\/children\/([^/]+)\/entries\/([^/]+)$/);
            if (entryDelMatch && method === 'DELETE') {
                const [, childId, entryId] = entryDelMatch;
                const child = db.children[childId];
                if (!child || !hasChildAccess(child, authUserId)) {
                    return sendJson(res, 404, { success: false, message: 'Child not found' });
                }

                child.points48h = (child.points48h || []).filter(p => p.id !== entryId);
                saveDatabase();
                broadcastToUsers(getChildUserIds(child), { type: 'entry_deleted', childId, ts: Date.now() });
                return sendJson(res, 200, { success: true, message: 'Entry deleted' });
            }

            // POST /api/children/:id/reminders
            const remAddMatch = pathname.match(/^\/api\/children\/([^/]+)\/reminders$/);
            if (remAddMatch && method === 'POST') {
                const childId = remAddMatch[1];
                const child = db.children[childId];
                if (!child || !hasChildAccess(child, authUserId)) {
                    return sendJson(res, 404, { success: false, message: 'Child not found' });
                }

                const body = await parseJsonBody(req);
                const newRem = {
                    id: 'r_' + Date.now() + '_' + crypto.randomBytes(2).toString('hex'),
                    title: (body.title || 'Reminder').trim(),
                    time: (body.time || 'Today').trim(),
                    author: activeUser.name || 'Parent',
                    done: false
                };

                child.reminders = Array.isArray(child.reminders) ? child.reminders : [];
                child.reminders.unshift(newRem);
                saveDatabase();
                broadcastToUsers(getChildUserIds(child), { type: 'rem_added', childId, ts: Date.now() });
                return sendJson(res, 200, { success: true, data: newRem });
            }

            // PUT /api/children/:id/reminders/:remId/toggle
            const remToggleMatch = pathname.match(/^\/api\/children\/([^/]+)\/reminders\/([^/]+)\/toggle$/);
            if (remToggleMatch && method === 'PUT') {
                const [, childId, remId] = remToggleMatch;
                const child = db.children[childId];
                if (!child || !hasChildAccess(child, authUserId)) {
                    return sendJson(res, 404, { success: false, message: 'Child not found' });
                }

                const rem = (child.reminders || []).find(r => r.id === remId);
                if (!rem) return sendJson(res, 404, { success: false, message: 'Reminder not found' });

                rem.done = !rem.done;
                saveDatabase();
                broadcastToUsers(getChildUserIds(child), { type: 'rem_toggled', childId, ts: Date.now() });
                return sendJson(res, 200, { success: true, data: rem.done });
            }

            // DELETE /api/children/:id/reminders/:remId
            const remDelMatch = pathname.match(/^\/api\/children\/([^/]+)\/reminders\/([^/]+)$/);
            if (remDelMatch && method === 'DELETE') {
                const [, childId, remId] = remDelMatch;
                const child = db.children[childId];
                if (!child || !hasChildAccess(child, authUserId)) {
                    return sendJson(res, 404, { success: false, message: 'Child not found' });
                }

                child.reminders = (child.reminders || []).filter(r => r.id !== remId);
                saveDatabase();
                broadcastToUsers(getChildUserIds(child), { type: 'rem_deleted', childId, ts: Date.now() });
                return sendJson(res, 200, { success: true, message: 'Reminder deleted' });
            }

            return sendJson(res, 404, { success: false, message: 'API route not found' });
        } catch (err) {
            console.error('API Error:', err);
            return sendJson(res, 500, { success: false, message: 'Internal server error' });
        }
    }

    function sendStaticFile(filePath, contentType) {
        const raw = fs.readFileSync(filePath);
        const acceptEnc = String(req.headers['accept-encoding'] || '');
        if (acceptEnc.includes('gzip')) {
            const gz = zlib.gzipSync(raw);
            res.writeHead(200, {
                'Content-Type': contentType,
                'Content-Encoding': 'gzip',
                'Content-Length': gz.length,
                'Vary': 'Accept-Encoding',
                'Cache-Control': 'no-cache'
            });
            return res.end(gz);
        }
        res.writeHead(200, {
            'Content-Type': contentType,
            'Content-Length': raw.length,
            'Cache-Control': 'no-cache'
        });
        return res.end(raw);
    }

    // ---------------- STATIC SERVING (PWA + HTML) ----------------
    if (pathname === '/manifest.json') {
        const manifestPath = fs.existsSync(path.join(__dirname, 'static', 'manifest.json'))
            ? path.join(__dirname, 'static', 'manifest.json')
            : path.join(__dirname, 'manifest.json');
        if (fs.existsSync(manifestPath)) {
            return sendStaticFile(manifestPath, 'application/manifest+json; charset=utf-8');
        }
    }

    if (pathname === '/sw.js') {
        const swPath = fs.existsSync(path.join(__dirname, 'static', 'sw.js'))
            ? path.join(__dirname, 'static', 'sw.js')
            : path.join(__dirname, 'sw.js');
        if (fs.existsSync(swPath)) {
            return sendStaticFile(swPath, 'application/javascript; charset=utf-8');
        }
    }

    if (pathname === '/icon.svg' || pathname === '/favicon.ico') {
        const iconPath = fs.existsSync(path.join(__dirname, 'static', 'icon.svg'))
            ? path.join(__dirname, 'static', 'icon.svg')
            : path.join(__dirname, 'icon.svg');
        if (fs.existsSync(iconPath)) {
            return sendStaticFile(iconPath, 'image/svg+xml; charset=utf-8');
        }
    }

    let indexPath = path.join(__dirname, 'static', 'index.html');
    if (!fs.existsSync(indexPath)) {
        indexPath = path.join(__dirname, 'index.html');
    }

    if (fs.existsSync(indexPath)) {
        return sendStaticFile(indexPath, 'text/html; charset=utf-8');
    } else {
        res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('index.html not found');
    }
});

server.listen(PORT, '0.0.0.0', () => {
    console.log('===========================================================');
    console.log('NestNote — Server Ready (Render / Local Node + SSE Sync)');
    console.log('===========================================================');
    console.log(`Server listening on port: ${PORT}`);
    console.log(`Open in browser:          http://localhost:${PORT}`);
    console.log(`Render health check:      http://localhost:${PORT}/api/health`);
    console.log('===========================================================');
});
