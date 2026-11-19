/**
 * ============================================================================
 * WAstatus-Style HD WhatsApp Status Bot (CommonJS, QR Scan, Zero-DB, VPS-Ready)
 * ============================================================================
 *
 * HOW IT WORKS (SELF-ONLY MODE):
 * 1. Start the bot on your VPS and scan the terminal QR code in WhatsApp
 *    (WhatsApp -> Linked Devices -> Link a Device).
 * 2. In your own "Message yourself" chat, send any video or photo as a
 *    FILE / DOCUMENT (📎 -> Document) so WhatsApp uploads the untouched original.
 * 3. This bot encodes it with FFmpeg (libx264 + AAC) using WAstatus's
 *    Status-safe HD profile (1080p/30fps <= 29s, or add "720p" for 720p <= 59s,
 *    strictly < 15.4 MB) and returns the ready-to-post clip(s) to your chat.
 * 4. Reply/quote any video, photo, or image/video file with `.status`
 *    (or `.status your caption`) to post directly to your WhatsApp Status in
 *    full original quality and native FPS (e.g. 60 fps), honoring your
 *    WhatsApp Status privacy ("My contacts except...").
 * 5. Reply/quote any regular (non-View Once) photo, video, audio, or file with
 *    `.save` to copy it privately to your "Message yourself" inbox.
 * 6. Utility commands: `.menu`, `.ping`, `.runtime`, `.statusinfo`.
 *
 * VPS SETUP (Ubuntu / Debian):
 *   sudo apt update && sudo apt install -y ffmpeg
 *   npm init -y
 *   npm install @whiskeysockets/baileys pino qrcode-terminal
 *   node bot.js
 *
 * OPTIONAL ENVIRONMENT VARIABLES:
 *   STATUS_CONTACTS=14155551234,919876543210  # Extra contacts always included in .status viewers
 *   STATUS_EXCEPT=14155551234,919876543210    # Extra contacts always excluded ("My contacts except...")
 *   MIN_SEND_DELAY_MS=1200                    # Minimum pacing delay between outgoing messages
 *   FFMPEG_PRESET=veryfast                    # x264 speed preset (ultrafast|veryfast|fast|medium)
 *   PHOTO_DURATION_SEC=7                      # Default photo-to-video length in seconds (3..29)
 *   MAX_INPUT_MB=250                          # Max incoming document size accepted
 *   AUTH_DIR=./auth_info                      # Folder where WhatsApp session files are saved
 * ============================================================================
 */

'use strict';

const { spawn } = require('node:child_process');
const fs = require('node:fs/promises');
const { createWriteStream } = require('node:fs');
const { pipeline } = require('node:stream/promises');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');
const pino = require('pino');
const qrcode = require('qrcode-terminal');

// ----------------------------------------------------------------------------
// Configuration (Zero-DB: env-driven settings + local multi-file auth folder)
// ----------------------------------------------------------------------------
const CONFIG = {
  authDir: process.env.AUTH_DIR || path.resolve('./auth_info'),
  allowedNumbers: new Set(
    (process.env.ALLOWED_NUMBERS || '')
      .split(',')
      .map((n) => n.replace(/\D/g, ''))
      .filter(Boolean)
  ),
  statusContacts: (process.env.STATUS_CONTACTS || '')
    .split(',')
    .map((n) => n.replace(/\D/g, ''))
    .filter(Boolean)
    .map((n) => `${n}@s.whatsapp.net`),
  statusExcept: new Set(
    (process.env.STATUS_EXCEPT || '')
      .split(',')
      .map((n) => n.replace(/\D/g, ''))
      .filter(Boolean)
      .map((n) => `${n}@s.whatsapp.net`)
  ),
  ffmpegPreset: process.env.FFMPEG_PRESET || 'veryfast',
  minSendDelayMs: Math.max(500, Number(process.env.MIN_SEND_DELAY_MS || 1200)),
  downloadTimeoutMs: Number(process.env.DOWNLOAD_TIMEOUT_MS || 120000),
  reuploadTimeoutMs: Number(process.env.REUPLOAD_TIMEOUT_MS || 20000),
  ffmpegTimeoutMs: Number(process.env.FFMPEG_TIMEOUT_MS || 300000),
  sendTimeoutMs: Number(process.env.SEND_TIMEOUT_MS || 120000),
  maxInputBytes: Number(process.env.MAX_INPUT_MB || 250) * 1024 * 1024,
  // WhatsApp Status re-compresses/rejects clips over ~16 MB; keep a safety margin.
  maxOutputBytes: 15.4 * 1024 * 1024,
  targetOutputMb: 14.8,
  defaultPhotoDurationSec: Math.min(29, Math.max(3, Number(process.env.PHOTO_DURATION_SEC || 7))),
  // WAstatus tested quality profiles
  profiles: {
    '1080p': {
      name: '1080p HD',
      maxShortEdge: 1080,
      maxLongEdge: 1920,
      partDurationSec: 29,
      fps: 30,
      crf: 20,
      maxVideoBitrateKbps: 5500,
    },
    '720p': {
      name: '720p Extended',
      maxShortEdge: 720,
      maxLongEdge: 1280,
      partDurationSec: 59,
      fps: 30,
      crf: 21,
      maxVideoBitrateKbps: 3000,
    },
  },
};

const VIDEO_EXTS = new Set([
  '.mp4',
  '.mov',
  '.mkv',
  '.webm',
  '.avi',
  '.m4v',
  '.3gp',
  '.ts',
  '.mts',
  '.m2ts',
  '.flv',
  '.wmv',
  '.mpg',
  '.mpeg',
]);
const IMAGE_EXTS = new Set([
  '.jpg',
  '.jpeg',
  '.png',
  '.webp',
  '.bmp',
  '.heic',
  '.heif',
  '.tiff',
  '.tif',
  '.jfif',
  '.avif',
]);

// Track message IDs sent by this bot to prevent reply loops (including self-chat)
const botSentMessageIds = new Set();

// In-memory contact JIDs allowed to view bot's Status updates (zero-DB)
const knownStatusJids = new Set([
  ...CONFIG.statusContacts,
  ...Array.from(CONFIG.allowedNumbers, (n) => `${n}@s.whatsapp.net`),
]);
const CONTACTS_CACHE_FILE = path.join(CONFIG.authDir, 'status-contacts.json');
let contactsCacheSaveTimer = null;

// Sequential in-memory queue so a small VPS never runs competing FFmpeg processes
const jobQueue = [];
let isProcessingQueue = false;
let activeSocket = null;
let isShuttingDown = false;
let cachedBaileys = null;
let reconnectAttempts = 0;
let lastSendTimestamp = 0;
const botStartedAt = Date.now();

const logger = pino({ level: process.env.LOG_LEVEL || 'silent' });

// ----------------------------------------------------------------------------
// Baileys Loader (Supports both CommonJS v6 and ESM v7 seamlessly in CJS)
// ----------------------------------------------------------------------------

async function loadBaileys() {
  if (cachedBaileys) return cachedBaileys;

  let mod;
  try {
    mod = require('@whiskeysockets/baileys');
  } catch (err) {
    if (err && err.code === 'ERR_REQUIRE_ESM') {
      mod = await import('@whiskeysockets/baileys');
    } else {
      throw err;
    }
  }

  const makeWASocket =
    typeof mod.default === 'function'
      ? mod.default
      : typeof mod.makeWASocket === 'function'
        ? mod.makeWASocket
        : mod.default?.default;

  if (typeof makeWASocket !== 'function') {
    throw new Error('Could not resolve makeWASocket from @whiskeysockets/baileys.');
  }

  cachedBaileys = {
    makeWASocket,
    DisconnectReason: mod.DisconnectReason,
    downloadMediaMessage: mod.downloadMediaMessage,
    fetchLatestBaileysVersion: mod.fetchLatestBaileysVersion,
    useMultiFileAuthState: mod.useMultiFileAuthState,
  };

  return cachedBaileys;
}

// ----------------------------------------------------------------------------
// Utility Helpers
// ----------------------------------------------------------------------------

function logInfo(message) {
  console.log(`[${new Date().toISOString()}] [INFO] ${message}`);
}

function logError(message, err) {
  console.error(`[${new Date().toISOString()}] [ERROR] ${message}`, err?.message || err || '');
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function formatRuntime(ms) {
  const totalSec = Math.max(0, Math.floor(ms / 1000));
  const days = Math.floor(totalSec / 86400);
  const hours = Math.floor((totalSec % 86400) / 3600);
  const minutes = Math.floor((totalSec % 3600) / 60);
  const seconds = totalSec % 60;

  const parts = [];
  if (days > 0) parts.push(`${days}d`);
  if (hours > 0 || days > 0) parts.push(`${hours}h`);
  if (minutes > 0 || hours > 0 || days > 0) parts.push(`${minutes}m`);
  parts.push(`${seconds}s`);
  return parts.join(' ');
}

function buildMenuText() {
  return [
    '*HD Status Bot — User Manual*',
    '_Mode: Self-Only (ignores all other users)_',
    '',
    '*1. HD Status Video Converter*',
    '• In your *Message yourself* chat, send any video or photo as a *Document / File* (📎 → Document).',
    '• Default: 1080p HD, 30 fps, auto-split ≤ 29s (< 16 MB).',
    '• Add `720p` in file caption for 720p clips up to 59s.',
    '• Add e.g. `12s` on a photo file to set video length (3–29s).',
    '• Any other caption text is kept; empty caption stays empty.',
    '',
    '*2. Direct Full-Quality Status Post*',
    '• `.status` — Reply to any video, photo, or image/video file (or put `.status` in its caption) to post directly to your Status at full original quality & FPS (including 60 fps).',
    '• `.status Your caption` — Post with a custom caption.',
    '• Automatically respects your WhatsApp Status privacy ("My contacts except...").',
    '',
    '*3. Utilities*',
    '• `.save` — Reply to any regular (non-View Once) photo, video, audio, or file to copy it privately to your inbox.',
    '• `.statusinfo` — Check active Status privacy & viewer count.',
    '• `.ping` — Check bot & WhatsApp server latency.',
    '• `.runtime` — Check bot uptime, RAM usage & queue.',
    '• `.menu` — Show this user manual.',
  ].join('\n');
}

/**
 * Measures real WhatsApp server round-trip latency via XMPP ping IQ,
 * falling back to local event-loop latency if unavailable.
 */
async function measureWhatsAppPing(sock) {
  const startHr = process.hrtime.bigint();
  if (typeof sock.query === 'function') {
    try {
      await withTimeout(
        sock.query({
          tag: 'iq',
          attrs: {
            to: 's.whatsapp.net',
            type: 'get',
            xmlns: 'w:p',
          },
          content: [{ tag: 'ping', attrs: {} }],
        }),
        5000,
        'WhatsApp ping'
      );
      const rttMs = Number(process.hrtime.bigint() - startHr) / 1e6;
      return { ms: Math.round(rttMs), type: 'WhatsApp Server RTT' };
    } catch {
      // Fall back to local response measurement
    }
  }
  const localMs = Number(process.hrtime.bigint() - startHr) / 1e6;
  return { ms: Math.max(1, Math.round(localMs)), type: 'Bot Response' };
}

/**
 * Prevents hanging Baileys calls (e.g. updateMediaMessage / upload) from locking the queue.
 */
function withTimeout(promise, ms, label) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => {
        reject(new Error(`${label} timed out after ${Math.round(ms / 1000)}s`));
      }, ms);
    }),
  ]).finally(() => clearTimeout(timer));
}

function rememberSentMessage(sentMsg) {
  const id = sentMsg?.key?.id;
  if (!id) return;
  botSentMessageIds.add(id);
  // Cap Set size to avoid unbounded memory growth on long-running VPS instances
  if (botSentMessageIds.size > 1000) {
    const oldest = botSentMessageIds.values().next().value;
    botSentMessageIds.delete(oldest);
  }
}

async function safeSendMessage(sock, jid, content, options = {}) {
  // Pace outgoing messages to avoid burst-send patterns that can trigger account flags
  const elapsed = Date.now() - lastSendTimestamp;
  if (elapsed < CONFIG.minSendDelayMs) {
    await sleep(CONFIG.minSendDelayMs - elapsed);
  }
  lastSendTimestamp = Date.now();

  const sent = await withTimeout(
    sock.sendMessage(jid, content, options),
    CONFIG.sendTimeoutMs,
    `sendMessage(${jid})`
  );
  rememberSentMessage(sent);
  return sent;
}

/**
 * Returns the bot owner's own chat JID ("Message yourself") for private notifications.
 */
function getSelfChatJid(sock) {
  return normalizeUserJid(sock.user?.id) || normalizeAnyJid(sock.user?.lid);
}

/**
 * Checks whether a message belongs to the owner's own "Message yourself" chat
 * (handles both @s.whatsapp.net and @lid addressing).
 */
function isSelfChat(sock, msgKey) {
  const selfIds = new Set(
    [normalizeAnyJid(sock.user?.id), normalizeAnyJid(sock.user?.lid)].filter(Boolean)
  );
  if (selfIds.size === 0) return false;

  const chatIds = [
    normalizeAnyJid(msgKey?.remoteJid),
    normalizeAnyJid(msgKey?.remoteJidAlt),
  ].filter(Boolean);

  return chatIds.some((id) => selfIds.has(id));
}

/**
 * Strips device suffixes (e.g. ":12") while keeping "@s.whatsapp.net" or "@lid" domain.
 */
function normalizeAnyJid(jid) {
  if (!jid || typeof jid !== 'string') return null;
  const [userPart, domain] = jid.split('@');
  if (!userPart || !domain) return null;
  const cleanUser = userPart.split(':')[0].trim();
  return cleanUser ? `${cleanUser}@${domain}` : null;
}

/**
 * Normalizes an individual WhatsApp JID by stripping device suffixes (e.g. ":12")
 * and keeping only standard "@s.whatsapp.net" user JIDs for Status broadcast delivery.
 */
function normalizeUserJid(jid) {
  if (!jid || typeof jid !== 'string') return null;
  if (
    jid.endsWith('@g.us') ||
    jid.endsWith('@newsletter') ||
    jid.endsWith('@broadcast') ||
    jid.endsWith('@lid')
  ) {
    return null;
  }
  const [userPart, domain] = jid.split('@');
  if (!userPart || domain !== 's.whatsapp.net') return null;
  const cleanUser = userPart.split(':')[0].replace(/\D/g, '');
  return cleanUser ? `${cleanUser}@s.whatsapp.net` : null;
}

async function loadContactsCache() {
  try {
    const raw = await fs.readFile(CONTACTS_CACHE_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      for (const jid of parsed) {
        const normalized = normalizeUserJid(jid);
        if (normalized) knownStatusJids.add(normalized);
      }
      logInfo(`Loaded ${knownStatusJids.size} cached contact JID(s) from ${CONTACTS_CACHE_FILE}`);
    }
  } catch {
    // Cache file does not exist yet on first run
  }
}

function scheduleContactsCacheSave() {
  if (contactsCacheSaveTimer) return;
  contactsCacheSaveTimer = setTimeout(async () => {
    contactsCacheSaveTimer = null;
    try {
      await fs.mkdir(CONFIG.authDir, { recursive: true });
      await fs.writeFile(
        CONTACTS_CACHE_FILE,
        JSON.stringify(Array.from(knownStatusJids), null, 2),
        'utf8'
      );
    } catch (err) {
      logError('Failed saving contacts cache', err);
    }
  }, 2500);
}

function addStatusJid(jid) {
  const normalized = normalizeUserJid(jid);
  if (normalized && !knownStatusJids.has(normalized)) {
    knownStatusJids.add(normalized);
    scheduleContactsCacheSave();
  }
}

function collectStatusJidsFromItems(items) {
  if (!Array.isArray(items)) return;
  for (const item of items) {
    if (!item || typeof item !== 'object') continue;
    addStatusJid(item.id);
    addStatusJid(item.jid);
    if (typeof item.phoneNumber === 'string') {
      const digits = item.phoneNumber.replace(/\D/g, '');
      if (digits.length >= 7) {
        addStatusJid(`${digits}@s.whatsapp.net`);
      }
    }
  }
}

/**
 * Resolves a phone-number JID or LID to a canonical "@s.whatsapp.net" JID when possible.
 */
async function resolveToPhoneJid(sock, jid) {
  const pn = normalizeUserJid(jid);
  if (pn) return pn;

  const lid = normalizeAnyJid(jid);
  if (lid && lid.endsWith('@lid')) {
    try {
      const mapped = await sock.signalRepository?.lidMapping?.getPNForLID?.(lid);
      const normalizedMapped = normalizeUserJid(mapped);
      if (normalizedMapped) return normalizedMapped;
    } catch {
      // Ignore LID mapping lookup errors
    }
  }
  return null;
}

/**
 * Queries WhatsApp's native Status privacy list ("My contacts", "My contacts except...",
 * or "Only share with...") directly from WhatsApp servers via IQ stanza.
 */
async function fetchStatusPrivacy(sock) {
  const fallback = { type: 'contacts', list: [], source: 'fallback' };
  if (typeof sock.query !== 'function') {
    return fallback;
  }

  try {
    const resp = await withTimeout(
      sock.query({
        tag: 'iq',
        attrs: {
          to: 's.whatsapp.net',
          type: 'get',
          xmlns: 'status',
        },
        content: [{ tag: 'privacy', attrs: {} }],
      }),
      15000,
      'Fetch WhatsApp status privacy'
    );

    const topContent = Array.isArray(resp?.content) ? resp.content : [];
    const privacyNode = topContent.find((n) => n?.tag === 'privacy');
    const lists = Array.isArray(privacyNode?.content)
      ? privacyNode.content.filter((n) => n?.tag === 'list')
      : [];

    if (lists.length === 0) {
      return { type: 'contacts', list: [], source: 'whatsapp' };
    }

    // WhatsApp marks the currently active Status privacy setting with default="true"
    const activeList =
      lists.find((l) => l?.attrs?.default === 'true' || l?.attrs?.default === true) || lists[0];
    const privacyType = String(activeList?.attrs?.type || 'contacts').toLowerCase();
    const userNodes = Array.isArray(activeList?.content) ? activeList.content : [];
    const rawJids = userNodes
      .filter((n) => n?.tag === 'user' && n?.attrs?.jid)
      .map((n) => String(n.attrs.jid));

    return {
      type: privacyType,
      list: rawJids,
      source: 'whatsapp',
    };
  } catch (err) {
    logError('Could not fetch WhatsApp status privacy (using contacts + STATUS_EXCEPT)', err);
    return fallback;
  }
}

/**
 * Builds the exact statusJidList honoring WhatsApp's "My contacts except..." (blacklist),
 * "Only share with..." (whitelist), or "My contacts" setting + optional STATUS_EXCEPT env.
 */
async function resolveStatusAudience(sock, extraJids = []) {
  const privacy = await fetchStatusPrivacy(sock);
  const selfJid = normalizeUserJid(sock.user?.id);

  // Resolve privacy list entries (handles both @s.whatsapp.net and @lid entries)
  const resolvedPrivacyJids = new Set();
  for (const rawJid of privacy.list) {
    const pn = await resolveToPhoneJid(sock, rawJid);
    if (pn) resolvedPrivacyJids.add(pn);
  }

  const excludedSet = new Set(CONFIG.statusExcept);
  if (privacy.type === 'blacklist') {
    for (const jid of resolvedPrivacyJids) {
      excludedSet.add(jid);
    }
  }

  const audience = new Set();
  if (privacy.type === 'whitelist') {
    for (const jid of resolvedPrivacyJids) {
      if (!CONFIG.statusExcept.has(jid)) {
        audience.add(jid);
      }
    }
  } else {
    // "contacts" or "blacklist" ("My contacts except...")
    for (const jid of knownStatusJids) {
      if (!excludedSet.has(jid)) {
        audience.add(jid);
      }
    }
    for (const rawExtra of extraJids) {
      const pn = await resolveToPhoneJid(sock, rawExtra);
      if (pn && !excludedSet.has(pn)) {
        audience.add(pn);
      }
    }
  }

  // Always include self JID so you can see your own posted Status in WhatsApp
  if (selfJid) {
    audience.add(selfJid);
  }

  const labelMap = {
    blacklist: 'My contacts except...',
    whitelist: 'Only share with...',
    contacts: 'My contacts',
  };

  return {
    statusJidList: Array.from(audience),
    privacyType: privacy.type,
    privacyLabel: labelMap[privacy.type] || privacy.type,
    excludedCount: excludedSet.size,
  };
}

/**
 * Recursively unwraps ephemeral / documentWithCaption / edited envelopes.
 */
function unwrapMessage(rawMessage) {
  let msg = rawMessage;
  if (!msg) return null;

  for (let i = 0; i < 6 && msg; i++) {
    if (msg.ephemeralMessage?.message) {
      msg = msg.ephemeralMessage.message;
    } else if (msg.documentWithCaptionMessage?.message) {
      msg = msg.documentWithCaptionMessage.message;
    } else if (msg.editedMessage?.message?.protocolMessage?.editedMessage) {
      msg = msg.editedMessage.message.protocolMessage.editedMessage;
    } else {
      break;
    }
  }

  return msg;
}

/**
 * Checks whether a message is marked as View Once so ephemeral media is skipped.
 */
function isViewOnceMessage(rawMessage) {
  if (!rawMessage) return false;
  if (
    rawMessage.viewOnceMessage ||
    rawMessage.viewOnceMessageV2 ||
    rawMessage.viewOnceMessageV2Extension
  ) {
    return true;
  }

  const unwrapped = unwrapMessage(rawMessage);
  if (!unwrapped) return false;
  if (
    unwrapped.viewOnceMessage ||
    unwrapped.viewOnceMessageV2 ||
    unwrapped.viewOnceMessageV2Extension
  ) {
    return true;
  }

  return Boolean(
    unwrapped.imageMessage?.viewOnce ||
      unwrapped.videoMessage?.viewOnce ||
      unwrapped.audioMessage?.viewOnce
  );
}

function extractDocumentFromMessage(rawMessage) {
  const msg = unwrapMessage(rawMessage);
  return msg?.documentMessage || null;
}

/**
 * Extracts text and reply contextInfo from an incoming message (for commands like .status).
 */
function extractTextAndContext(rawMessage) {
  const msg = unwrapMessage(rawMessage);
  if (!msg) return { text: '', contextInfo: null };

  if (typeof msg.conversation === 'string') {
    return { text: msg.conversation, contextInfo: null };
  }
  if (msg.extendedTextMessage) {
    return {
      text: msg.extendedTextMessage.text || '',
      contextInfo: msg.extendedTextMessage.contextInfo || null,
    };
  }
  return { text: '', contextInfo: null };
}

/**
 * Extracts video, photo, or image/video document metadata from a message object.
 * Set allowProbeFallback=true for `.status` so extensionless/octet-stream media files
 * can still be downloaded and identified via FFprobe.
 */
function extractMediaDescriptor(rawMessage, allowProbeFallback = false) {
  const unwrapped = unwrapMessage(rawMessage);
  if (!unwrapped) return null;

  if (unwrapped.videoMessage) {
    return {
      kind: 'video',
      ext: '.mp4',
      mimetype: unwrapped.videoMessage.mimetype || 'video/mp4',
      caption: unwrapped.videoMessage.caption || '',
      fileLength: Number(unwrapped.videoMessage.fileLength || 0),
      message: { videoMessage: unwrapped.videoMessage },
    };
  }

  if (unwrapped.imageMessage) {
    const mime = (unwrapped.imageMessage.mimetype || 'image/jpeg').toLowerCase();
    const ext = mime.includes('png') ? '.png' : mime.includes('webp') ? '.webp' : '.jpg';
    return {
      kind: 'image',
      ext,
      mimetype: mime,
      caption: unwrapped.imageMessage.caption || '',
      fileLength: Number(unwrapped.imageMessage.fileLength || 0),
      message: { imageMessage: unwrapped.imageMessage },
    };
  }

  if (unwrapped.documentMessage) {
    const docMsg = unwrapped.documentMessage;
    const classification = classifyDocument(docMsg);
    if (classification) {
      return {
        kind: classification.kind,
        ext: classification.ext,
        mimetype:
          docMsg.mimetype ||
          (classification.kind === 'video' ? 'video/mp4' : 'image/jpeg'),
        caption: docMsg.caption || '',
        fileLength: Number(docMsg.fileLength || 0),
        message: { documentMessage: docMsg },
      };
    }

    // Fallback for image/video files sent with generic MIME (e.g. application/octet-stream)
    const mime = (docMsg.mimetype || '').toLowerCase();
    if (allowProbeFallback && (!mime || mime === 'application/octet-stream')) {
      const fileName = docMsg.fileName || docMsg.title || '';
      const ext = path.extname(fileName).toLowerCase() || '.bin';
      return {
        kind: 'probe',
        ext,
        mimetype: mime || 'application/octet-stream',
        caption: docMsg.caption || '',
        fileLength: Number(docMsg.fileLength || 0),
        message: { documentMessage: docMsg },
      };
    }
  }

  return null;
}

/**
 * Extracts quoted video, photo, or image/video document metadata from a reply contextInfo.
 */
function extractQuotedMedia(contextInfo) {
  const rawQuoted = contextInfo?.quotedMessage;
  return extractMediaDescriptor(rawQuoted, true);
}

/**
 * Extracts quoted regular (non-View Once) photo, video, audio, or document metadata for `.save`.
 */
function extractQuotedSavableMedia(contextInfo) {
  const rawQuoted = contextInfo?.quotedMessage;
  if (!rawQuoted) return null;

  const unwrapped = unwrapMessage(rawQuoted);
  if (!unwrapped) return null;
console.log(unwrapped)
  if (unwrapped.imageMessage) {
    const mime = (unwrapped.imageMessage.mimetype || 'image/jpeg').toLowerCase();
    const ext = mime.includes('png') ? '.png' : mime.includes('webp') ? '.webp' : '.jpg';
    return {
      kind: 'image',
      ext,
      mimetype: mime,
      caption: unwrapped.imageMessage.caption || '',
      fileLength: Number(unwrapped.imageMessage.fileLength || 0),
      message: { imageMessage: unwrapped.imageMessage },
    };
  }

  if (unwrapped.videoMessage) {
    return {
      kind: 'video',
      ext: '.mp4',
      mimetype: unwrapped.videoMessage.mimetype || 'video/mp4',
      caption: unwrapped.videoMessage.caption || '',
      fileLength: Number(unwrapped.videoMessage.fileLength || 0),
      message: { videoMessage: unwrapped.videoMessage },
    };
  }

  if (unwrapped.audioMessage) {
    const mime = (unwrapped.audioMessage.mimetype || 'audio/ogg; codecs=opus').toLowerCase();
    const ext =
      mime.includes('mpeg') || mime.includes('mp3')
        ? '.mp3'
        : mime.includes('mp4') || mime.includes('m4a') || mime.includes('aac')
          ? '.m4a'
          : '.ogg';
    return {
      kind: 'audio',
      ext,
      mimetype: unwrapped.audioMessage.mimetype || 'audio/ogg; codecs=opus',
      ptt: Boolean(unwrapped.audioMessage.ptt),
      caption: '',
      fileLength: Number(unwrapped.audioMessage.fileLength || 0),
      message: { audioMessage: unwrapped.audioMessage },
    };
  }

  if (unwrapped.documentMessage) {
    const docMsg = unwrapped.documentMessage;
    const fileName = docMsg.fileName || docMsg.title || 'saved_file';
    const ext = path.extname(fileName).toLowerCase() || '.bin';
    return {
      kind: 'document',
      ext,
      fileName,
      mimetype: docMsg.mimetype || 'application/octet-stream',
      caption: docMsg.caption || '',
      fileLength: Number(docMsg.fileLength || 0),
      message: { documentMessage: docMsg },
    };
  }

  return null;
}

/**
 * Detects whether an incoming WhatsApp document is a video or an image.
 */
function classifyDocument(docMsg) {
  const mime = (docMsg.mimetype || '').toLowerCase();
  const fileName = docMsg.fileName || docMsg.title || '';
  const ext = path.extname(fileName).toLowerCase();

  if (mime.startsWith('video/') || VIDEO_EXTS.has(ext)) {
    return { kind: 'video', ext: ext || '.mp4' };
  }
  if (mime.startsWith('image/') || IMAGE_EXTS.has(ext)) {
    return { kind: 'image', ext: ext || '.jpg' };
  }
  return null;
}

/**
 * Parses optional document caption flags ("720p", "59s", or photo duration like "12s")
 * and returns any remaining user caption text (empty string if none was provided).
 */
function parseCaptionOptions(captionText = '', kind = 'video') {
  const lower = captionText.toLowerCase();
  const profileKey = /\b(720p|59s)\b/.test(lower) ? '720p' : '1080p';

  let photoDurationSec = CONFIG.defaultPhotoDurationSec;
  if (kind === 'image') {
    const durMatch = lower.match(/\b(\d{1,2})\s*s\b/);
    if (durMatch) {
      photoDurationSec = Math.min(29, Math.max(3, Number(durMatch[1])));
    }
  }

  let cleaned = captionText.replace(/\b(720p|59s)\b/gi, '');
  if (kind === 'image') {
    cleaned = cleaned.replace(/\b\d{1,2}\s*s\b/gi, '');
  }
  const userCaption = cleaned.replace(/\s+/g, ' ').trim();

  return {
    profile: CONFIG.profiles[profileKey],
    photoDurationSec,
    userCaption,
  };
}

/**
 * Parses an FFprobe fraction string like "60000/1001" or "60/1" into a number.
 */
function parseFrameRate(rateStr) {
  if (!rateStr || typeof rateStr !== 'string') return 0;
  const parts = rateStr.split('/');
  if (parts.length === 2) {
    const num = Number(parts[0]);
    const den = Number(parts[1]);
    if (Number.isFinite(num) && Number.isFinite(den) && den > 0) {
      return num / den;
    }
  }
  const single = Number(rateStr);
  return Number.isFinite(single) && single > 0 ? single : 0;
}

/**
 * Probes media metadata (duration, codecs, resolution, FPS, still-image detection, audio).
 */
async function probeMedia(filePath) {
  const { stdout } = await runCommand('ffprobe', [
    '-v',
    'error',
    '-print_format',
    'json',
    '-show_format',
    '-show_streams',
    filePath,
  ]);

  const info = JSON.parse(stdout);
  const streams = Array.isArray(info.streams) ? info.streams : [];
  const videoStream = streams.find((s) => s.codec_type === 'video');
  const audioStream = streams.find((s) => s.codec_type === 'audio');

  const duration = Number(info.format?.duration || videoStream?.duration || 0);
  const fps =
    parseFrameRate(videoStream?.avg_frame_rate) ||
    parseFrameRate(videoStream?.r_frame_rate) ||
    0;
  const formatName = (info.format?.format_name || '').toLowerCase();
  const videoCodec = (videoStream?.codec_name || '').toLowerCase();
  const nbFrames = Number(videoStream?.nb_frames || 0);

  const isStillImageFormat =
    /(?:image2|_pipe|webp|heif|heic|avif|bmp|tiff|png|jpeg|mjpeg)/.test(formatName) ||
    ['mjpeg', 'png', 'webp', 'bmp', 'tiff'].includes(videoCodec);
  const isStillImage =
    Boolean(videoStream) &&
    !audioStream &&
    (isStillImageFormat || (nbFrames === 1 && duration <= 0.1));

  return {
    duration: Number.isFinite(duration) && duration > 0 ? duration : 0,
    hasVideo: Boolean(videoStream),
    hasAudio: Boolean(audioStream),
    isStillImage,
    videoCodec,
    audioCodec: (audioStream?.codec_name || '').toLowerCase(),
    width: Number(videoStream?.width || 0),
    height: Number(videoStream?.height || 0),
    fps: Number.isFinite(fps) ? Number(fps.toFixed(2)) : 0,
    formatName,
  };
}

/**
 * Checks whether a 1-on-1 DM sender is permitted to trigger the bot.
 * Supports standard JIDs (@s.whatsapp.net) and Baileys v7 LID alt JIDs.
 */
function isAllowedSender(msgKey) {
  const remoteJid = msgKey?.remoteJid;
  if (!remoteJid) return false;

  // Ignore groups, channels, and status broadcasts
  if (
    remoteJid.endsWith('@g.us') ||
    remoteJid.endsWith('@newsletter') ||
    remoteJid === 'status@broadcast'
  ) {
    return false;
  }

  if (CONFIG.allowedNumbers.size === 0) {
    return true;
  }

  const candidateJids = [remoteJid, msgKey.remoteJidAlt, msgKey.senderPn].filter(Boolean);
  return candidateJids.some((jid) => {
    const digits = String(jid).split('@')[0].split(':')[0].replace(/\D/g, '');
    return digits && CONFIG.allowedNumbers.has(digits);
  });
}

// ----------------------------------------------------------------------------
// FFmpeg / FFprobe Execution Helpers
// ----------------------------------------------------------------------------

function runCommand(binary, args, timeoutMs = CONFIG.ffmpegTimeoutMs) {
  return new Promise((resolve, reject) => {
    const child = spawn(binary, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    let settled = false;

    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      try {
        child.kill('SIGTERM');
        setTimeout(() => child.kill('SIGKILL'), 3000).unref();
      } catch {
        // Ignore kill error
      }
      reject(new Error(`${binary} timed out after ${Math.round(timeoutMs / 1000)}s`));
    }, timeoutMs);

    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    child.on('error', (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(new Error(`Failed to start ${binary}: ${err.message}`));
    });

    child.on('close', (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (code === 0) {
        resolve({ stdout, stderr });
      } else {
        const tail = stderr.trim().split('\n').slice(-6).join('\n');
        reject(new Error(`${binary} exited with code ${code}:\n${tail}`));
      }
    });
  });
}

/**
 * Downloads media safely with strict timeouts on both download and Baileys reuploadRequest
 * so an unresponsive phone/CDN can never freeze the job queue forever.
 */
async function downloadMediaToFile(sock, downloadMediaMessage, targetMsg, outputPath) {
  const safeReuploadRequest = (m) =>
    withTimeout(
      sock.updateMediaMessage(m),
      CONFIG.reuploadTimeoutMs,
      'Media re-upload request'
    );

  const mediaStream = await withTimeout(
    downloadMediaMessage(
      targetMsg,
      'stream',
      {},
      {
        logger,
        reuploadRequest: safeReuploadRequest,
      }
    ),
    CONFIG.downloadTimeoutMs,
    'Start media download'
  );

  const writeStream = createWriteStream(outputPath);
  try {
    await withTimeout(
      pipeline(mediaStream, writeStream),
      CONFIG.downloadTimeoutMs,
      'Media download stream'
    );
  } catch (err) {
    mediaStream.destroy();
    writeStream.destroy();
    throw err;
  }
}

async function verifyFfmpegInstalled() {
  try {
    await runCommand('ffmpeg', ['-version']);
    await runCommand('ffprobe', ['-version']);
  } catch (err) {
    throw new Error(
      `FFmpeg/FFprobe not found on PATH. Install it on your VPS first (e.g. "sudo apt install -y ffmpeg"). Details: ${err.message}`
    );
  }
}

/**
 * Builds an orientation-aware scale filter that fits portrait media into
 * maxShort x maxLong (e.g. 1080x1920) and landscape into maxLong x maxShort
 * without upscaling and with even dimensions required by yuv420p/x264.
 */
function buildScaleFilter(profile) {
  const s = profile.maxShortEdge;
  const l = profile.maxLongEdge;
  return (
    `scale='if(gte(ih,iw),min(${s},iw),min(${l},iw))':'if(gte(ih,iw),min(${l},ih),min(${s},ih))'` +
    `:force_original_aspect_ratio=decrease,scale=trunc(iw/2)*2:trunc(ih/2)*2,setsar=1`
  );
}

/**
 * Encodes a single video segment using the WAstatus Status-safe H.264/30fps profile
 * and strictly enforces the < 15.4 MB file size cap with an automatic fallback pass.
 */
async function encodeVideoSegment({
  inputPath,
  outputPath,
  startSec,
  durationSec,
  hasAudio,
  profile,
}) {
  const scaleFilter = buildScaleFilter(profile);
  const audioBitrateKbps = 128;

  // Dynamic VBV cap so each clip stays safely below WhatsApp's ~16 MB Status limit
  const maxTotalBitrateKbps = Math.floor(
    (CONFIG.targetOutputMb * 1024 * 8) / Math.max(durationSec, 1)
  );
  const targetMaxVideoKbps = Math.min(
    profile.maxVideoBitrateKbps,
    Math.max(800, maxTotalBitrateKbps - audioBitrateKbps - 150)
  );

  const buildArgs = (videoRateArgs) => {
    const args = ['-y', '-ss', startSec.toFixed(3), '-t', durationSec.toFixed(3), '-i', inputPath];

    // Attach silent AAC if source has no audio track for consistent Status playback
    if (!hasAudio) {
      args.push(
        '-f',
        'lavfi',
        '-t',
        durationSec.toFixed(3),
        '-i',
        'anullsrc=channel_layout=stereo:sample_rate=44100'
      );
    }

    args.push(
      '-map',
      '0:v:0',
      '-map',
      hasAudio ? '0:a:0' : '1:a:0',
      '-vf',
      scaleFilter,
      '-r',
      String(profile.fps),
      '-c:v',
      'libx264',
      '-preset',
      CONFIG.ffmpegPreset,
      '-profile:v',
      'high',
      '-level',
      '4.1',
      '-pix_fmt',
      'yuv420p',
      ...videoRateArgs,
      '-c:a',
      'aac',
      '-b:a',
      `${audioBitrateKbps}k`,
      '-ar',
      '44100',
      '-ac',
      '2',
      '-movflags',
      '+faststart'
    );

    if (!hasAudio) {
      args.push('-shortest');
    }

    args.push(outputPath);
    return args;
  };

  // Pass 1: CRF quality mode constrained by VBV maxrate
  await runCommand(
    'ffmpeg',
    buildArgs([
      '-crf',
      String(profile.crf),
      '-maxrate',
      `${targetMaxVideoKbps}k`,
      '-bufsize',
      `${targetMaxVideoKbps * 2}k`,
    ])
  );

  // Verify final size is strictly under WhatsApp's threshold; tighten bitrate if needed
  const stat = await fs.stat(outputPath);
  if (stat.size > CONFIG.maxOutputBytes) {
    const strictVideoKbps = Math.max(
      600,
      Math.floor(targetMaxVideoKbps * (CONFIG.maxOutputBytes / stat.size) * 0.9)
    );
    logInfo(
      `Clip exceeded ${(CONFIG.maxOutputBytes / 1024 / 1024).toFixed(1)} MB; re-encoding at ${strictVideoKbps} kbps`
    );
    await runCommand(
      'ffmpeg',
      buildArgs([
        '-b:v',
        `${strictVideoKbps}k`,
        '-maxrate',
        `${strictVideoKbps}k`,
        '-bufsize',
        `${strictVideoKbps * 2}k`,
      ])
    );
  }
}

/**
 * Converts a full-resolution photo file into a crisp HD Status-ready MP4 video
 * with silent AAC audio so WhatsApp preserves sharpness.
 */
async function encodePhotoToStatusVideo({ inputPath, outputPath, durationSec, profile }) {
  const scaleFilter = buildScaleFilter(profile);

  const args = [
    '-y',
    '-loop',
    '1',
    '-framerate',
    String(profile.fps),
    '-t',
    String(durationSec),
    '-i',
    inputPath,
    '-f',
    'lavfi',
    '-t',
    String(durationSec),
    '-i',
    'anullsrc=channel_layout=stereo:sample_rate=44100',
    '-map',
    '0:v:0',
    '-map',
    '1:a:0',
    '-vf',
    scaleFilter,
    '-r',
    String(profile.fps),
    '-c:v',
    'libx264',
    '-preset',
    CONFIG.ffmpegPreset,
    '-tune',
    'stillimage',
    '-crf',
    '18',
    '-maxrate',
    `${profile.maxVideoBitrateKbps}k`,
    '-bufsize',
    `${profile.maxVideoBitrateKbps * 2}k`,
    '-profile:v',
    'high',
    '-level',
    '4.1',
    '-pix_fmt',
    'yuv420p',
    '-c:a',
    'aac',
    '-b:a',
    '128k',
    '-ar',
    '44100',
    '-ac',
    '2',
    '-shortest',
    '-movflags',
    '+faststart',
    outputPath,
  ];

  await runCommand('ffmpeg', args);
}

/**
 * Prepares quoted/direct media (including image/video files) for status@broadcast
 * at FULL original quality and native FPS (e.g., 60 fps untouched, zero quality drop).
 */
async function prepareFullQualityStatusMedia({ inputPath, workDir, kind, ext, mimetype }) {
  const normalizedExt = (ext || '').toLowerCase();
  const meta = await probeMedia(inputPath);

  if (!meta.hasVideo) {
    throw new Error('The file is not a valid photo or video.');
  }

  const resolvedKind = kind === 'probe' ? (meta.isStillImage ? 'image' : 'video') : kind;
  const resLabel = meta.width && meta.height ? `${meta.width}x${meta.height}` : 'native';

  if (resolvedKind === 'image') {
    const isJpeg =
      meta.videoCodec === 'mjpeg' ||
      normalizedExt === '.jpg' ||
      normalizedExt === '.jpeg' ||
      mimetype === 'image/jpeg';
    const isPng =
      meta.videoCodec === 'png' ||
      normalizedExt === '.png' ||
      mimetype === 'image/png';

    if (isJpeg || isPng) {
      return {
        kind: 'image',
        finalPath: inputPath,
        mimetype: isPng ? 'image/png' : 'image/jpeg',
        summary: `${resLabel} original image (untouched)`,
      };
    }

    // Convert non-native image files (HEIC/WebP/BMP/TIFF/AVIF) at full native resolution
    const outPath = path.join(workDir, 'status_full_quality.jpg');
    await runCommand('ffmpeg', ['-y', '-i', inputPath, '-q:v', '1', outPath]);
    return {
      kind: 'image',
      finalPath: outPath,
      mimetype: 'image/jpeg',
      summary: `${resLabel} full-resolution JPEG`,
    };
  }

  // Video handling: preserve exact original quality, resolution, and native FPS (e.g. 60 fps)
  const fpsLabel = meta.fps ? `${meta.fps} fps` : 'original fps';
  const isMp4Container = normalizedExt === '.mp4' || meta.formatName.includes('mp4');
  const audioOk = !meta.hasAudio || meta.audioCodec === 'aac';

  // Case 1: Already H.264 + AAC inside MP4 -> upload original bytes untouched (zero loss)
  if (meta.videoCodec === 'h264' && audioOk && isMp4Container) {
    return {
      kind: 'video',
      finalPath: inputPath,
      mimetype: 'video/mp4',
      summary: `${resLabel} @ ${fpsLabel} (100% original untouched)`,
    };
  }

  const outPath = path.join(workDir, 'status_full_quality.mp4');

  // Case 2: Already H.264 in MOV/MKV or non-AAC audio -> lossless video stream copy
  if (meta.videoCodec === 'h264') {
    const args = ['-y', '-i', inputPath, '-map', '0:v:0'];
    if (meta.hasAudio) {
      args.push('-map', '0:a:0');
    }
    args.push('-c:v', 'copy');
    if (meta.hasAudio) {
      if (meta.audioCodec === 'aac') {
        args.push('-c:a', 'copy');
      } else {
        args.push('-c:a', 'aac', '-b:a', '192k');
      }
    }
    args.push('-movflags', '+faststart', outPath);

    await runCommand('ffmpeg', args);
    return {
      kind: 'video',
      finalPath: outPath,
      mimetype: 'video/mp4',
      summary: `${resLabel} @ ${fpsLabel} (lossless remux)`,
    };
  }

  // Case 3: Non-H.264 codec (HEVC/VP9/AV1) -> re-encode at native resolution & native FPS (no 30fps cap)
  const args = ['-y', '-i', inputPath, '-map', '0:v:0'];
  if (meta.hasAudio) {
    args.push('-map', '0:a:0');
  }
  args.push(
    '-c:v',
    'libx264',
    '-preset',
    CONFIG.ffmpegPreset,
    '-crf',
    '16',
    '-profile:v',
    'high',
    '-pix_fmt',
    'yuv420p'
  );
  if (meta.hasAudio) {
    args.push('-c:a', 'aac', '-b:a', '192k');
  }
  args.push('-movflags', '+faststart', outPath);

  await runCommand('ffmpeg', args);
  return {
    kind: 'video',
    finalPath: outPath,
    mimetype: 'video/mp4',
    summary: `${resLabel} @ ${fpsLabel} (full-quality H.264)`,
  };
}

// ----------------------------------------------------------------------------
// Core Media Job Processor
// ----------------------------------------------------------------------------

async function processStatusCommandJob(
  sock,
  downloadMediaMessage,
  msg,
  contextInfo,
  targetMedia,
  cmdCaption,
  isDirectMedia = false
) {
  const sourceChatJid = msg.key.remoteJid;
  // Always send bot confirmations to your own self-chat so the bot never messages other chats
  const notifyJid = isSelfChat(sock, msg.key)
    ? sourceChatJid
    : getSelfChatJid(sock) || sourceChatJid;
  const replyOpts = notifyJid === sourceChatJid ? { quoted: msg } : {};

  if (targetMedia.fileLength > CONFIG.maxInputBytes) {
    await safeSendMessage(
      sock,
      notifyJid,
      {
        text: `File is too large (${(targetMedia.fileLength / 1024 / 1024).toFixed(1)} MB). Max allowed is ${(
          CONFIG.maxInputBytes /
          1024 /
          1024
        ).toFixed(0)} MB.`,
      },
      replyOpts
    );
    return;
  }

  const workDir = path.join(os.tmpdir(), `wa-status-cmd-${crypto.randomUUID()}`);
  await fs.mkdir(workDir, { recursive: true });
  const inputPath = path.join(workDir, `status_source${targetMedia.ext}`);

  try {
    logInfo(`[.status] Step 1/4: Resolving WhatsApp Status privacy & audience for ${sourceChatJid}...`);
    const audience = await resolveStatusAudience(sock, [
      msg.key.remoteJid,
      msg.key.remoteJidAlt,
      msg.key.participant,
      msg.key.participantAlt,
    ]);

    if (audience.statusJidList.length === 0) {
      await safeSendMessage(
        sock,
        notifyJid,
        {
          text: 'Cannot publish Status yet: no contact JIDs are available. Set STATUS_CONTACTS or wait for WhatsApp contacts to sync.',
        },
        replyOpts
      );
      return;
    }

    // Build the exact WAMessage structure Baileys needs to stream-download the media/file
    const botSelfIds = new Set(
      [normalizeAnyJid(sock.user?.id), normalizeAnyJid(sock.user?.lid)].filter(Boolean)
    );
    const quotedSender = normalizeAnyJid(contextInfo?.participant);
    const normalizedChatJid = normalizeAnyJid(sourceChatJid);
    const quotedFromMe = quotedSender
      ? botSelfIds.has(quotedSender)
      : botSelfIds.has(normalizedChatJid);

    const downloadTargetMsg = isDirectMedia
      ? {
          key: msg.key,
          message: targetMedia.message,
        }
      : {
          key: {
            remoteJid: sourceChatJid,
            id: contextInfo.stanzaId,
            fromMe: quotedFromMe,
            participant: contextInfo.participant,
          },
          message: targetMedia.message,
        };

    logInfo(`[.status] Step 2/4: Downloading ${targetMedia.kind} media...`);
    await downloadMediaToFile(sock, downloadMediaMessage, downloadTargetMsg, inputPath);

    logInfo(`[.status] Step 3/4: Probing & preparing full-quality ${targetMedia.kind}...`);
    const prepared = await prepareFullQualityStatusMedia({
      inputPath,
      workDir,
      kind: targetMedia.kind,
      ext: targetMedia.ext,
      mimetype: targetMedia.mimetype,
    });

    // Only attach a caption if the user actually provided one
    const inheritedCaption = isDirectMedia ? '' : targetMedia.caption || '';
    const finalCaption = (cmdCaption || inheritedCaption).trim();
    const statusPayload =
      prepared.kind === 'video'
        ? { video: { url: prepared.finalPath }, mimetype: prepared.mimetype }
        : { image: { url: prepared.finalPath }, mimetype: prepared.mimetype };

    if (finalCaption) {
      statusPayload.caption = finalCaption;
    }

    logInfo(
      `[.status] Step 4/4: Uploading to status@broadcast (${audience.privacyLabel}, ${audience.statusJidList.length} viewer(s), ${audience.excludedCount} excluded)...`
    );
    await safeSendMessage(sock, 'status@broadcast', statusPayload, {
      broadcast: true,
      statusJidList: audience.statusJidList,
    });

    const stat = await fs.stat(prepared.finalPath);
    const sizeMb = (stat.size / 1024 / 1024).toFixed(2);
    logInfo(
      `Published ${prepared.kind} to status@broadcast (${prepared.summary}, ${sizeMb} MB, ${audience.privacyLabel}, ${audience.statusJidList.length} viewer(s), ${audience.excludedCount} excluded)`
    );

    const exceptNote =
      audience.excludedCount > 0 ? `, ${audience.excludedCount} excluded` : '';
    await safeSendMessage(
      sock,
      notifyJid,
      {
        text: `Posted to bot Status in full quality (${prepared.summary}, ${sizeMb} MB) • Privacy: ${audience.privacyLabel} (${audience.statusJidList.length} viewer(s)${exceptNote}).`,
      },
      replyOpts
    );
  } catch (err) {
    logError(`Failed .status command from ${sourceChatJid}`, err);
    try {
      await safeSendMessage(
        sock,
        notifyJid,
        { text: `Failed to post Status: ${err.message}` },
        replyOpts
      );
    } catch {
      // Ignore secondary send error if socket disconnected mid-job
    }
  } finally {
    await fs.rm(workDir, { recursive: true, force: true }).catch(() => {});
  }
}

/**
 * Copies a quoted regular (non-View Once) photo, video, audio, or document privately
 * to your own "Message yourself" inbox without posting anything in the original chat.
 */
async function processSaveMediaJob(
  sock,
  downloadMediaMessage,
  msg,
  contextInfo,
  targetMedia,
  cmdCaption
) {
  const sourceChatJid = msg.key.remoteJid;
  const selfChatJid = getSelfChatJid(sock);
  if (!selfChatJid) {
    logError('Cannot run .save: unable to resolve self-chat JID.');
    return;
  }

  if (targetMedia.fileLength > CONFIG.maxInputBytes) {
    await safeSendMessage(sock, selfChatJid, {
      text: `Cannot save file: too large (${(targetMedia.fileLength / 1024 / 1024).toFixed(1)} MB). Max allowed is ${(
        CONFIG.maxInputBytes /
        1024 /
        1024
      ).toFixed(0)} MB.`,
    });
    return;
  }

  const workDir = path.join(os.tmpdir(), `wa-save-${crypto.randomUUID()}`);
  await fs.mkdir(workDir, { recursive: true });
  const inputPath = path.join(workDir, `saved${targetMedia.ext}`);

  try {
    const botSelfIds = new Set(
      [normalizeAnyJid(sock.user?.id), normalizeAnyJid(sock.user?.lid)].filter(Boolean)
    );
    const quotedSender = normalizeAnyJid(contextInfo?.participant);
    const normalizedChatJid = normalizeAnyJid(sourceChatJid);
    const quotedFromMe = quotedSender
      ? botSelfIds.has(quotedSender)
      : botSelfIds.has(normalizedChatJid);

    const downloadTargetMsg = {
      key: {
        remoteJid: sourceChatJid,
        id: contextInfo.stanzaId,
        fromMe: quotedFromMe,
        participant: contextInfo.participant,
      },
      message: targetMedia.message,
    };

    logInfo(`[.save] Downloading ${targetMedia.kind} from ${sourceChatJid} to self inbox...`);
    await downloadMediaToFile(sock, downloadMediaMessage, downloadTargetMsg, inputPath);

    const finalCaption = (cmdCaption || targetMedia.caption || '').trim();
    let payload;

    if (targetMedia.kind === 'image') {
      payload = { image: { url: inputPath }, mimetype: targetMedia.mimetype };
      if (finalCaption) payload.caption = finalCaption;
    } else if (targetMedia.kind === 'video') {
      payload = { video: { url: inputPath }, mimetype: targetMedia.mimetype };
      if (finalCaption) payload.caption = finalCaption;
    } else if (targetMedia.kind === 'audio') {
      payload = {
        audio: { url: inputPath },
        mimetype: targetMedia.mimetype,
        ptt: Boolean(targetMedia.ptt),
      };
    } else {
      payload = {
        document: { url: inputPath },
        mimetype: targetMedia.mimetype,
        fileName: targetMedia.fileName,
      };
      if (finalCaption) payload.caption = finalCaption;
    }

    await safeSendMessage(sock, selfChatJid, payload);
    logInfo(`[.save] Saved ${targetMedia.kind} to self inbox (${selfChatJid}).`);
  } catch (err) {
    logError(`Failed .save command from ${sourceChatJid}`, err);
    try {
      await safeSendMessage(sock, selfChatJid, {
        text: `Failed to save media: ${err.message}`,
      });
    } catch {
      // Ignore secondary send error if socket disconnected mid-job
    }
  } finally {
    await fs.rm(workDir, { recursive: true, force: true }).catch(() => {});
  }
}

async function processDocumentJob(sock, downloadMediaMessage, msg, docMsg, classification) {
  const jid = msg.key.remoteJid;
  const caption = docMsg.caption || '';
  const { profile, photoDurationSec, userCaption } = parseCaptionOptions(
    caption,
    classification.kind
  );

  const fileSize = Number(docMsg.fileLength || 0);
  if (fileSize > CONFIG.maxInputBytes) {
    await safeSendMessage(
      sock,
      jid,
      {
        text: `File is too large (${(fileSize / 1024 / 1024).toFixed(1)} MB). Max allowed is ${(
          CONFIG.maxInputBytes /
          1024 /
          1024
        ).toFixed(0)} MB.`,
      },
      { quoted: msg }
    );
    return;
  }

  // Create an isolated temporary working folder for this job
  const workDir = path.join(os.tmpdir(), `wa-hd-${crypto.randomUUID()}`);
  await fs.mkdir(workDir, { recursive: true });

  const inputPath = path.join(workDir, `input${classification.ext}`);
  const outputClips = [];

  try {
    await safeSendMessage(
      sock,
      jid,
      {
        text:
          classification.kind === 'image'
            ? `Received photo file. Creating a ${photoDurationSec}s ${profile.name} Status video...`
            : `Received video file. Preparing ${profile.name} Status clip(s) (30 fps, <16 MB)...`,
      },
      { quoted: msg }
    );

    // Stream incoming document directly to disk (keeps VPS RAM usage low)
    logInfo(`[document] Downloading "${docMsg.fileName || 'unnamed'}" from ${jid}...`);
    await downloadMediaToFile(sock, downloadMediaMessage, msg, inputPath);

    if (classification.kind === 'image') {
      const outPath = path.join(workDir, 'status_photo_hd.mp4');
      await encodePhotoToStatusVideo({
        inputPath,
        outputPath: outPath,
        durationSec: photoDurationSec,
        profile,
      });
      outputClips.push({ path: outPath, part: 1, total: 1 });
    } else {
      const meta = await probeMedia(inputPath);
      if (!meta.hasVideo || meta.duration <= 0) {
        throw new Error('Could not read a valid video stream from this file.');
      }

      // Auto-split into Status-ready parts (29s for 1080p, 59s for 720p)
      const segments = [];
      let start = 0;
      while (start < meta.duration) {
        const remaining = meta.duration - start;
        if (segments.length > 0 && remaining < 0.8) {
          break;
        }
        const segDur = Math.min(profile.partDurationSec, remaining);
        segments.push({ start, duration: segDur });
        start += segDur;
      }

      for (let i = 0; i < segments.length; i++) {
        const seg = segments[i];
        const outPath = path.join(workDir, `status_part_${i + 1}.mp4`);
        logInfo(
          `Encoding part ${i + 1}/${segments.length} (${seg.duration.toFixed(1)}s, ${profile.name}) for ${jid}`
        );

        await encodeVideoSegment({
          inputPath,
          outputPath: outPath,
          startSec: seg.start,
          durationSec: seg.duration,
          hasAudio: meta.hasAudio,
          profile,
        });

        outputClips.push({ path: outPath, part: i + 1, total: segments.length });
      }
    }

    // Deliver finished clip(s) directly back to chat via Baileys
    for (const clip of outputClips) {
      const videoPayload = {
        video: { url: clip.path },
        mimetype: 'video/mp4',
      };

      // Only add a caption if the incoming file actually had one
      if (userCaption) {
        videoPayload.caption = userCaption;
      }

      await safeSendMessage(sock, jid, videoPayload);
    }

    // Send concise WAstatus full-HD posting steps
    await safeSendMessage(sock, jid, {
      text: [
        'Done! To post on Status with ZERO quality drop (WAstatus method):',
        '1. Open the video above and tap Save / Download.',
        '2. Open "Message yourself" -> tap 📎 Gallery -> select the saved clip -> tap HD (do NOT trim or edit).',
        '3. Forward that newly sent copy to My Status.',
      ].join('\n'),
    });
  } catch (err) {
    logError(`Failed processing job for ${jid}`, err);
    try {
      await safeSendMessage(
        sock,
        jid,
        { text: `Failed to process your file: ${err.message}` },
        { quoted: msg }
      );
    } catch {
      // Ignore secondary send error if socket disconnected mid-job
    }
  } finally {
    // Always remove temporary files from VPS disk
    await fs.rm(workDir, { recursive: true, force: true }).catch(() => {});
  }
}

function enqueueJob(jobFn) {
  jobQueue.push(jobFn);
  if (!isProcessingQueue) {
    drainQueue();
  }
}

async function drainQueue() {
  if (isProcessingQueue) return;
  isProcessingQueue = true;

  while (jobQueue.length > 0) {
    const nextJob = jobQueue.shift();
    try {
      await nextJob();
    } catch (err) {
      logError('Unhandled error inside job queue', err);
    }
  }

  isProcessingQueue = false;
}

// ----------------------------------------------------------------------------
// WhatsApp Socket Initialization (QR Code Scan)
// ----------------------------------------------------------------------------

async function startBot() {
  await verifyFfmpegInstalled();
  await fs.mkdir(CONFIG.authDir, { recursive: true });
  await loadContactsCache();

  const {
    makeWASocket,
    DisconnectReason,
    downloadMediaMessage,
    fetchLatestBaileysVersion,
    useMultiFileAuthState,
  } = await loadBaileys();

  const { state, saveCreds } = await useMultiFileAuthState(CONFIG.authDir);
  const { version } = await fetchLatestBaileysVersion();

  logInfo(`Starting WhatsApp bot (WA Web v${version.join('.')})...`);

  const sock = makeWASocket({
    version,
    auth: state,
    logger,
    browser: ['Ubuntu', 'Chrome', '128.0.0'],
    markOnlineOnConnect: false,
    syncFullHistory: false,
  });

  activeSocket = sock;

  // Save updated session credentials automatically
  sock.ev.on('creds.update', saveCreds);

  // Collect contact JIDs in memory (no DB) so status@broadcast reaches your contacts
  sock.ev.on('messaging-history.set', ({ contacts = [], chats = [] }) => {
    collectStatusJidsFromItems(contacts);
    collectStatusJidsFromItems(chats);
  });
  sock.ev.on('contacts.upsert', (contacts) => collectStatusJidsFromItems(contacts));
  sock.ev.on('contacts.update', (contacts) => collectStatusJidsFromItems(contacts));
  sock.ev.on('chats.upsert', (chats) => collectStatusJidsFromItems(chats));

  sock.ev.on('connection.update', (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      console.log('\n==============================================================');
      console.log('Scan this QR code in WhatsApp -> Linked Devices -> Link a Device');
      console.log('==============================================================\n');
      qrcode.generate(qr, { small: true });
    }

    if (connection === 'open') {
      reconnectAttempts = 0;
      addStatusJid(sock.user?.id);
      logInfo(
        'Connected to WhatsApp (SELF-ONLY mode)! Send .menu in "Message yourself" to see all commands.'
      );
    }

    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      const shouldReconnect = statusCode !== DisconnectReason.loggedOut && !isShuttingDown;

      logInfo(`Connection closed (code: ${statusCode ?? 'unknown'}). Reconnect: ${shouldReconnect}`);

      if (shouldReconnect) {
        // Exponential backoff (4s -> 8s -> 16s -> ... max 60s) prevents rapid reconnect loops
        const delayMs = Math.min(60000, 4000 * Math.pow(2, reconnectAttempts));
        reconnectAttempts += 1;
        logInfo(`Reconnecting in ${(delayMs / 1000).toFixed(0)}s (attempt ${reconnectAttempts})...`);
        setTimeout(() => {
          startBot().catch((err) => logError('Reconnect failed', err));
        }, delayMs);
      } else if (statusCode === DisconnectReason.loggedOut) {
        logInfo(
          `Logged out. Delete "${CONFIG.authDir}" and restart to scan a new QR code.`
        );
      }
    }
  });

  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    if (type !== 'notify') return;

    for (const msg of messages) {
      if (!msg?.message) continue;
      if (botSentMessageIds.has(msg.key.id)) continue;

      // Passively remember DM contacts in memory for status@broadcast delivery
      addStatusJid(msg.key.remoteJid);
      addStatusJid(msg.key.remoteJidAlt);

      // STRICT SELF-ONLY MODE: Never respond to or process messages from anyone else
      if (!msg.key.fromMe) continue;

      const inSelfChat = isSelfChat(sock, msg.key);
      const replyTargetJid = inSelfChat
        ? msg.key.remoteJid
        : getSelfChatJid(sock) || msg.key.remoteJid;
      const replyOpts = replyTargetJid === msg.key.remoteJid ? { quoted: msg } : {};

      // 1) Handle `.status` put directly as the caption of a photo, video, or image/video file
      const directMedia = extractMediaDescriptor(msg.message, true);
      const directStatusMatch = directMedia?.caption
        ? directMedia.caption.trim().match(/^\.status(?:\s+([\s\S]*))?$/i)
        : null;
      if (directStatusMatch) {
        const cmdCaption = (directStatusMatch[1] || '').trim();
        logInfo(`Queued direct .status (${directMedia.kind}) from ${msg.key.remoteJid}`);
        enqueueJob(() =>
          processStatusCommandJob(
            sock,
            downloadMediaMessage,
            msg,
            null,
            directMedia,
            cmdCaption,
            true
          )
        );
        continue;
      }

      // 2) Handle text commands (`.menu`, `.ping`, `.runtime`, `.statusinfo`, `.status`)
      const { text, contextInfo } = extractTextAndContext(msg.message);
      const trimmedText = text.trim();
      const cmdLower = trimmedText.toLowerCase();

      if (cmdLower === '.menu' || cmdLower === '.help') {
        await safeSendMessage(sock, replyTargetJid, { text: buildMenuText() }, replyOpts);
        continue;
      }

      if (cmdLower === '.ping') {
        const pingResult = await measureWhatsAppPing(sock);
        await safeSendMessage(
          sock,
          replyTargetJid,
          { text: `Pong!\n${pingResult.type}: ${pingResult.ms} ms` },
          replyOpts
        );
        continue;
      }

      if (cmdLower === '.runtime') {
        const uptimeStr = formatRuntime(Date.now() - botStartedAt);
        const memMb = (process.memoryUsage().rss / 1024 / 1024).toFixed(1);
        await safeSendMessage(
          sock,
          replyTargetJid,
          {
            text: [
              '*Bot Runtime*',
              `Uptime: ${uptimeStr}`,
              `RAM Usage: ${memMb} MB`,
              `Queued Jobs: ${jobQueue.length}`,
              'Mode: Self-Only',
            ].join('\n'),
          },
          replyOpts
        );
        continue;
      }

      if (cmdLower === '.statusinfo') {
        const audience = await resolveStatusAudience(sock);
        await safeSendMessage(
          sock,
          replyTargetJid,
          {
            text: [
              `Status Privacy: ${audience.privacyLabel}`,
              `Synced contacts cached: ${knownStatusJids.size}`,
              `Excluded contacts: ${audience.excludedCount}`,
              `Viewers who will receive .status: ${audience.statusJidList.length}`,
            ].join('\n'),
          },
          replyOpts
        );
        continue;
      }

      const saveCmdMatch = trimmedText.match(/^\.save(?:\s+([\s\S]*))?$/i);
      if (saveCmdMatch) {
        const quotedSavable = extractQuotedSavableMedia(contextInfo);
        if (quotedSavable?.blockedViewOnce) {
          await safeSendMessage(
            sock,
            replyTargetJid,
            {
              text: 'View Once media is skipped and cannot be saved. `.save` works on regular photos, videos, audio, and files.',
            },
            replyOpts
          );
          continue;
        }

        if (!quotedSavable) {
          await safeSendMessage(
            sock,
            replyTargetJid,
            {
              text: 'Reply to (quote) a regular photo, video, audio, or file with `.save` to copy it privately to your inbox.',
            },
            replyOpts
          );
          continue;
        }

        const cmdCaption = (saveCmdMatch[1] || '').trim();
        logInfo(`Queued .save (${quotedSavable.kind}) from ${msg.key.remoteJid}`);
        enqueueJob(() =>
          processSaveMediaJob(
            sock,
            downloadMediaMessage,
            msg,
            contextInfo,
            quotedSavable,
            cmdCaption
          )
        );
        continue;
      }

      const statusCmdMatch = trimmedText.match(/^\.status(?:\s+([\s\S]*))?$/i);
      if (statusCmdMatch) {
        const quotedMedia = extractQuotedMedia(contextInfo);
        if (!quotedMedia) {
          await safeSendMessage(
            sock,
            replyTargetJid,
            {
              text: 'Reply to (quote) a video, photo, or image/video file with `.status` to post it to Status in full original quality.',
            },
            replyOpts
          );
          continue;
        }

        const cmdCaption = (statusCmdMatch[1] || '').trim();
        logInfo(`Queued quoted .status (${quotedMedia.kind}) from ${msg.key.remoteJid}`);
        enqueueJob(() =>
          processStatusCommandJob(
            sock,
            downloadMediaMessage,
            msg,
            contextInfo,
            quotedMedia,
            cmdCaption,
            false
          )
        );
        continue;
      }

      // 3) Auto-convert incoming Document/File uploads ONLY inside your "Message yourself" chat
      if (!inSelfChat) continue;

      const docMsg = extractDocumentFromMessage(msg.message);
      if (!docMsg) continue;

      const classification = classifyDocument(docMsg);
      if (!classification) continue;

      logInfo(
        `Queued ${classification.kind} document "${docMsg.fileName || 'unnamed'}" from ${msg.key.remoteJid}`
      );

      enqueueJob(() =>
        processDocumentJob(sock, downloadMediaMessage, msg, docMsg, classification)
      );
    }
  });
}

// Graceful shutdown for PM2 / systemd on VPS
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => {
    isShuttingDown = true;
    logInfo(`Received ${signal}, shutting down...`);
    try {
      activeSocket?.end(undefined);
    } catch {
      // Ignore close errors during shutdown
    }
    process.exit(0);
  });
}

startBot().catch((err) => {
  logError('Fatal startup error', err);
  process.exit(1);
});
