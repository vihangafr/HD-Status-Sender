/**
 * ============================================================================
 * WAstatus-Style HD WhatsApp Status Bot (CommonJS, QR Scan, Zero-DB, VPS-Ready)
 * ============================================================================
 *
 * HOW IT WORKS:
 * 1. Start the bot on your VPS and scan the terminal QR code in WhatsApp
 *    (WhatsApp -> Linked Devices -> Link a Device).
 * 2. Send any video or photo to the chat as a FILE / DOCUMENT (📎 -> Document)
 *    so WhatsApp uploads the untouched original without pre-compression.
 * 3. This bot streams the file to a temporary folder on your VPS, encodes it
 *    with FFmpeg (libx264 + AAC) using WAstatus's Status-safe HD profile:
 *      - Default 1080p HD: max 1080x1920 (portrait) / 1920x1080 (landscape),
 *        30 fps (prevents 60 fps re-encoding), H.264 High/4.1, yuv420p,
 *        AAC 128k, auto-split into <= 29s clips, strictly under 15.4 MB.
 *      - Optional 720p Extended (add "720p" in file caption):
 *        max 720x1280, 30 fps, auto-split into <= 59s clips, under 15.4 MB.
 *      - Photos sent as files are converted into crisp HD Status MP4 videos
 *        (default 7s; add e.g. "12s" in the file caption to change length).
 * 4. The bot sends the ready-to-post HD clip(s) back to your chat and
 *    immediately deletes all temporary files from disk.
 * 5. Reply/quote any video, photo, or media document with `.status`
 *    (or `.status your caption`) to publish it directly to the bot's own
 *    WhatsApp Status in full original quality and native FPS (e.g., 60 fps).
 *
 * VPS SETUP (Ubuntu / Debian):
 *   sudo apt update && sudo apt install -y ffmpeg
 *   mkdir wa-hd-bot && cd wa-hd-bot
 *   # Save this file as bot.js, then run:
 *   npm init -y
 *   npm install @whiskeysockets/baileys pino qrcode-terminal
 *   node bot.js
 *
 * OPTIONAL ENVIRONMENT VARIABLES:
 *   ALLOWED_NUMBERS=14155551234,919876543210  # Restrict to specific numbers (empty = allow all DMs)
 *   STATUS_CONTACTS=14155551234,919876543210  # Extra contacts always included in .status viewers
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
  maxInputBytes: Number(process.env.MAX_INPUT_MB || 250) * 1024 * 1024,
  // WhatsApp Status re-compresses/rejects clips over ~16 MB; keep a safety margin.
  maxOutputBytes: 15.4 * 1024 * 1024,
  targetOutputMb: 14.8,
  defaultPhotoDurationSec: Math.min(29, Math.max(3, Number(process.env.PHOTO_DURATION_SEC || 7))),
  // WAstatus-tested HD Status profiles
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

const VIDEO_EXTS = new Set(['.mp4', '.mov', '.mkv', '.webm', '.avi', '.m4v', '.3gp']);
const IMAGE_EXTS = new Set(['.jpg', '.jpeg', '.png', '.webp', '.bmp', '.heic', '.heif', '.tiff']);

// Track message IDs sent by this bot to prevent reply loops (including self-chat)
const botSentMessageIds = new Set();

// In-memory contact JIDs allowed to view bot's Status updates (zero-DB)
const knownStatusJids = new Set([
  ...CONFIG.statusContacts,
  ...Array.from(CONFIG.allowedNumbers, (n) => `${n}@s.whatsapp.net`),
]);

// Sequential in-memory queue so a small VPS never runs competing FFmpeg processes
const jobQueue = [];
let isProcessingQueue = false;
let activeSocket = null;
let isShuttingDown = false;
let cachedBaileys = null;

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
  const sent = await sock.sendMessage(jid, content, options);
  rememberSentMessage(sent);
  return sent;
}

/**
 * Normalizes a WhatsApp user JID (strips device suffix like ":12") for status@broadcast.
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
  const user = jid.split('@')[0].split(':')[0].replace(/\D/g, '');
  return user ? `${user}@s.whatsapp.net` : null;
}

function addStatusJid(jid) {
  const normalized = normalizeUserJid(jid);
  if (normalized) {
    knownStatusJids.add(normalized);
  }
}

function collectStatusJidsFromItems(items = []) {
  for (const item of items) {
    if (!item) continue;
    addStatusJid(item.phoneNumber);
    addStatusJid(item.id);
    addStatusJid(item.jid);
  }
}

function getStatusJidList(sock, extraJids = []) {
  const recipients = new Set(knownStatusJids);
  addStatusJid(sock?.user?.id);
  const selfJid = normalizeUserJid(sock?.user?.id);
  if (selfJid) recipients.add(selfJid);

  for (const jid of extraJids) {
    const norm = normalizeUserJid(jid);
    if (norm) recipients.add(norm);
  }

  return Array.from(recipients);
}

/**
 * Unwraps ephemeral / view-once / documentWithCaption envelopes from Baileys messages.
 */
function unwrapMessage(rawMessage) {
  let msg = rawMessage;
  if (!msg) return null;

  if (msg.ephemeralMessage?.message) {
    msg = msg.ephemeralMessage.message;
  }
  if (msg.viewOnceMessage?.message) {
    msg = msg.viewOnceMessage.message;
  }
  if (msg.viewOnceMessageV2?.message) {
    msg = msg.viewOnceMessageV2.message;
  }
  if (msg.viewOnceMessageV2Extension?.message) {
    msg = msg.viewOnceMessageV2Extension.message;
  }
  if (msg.documentWithCaptionMessage?.message) {
    msg = msg.documentWithCaptionMessage.message;
  }

  return msg;
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
 * Extracts quoted video, photo, or media document metadata from a reply contextInfo.
 */
function extractQuotedMedia(contextInfo) {
  const rawQuoted = contextInfo?.quotedMessage;
  if (!rawQuoted) return null;

  const unwrapped = unwrapMessage(rawQuoted);
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
    if (!classification) return null;

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

  return null;
}

/**
 * Detects whether an incoming WhatsApp document is a video or an image.
 */
function classifyDocument(docMsg) {
  const mime = (docMsg.mimetype || '').toLowerCase();
  const fileName = docMsg.fileName || '';
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
 * Probes media metadata (duration, codecs, resolution, FPS, audio availability).
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

  return {
    duration: Number.isFinite(duration) && duration > 0 ? duration : 0,
    hasVideo: Boolean(videoStream),
    hasAudio: Boolean(audioStream),
    videoCodec: (videoStream?.codec_name || '').toLowerCase(),
    audioCodec: (audioStream?.codec_name || '').toLowerCase(),
    width: Number(videoStream?.width || 0),
    height: Number(videoStream?.height || 0),
    fps: Number.isFinite(fps) ? Number(fps.toFixed(2)) : 0,
    formatName: (info.format?.format_name || '').toLowerCase(),
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

function runCommand(binary, args) {
  return new Promise((resolve, reject) => {
    const child = spawn(binary, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';

    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    child.on('error', (err) => {
      reject(new Error(`Failed to start ${binary}: ${err.message}`));
    });

    child.on('close', (code) => {
      if (code === 0) {
        resolve({ stdout, stderr });
      } else {
        const tail = stderr.trim().split('\n').slice(-6).join('\n');
        reject(new Error(`${binary} exited with code ${code}:\n${tail}`));
      }
    });
  });
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
 * Probes media metadata (duration & audio stream availability).
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

  return {
    duration: Number.isFinite(duration) && duration > 0 ? duration : 0,
    hasVideo: Boolean(videoStream),
    hasAudio: Boolean(audioStream),
  };
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
      'medium',
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
    'medium',
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
 * Prepares quoted media for direct status@broadcast upload at FULL original
 * quality and native FPS (e.g., 60 fps untouched, zero unnecessary re-encoding).
 */
async function prepareFullQualityStatusMedia({ inputPath, workDir, kind, ext, mimetype }) {
  const normalizedExt = (ext || '').toLowerCase();

  if (kind === 'image') {
    const isNativeImage =
      normalizedExt === '.jpg' ||
      normalizedExt === '.jpeg' ||
      normalizedExt === '.png' ||
      mimetype === 'image/jpeg' ||
      mimetype === 'image/png';

    if (isNativeImage) {
      return {
        finalPath: inputPath,
        mimetype:
          normalizedExt === '.png' || mimetype === 'image/png' ? 'image/png' : 'image/jpeg',
        summary: 'original image (untouched)',
      };
    }

    // Convert non-native formats (HEIC/WebP/BMP/TIFF) at full native resolution & max quality
    const outPath = path.join(workDir, 'status_full_quality.jpg');
    await runCommand('ffmpeg', ['-y', '-i', inputPath, '-q:v', '1', outPath]);
    return {
      finalPath: outPath,
      mimetype: 'image/jpeg',
      summary: 'full-resolution JPEG',
    };
  }

  // Video handling: preserve exact original quality, resolution, and native FPS (e.g. 60 fps)
  const meta = await probeMedia(inputPath);
  if (!meta.hasVideo) {
    throw new Error('Could not read a valid video stream from the quoted media.');
  }

  const resLabel = meta.width && meta.height ? `${meta.width}x${meta.height}` : 'native';
  const fpsLabel = meta.fps ? `${meta.fps} fps` : 'original fps';
  const isMp4Container = normalizedExt === '.mp4' || meta.formatName.includes('mp4');
  const audioOk = !meta.hasAudio || meta.audioCodec === 'aac';

  // Case 1: Already H.264 + AAC inside MP4 -> upload original bytes untouched (zero loss)
  if (meta.videoCodec === 'h264' && audioOk && isMp4Container) {
    return {
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
    'medium',
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
  quotedMedia,
  cmdCaption
) {
  const jid = msg.key.remoteJid;

  if (quotedMedia.fileLength > CONFIG.maxInputBytes) {
    await safeSendMessage(
      sock,
      jid,
      {
        text: `Quoted file is too large (${(quotedMedia.fileLength / 1024 / 1024).toFixed(1)} MB). Max allowed is ${(
          CONFIG.maxInputBytes /
          1024 /
          1024
        ).toFixed(0)} MB.`,
      },
      { quoted: msg }
    );
    return;
  }

  const statusJidList = getStatusJidList(sock, [
    msg.key.remoteJid,
    msg.key.remoteJidAlt,
    msg.key.participant,
    msg.key.participantAlt,
  ]);

  if (statusJidList.length === 0) {
    await safeSendMessage(
      sock,
      jid,
      {
        text: 'Cannot publish Status yet: no contact JIDs are available. Set STATUS_CONTACTS or wait for WhatsApp contacts to sync.',
      },
      { quoted: msg }
    );
    return;
  }

  const workDir = path.join(os.tmpdir(), `wa-status-cmd-${crypto.randomUUID()}`);
  await fs.mkdir(workDir, { recursive: true });
  const inputPath = path.join(workDir, `quoted${quotedMedia.ext}`);

  try {
    // Wrap the quoted media into a minimal WAMessage so Baileys can stream-download it
    const quotedWAMsg = {
      key: {
        remoteJid: jid,
        id: contextInfo.stanzaId,
        participant: contextInfo.participant,
      },
      message: quotedMedia.message,
    };

    const mediaStream = await downloadMediaMessage(
      quotedWAMsg,
      'stream',
      {},
      {
        logger,
        reuploadRequest: sock.updateMediaMessage,
      }
    );
    await pipeline(mediaStream, createWriteStream(inputPath));

    const prepared = await prepareFullQualityStatusMedia({
      inputPath,
      workDir,
      kind: quotedMedia.kind,
      ext: quotedMedia.ext,
      mimetype: quotedMedia.mimetype,
    });

    // Only attach a caption if the user provided one (on .status or on the quoted media)
    const finalCaption = (cmdCaption || quotedMedia.caption || '').trim();
    const statusPayload =
      quotedMedia.kind === 'video'
        ? { video: { url: prepared.finalPath }, mimetype: prepared.mimetype }
        : { image: { url: prepared.finalPath }, mimetype: prepared.mimetype };

    if (finalCaption) {
      statusPayload.caption = finalCaption;
    }

    await safeSendMessage(sock, 'status@broadcast', statusPayload, {
      broadcast: true,
      statusJidList,
    });

    const stat = await fs.stat(prepared.finalPath);
    const sizeMb = (stat.size / 1024 / 1024).toFixed(2);
    logInfo(
      `Published ${quotedMedia.kind} to status@broadcast (${prepared.summary}, ${sizeMb} MB, ${statusJidList.length} viewer JIDs)`
    );

    await safeSendMessage(
      sock,
      jid,
      {
        text: `Posted to bot Status in full quality (${prepared.summary}, ${sizeMb} MB) for ${statusJidList.length} contact(s).`,
      },
      { quoted: msg }
    );
  } catch (err) {
    logError(`Failed .status command from ${jid}`, err);
    try {
      await safeSendMessage(
        sock,
        jid,
        { text: `Failed to post Status: ${err.message}` },
        { quoted: msg }
      );
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
    const mediaStream = await downloadMediaMessage(
      msg,
      'stream',
      {},
      {
        logger,
        reuploadRequest: sock.updateMediaMessage,
      }
    );
    await pipeline(mediaStream, createWriteStream(inputPath));

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
      addStatusJid(sock.user?.id);
      logInfo(
        'Connected to WhatsApp! Send a video/photo as a File to convert it, or reply to media with .status to post directly.'
      );
    }

    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      const shouldReconnect = statusCode !== DisconnectReason.loggedOut && !isShuttingDown;

      logInfo(`Connection closed (code: ${statusCode ?? 'unknown'}). Reconnect: ${shouldReconnect}`);

      if (shouldReconnect) {
        setTimeout(() => {
          startBot().catch((err) => logError('Reconnect failed', err));
        }, 4000);
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

      // Remember DM contacts in memory for status@broadcast delivery
      addStatusJid(msg.key.remoteJid);
      addStatusJid(msg.key.remoteJidAlt);

      // 1) Handle `.status` command (reply/quote a video, photo, or media file)
      const { text, contextInfo } = extractTextAndContext(msg.message);
      const statusCmdMatch = text.trim().match(/^\.status(?:\s+([\s\S]*))?$/i);
      if (statusCmdMatch) {
        // Allow bot owner anywhere (including groups/self-chat) or allowed DM senders
        if (!msg.key.fromMe && !isAllowedSender(msg.key)) continue;

        const quotedMedia = extractQuotedMedia(contextInfo);
        if (!quotedMedia) {
          await safeSendMessage(
            sock,
            msg.key.remoteJid,
            {
              text: 'Reply to (quote) a video, photo, or media file with `.status` to post it to Status in full original quality.',
            },
            { quoted: msg }
          );
          continue;
        }

        const cmdCaption = (statusCmdMatch[1] || '').trim();
        logInfo(`Queued .status (${quotedMedia.kind}) from ${msg.key.remoteJid}`);
        enqueueJob(() =>
          processStatusCommandJob(
            sock,
            downloadMediaMessage,
            msg,
            contextInfo,
            quotedMedia,
            cmdCaption
          )
        );
        continue;
      }

      if (!isAllowedSender(msg.key)) continue;

      // 2) Only handle Document/File uploads so WhatsApp doesn't compress the source
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
