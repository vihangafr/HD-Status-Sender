# HD Status Sender Bot (WhatsApp)

A lightweight, **zero-database**, CommonJS Node.js WhatsApp bot built with **Baileys** and **FFmpeg** that returns images and videos in **full original quality, original bitrate, and original FPS (including 60 fps)**—and lets you post crisp, zero-quality-drop WhatsApp Status updates directly from a Linux VPS.

- **Zero Database Required:** Session credentials and synced contact caches are stored locally in `./auth_info`.
- **QR Code Login:** Instant terminal QR scan via WhatsApp Linked Devices.
- **Public + Owner Modes:** Works for everyone out of the box (`.hd`, `.menu`, `.ping`, `.runtime`, DM file converter), while keeping personal controls (`.status`, `.statusinfo`, `.save`) locked strictly to the bot owner.

---

## Features

- **Full Original Quality `.hd` Command:** Reply to any image or video file (or put `.hd` in the file caption) to receive it back as native WhatsApp media with **zero compression**, preserving the **original resolution, original bitrate, and original FPS**, along with your caption if provided.
- **Share Full-Quality Status Directly from `.hd`:** You can share a full-quality Status simply by **forwarding the video or photo returned by `.hd` directly to My Status**.
- **Owner-Only `.status` Direct Posting:** Reply to any video, photo, or image/video file with `.status` to broadcast it straight to the bot's WhatsApp Status (`status@broadcast`) in full original quality and native FPS.
- **Automatic Status Privacy Support:** Reads your real WhatsApp Status privacy settings (**My contacts**, **My contacts except...**, or **Only share with...**) automatically, plus optional extra exclusions via `STATUS_EXCEPT`.
- **DM File Auto-Converter (WAstatus Profile):** Send any video or photo as a **Document / File** (`📎 → Document`) in a 1-on-1 DM to automatically encode and split it into Status-safe H.264 clips (`1080p HD` ≤ 29s or `720p Extended` ≤ 59s, strictly under `15.4 MB`).
- **Owner-Only `.save` Command:** Reply to any regular (non–View Once) photo, video, audio/voice note, or document to copy it privately to your **Message yourself** inbox without posting anything in the original chat.
- **Clean Captions:** Never adds unwanted auto-generated captions—captions are included only when you provide one.
- **VPS-Safe & Account-Friendly:** Sequential FFmpeg queue (prevents CPU/RAM spikes on small VPS servers), strict timeout guards, outgoing message pacing, and exponential reconnect backoff.

---

## How to Post Status in Full HD Quality

### Method 1: Using `.hd` → Forward Directly to Status (Recommended for Everyone)
1. Send your video or photo to a chat as a **Document / File** (`📎 → Document`) so WhatsApp uploads the untouched original file.
2. Reply (quote) that file with **`.hd`** *(or type `.hd` directly in the file caption before sending; you can also add `.hd Your caption`)*.
3. The bot sends the media back as a native WhatsApp video/photo in **full original quality, original bitrate, and original FPS**.
4. **Forward that `.hd` video/photo directly to My Status** to share your Status in full quality.

### Method 2: Direct Bot Status Broadcast (Owner Only)
1. Reply to any video, photo, or image/video file with **`.status`** (or `.status Your caption`).
2. The bot publishes it directly to `status@broadcast` at full original quality and native FPS while honoring your WhatsApp Status privacy settings (including **My contacts except...**).

### Method 3: DM Auto-Converter (For Splitting Long Videos)
1. Send a video or photo as a **Document / File** (`📎 → Document`) in a 1-on-1 DM with the bot.
   - Default profile: **1080p HD**, 30 fps, auto-split into ≤ 29s parts (`< 16 MB`).
   - Add `720p` in the caption for **720p Extended** parts up to 59s.
   - Add e.g. `12s` on a photo file to set its video duration (`3–29s`).
2. Save the returned clip(s), resend via `📎 → Gallery` with **HD** enabled, and forward that copy to **My Status**.

---

## Commands

### Public Commands (Work in DMs & Groups)
| Command | Usage | Description |
| :--- | :--- | :--- |
| `.hd` | Reply to image/video file (or put in caption) | Returns the media in **full original quality, bitrate & FPS**. Forward the returned video/photo directly to **My Status** to share Status in full quality. |
| `.hd <caption>` | Reply to image/video file | Same as `.hd`, with a custom caption (keeps original caption if omitted). |
| `.menu` / `.help` | Send anywhere | Displays the built-in user manual. |
| `.ping` | Send anywhere | Measures real WhatsApp server round-trip latency (`ms`). |
| `.runtime` | Send anywhere | Shows bot uptime, RAM usage, queue length, and active mode. |
| *(DM File Upload)* | Send image/video as Document in DM | Auto-converts & splits into Status-ready HD MP4 clips (`720p` / `12s` optional flags). |

### Owner-Only Commands (`fromMe` Only)
| Command | Usage | Description |
| :--- | :--- | :--- |
| `.status` | Reply to media/file (or put in caption) | Posts directly to the bot's Status in full original quality & native FPS. |
| `.status <caption>` | Reply to media/file | Posts directly to Status with your custom caption. |
| `.statusinfo` | Send anywhere | Shows active WhatsApp Status privacy mode, cached contacts, excluded count, and viewer count. |
| `.save` | Reply to regular photo/video/audio/file | Privately copies regular (non–View Once) media to your **Message yourself** inbox. |

---

## How Quality Preservation Works

WhatsApp normally compresses media inside the mobile app *before* end-to-end encryption. Because this bot uploads encrypted media bytes directly through Baileys:

- **H.264 + AAC `.mp4` videos & `.jpg`/`.png` photos:** Uploaded **100% byte-for-byte untouched** (exact original bitrate, resolution, and 60 fps).
- **H.264 `.mov` / `.mkv` videos:** Remuxed losslessly with `-c:v copy` (zero video re-encoding, original bitrate and FPS).
- **HEVC (H.265) / VP9 / AV1 videos & HEIC / WebP photos:** Converted once at near-lossless quality (`libx264 CRF 15` / JPEG `-q:v 1`) at **native resolution and native FPS** (no 30 fps cap) so WhatsApp plays them cleanly.

---

## VPS Installation & Setup

### Prerequisites
- Linux VPS (Ubuntu / Debian / Oracle Linux)
- **Node.js v18+** (Node.js v20+ recommended)
- **FFmpeg & FFprobe**

### 1. Install FFmpeg
On Ubuntu / Debian:
```bash
sudo apt update && sudo apt install -y ffmpeg
```

On Oracle Linux / RHEL / CentOS:
```bash
sudo dnf install -y epel-release
sudo dnf install -y ffmpeg
```

### 2. Clone the Repository & Install Dependencies
```bash
git clone https://github.com/vihangafr/HD-Status-Sende.git
cd HD-Status-Sende
npm install @whiskeysockets/baileys pino qrcode-terminal
```

### 3. Run & Link via QR Code
```bash
node index.js
```
*(If your main script file is named `bot.js`, run `node bot.js` instead.)*

Scan the terminal QR code on your phone:
**WhatsApp → Settings → Linked Devices → Link a Device**.

Once connected, your session is saved automatically inside `./auth_info`.

---

## Running 24/7 on a VPS with PM2

Keep the bot alive in the background and auto-start it after VPS reboots:

```bash
# 1. Install PM2 globally
sudo npm install -g pm2

# 2. Start the bot with safe memory & restart limits
pm2 start index.js --name hd-status-bot --restart-delay 5000 --max-memory-restart 400M

# 3. Enable auto-start on server reboot (run the command printed by pm2 startup)
pm2 startup
pm2 save
```

### Useful PM2 Commands
```bash
pm2 status                    # Check bot status
pm2 logs hd-status-bot        # View live logs (and QR code if re-linking)
pm2 restart hd-status-bot     # Restart after code updates
pm2 stop hd-status-bot        # Stop the bot
```

---

## Environment Variables (Optional)

All configuration is optional and database-free. You can pass variables directly when launching or via PM2:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `ALLOWED_NUMBERS` | *(empty = public)* | Comma-separated phone numbers with country code (e.g. `14155551234,919876543210`) to restrict access. Leave empty for everyone. |
| `STATUS_CONTACTS` | *(empty)* | Extra phone numbers always included in `.status` broadcasts. |
| `STATUS_EXCEPT` | *(empty)* | Extra phone numbers always excluded from `.status` broadcasts (in addition to WhatsApp's "My contacts except..."). |
| `FFMPEG_PRESET` | `veryfast` | x264 encoding speed preset (`ultrafast`, `veryfast`, `fast`, `medium`). |
| `MIN_SEND_DELAY_MS` | `1200` | Minimum pacing delay (`ms`) between outgoing messages to prevent burst-send flags. |
| `PHOTO_DURATION_SEC` | `7` | Default duration (`3–29` seconds) when converting a DM photo document into a Status video. |
| `MAX_INPUT_MB` | `250` | Maximum incoming media/document size accepted (`MB`). |
| `AUTH_DIR` | `./auth_info` | Local folder storing WhatsApp auth credentials and `status-contacts.json`. |
| `DOWNLOAD_TIMEOUT_MS` | `120000` | Timeout (`ms`) for downloading incoming media streams. |
| `REUPLOAD_TIMEOUT_MS` | `20000` | Timeout (`ms`) for Baileys media re-upload requests. |
| `FFMPEG_TIMEOUT_MS` | `300000` | Timeout (`ms`) for FFmpeg processing jobs. |
| `SEND_TIMEOUT_MS` | `120000` | Timeout (`ms`) for outgoing WhatsApp uploads. |
| `LOG_LEVEL` | `silent` | Baileys/Pino internal log verbosity (`silent`, `error`, `warn`, `info`). |

Example with PM2:
```bash
STATUS_EXCEPT=919876543210 FFMPEG_PRESET=veryfast pm2 restart hd-status-bot --update-env
pm2 save
```

---

## Disclaimer

This project uses the unofficial [`@whiskeysockets/baileys`](https://github.com/WhiskeySockets/Baileys) WhatsApp Web library and is not affiliated with, endorsed by, or sponsored by WhatsApp LLC or Meta Platforms, Inc. Use responsibly and avoid spam or automated bulk messaging to minimize account-restriction risks.
