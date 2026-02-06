import asyncio
import logging
import os
import time
import subprocess
import json
import re
import xml.etree.ElementTree as ET
import base64
import hashlib
import zlib
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding as crypto_padding
from cryptography.hazmat.backends import default_backend
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile, CallbackQuery
from aiogram.exceptions import TelegramBadRequest
import crunpyroll
from pywidevine.cdm import Cdm
from pywidevine.device import Device
from pywidevine.pssh import PSSH
import yt_dlp
import requests

# --- CONFIGURATION ---
TOKEN = "8542919849:AAGT0Cl0PbeQwOY6IEBV3SgYVXHFFNIHG0M"
CRUNCHYROLL_EMAIL = "martinez.margarita23@hotmail.com"
CRUNCHYROLL_PASSWORD = "Juan2024"
MAX_CONCURRENT_DOWNLOADS = 2
MAX_QUEUE_SIZE = 20
TELEGRAM_FILE_LIMIT_MB = 50
CDM_PATH = os.path.expanduser("~/.config/widevine/device.wvd")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DownloaderBot")

bot = Bot(token=TOKEN)
dp = Dispatcher()

download_queue = asyncio.Queue()
active_users = set()

cr_client = crunpyroll.Client(email=CRUNCHYROLL_EMAIL, password=CRUNCHYROLL_PASSWORD, locale="en-US")

async def start_cr_client():
    try:
        await cr_client.start()
        logger.info("Crunchyroll client started successfully.")
    except Exception as e:
        logger.error(f"Crunchyroll login failed: {e}")

asyncio.run(start_cr_client())

session_cache = {}  # {user_id: {'data': dict, 'timestamp': time}}

# --- SUBTITLE PARSING (Adapted from python-crunchyroll lib) ---
def fetch_subtitle_ass(sub_id, locale="en-US"):
    url = f"https://www.crunchyroll.com/xml/?req=RpcApiSubtitle_GetXml&subtitle_script_id={sub_id}"
    resp = requests.get(url)
    if resp.status_code != 200:
        raise ValueError("Failed to fetch subtitle XML")
    root = ET.fromstring(resp.text)
    subtitle_elem = root.find(".//subtitle")
    if subtitle_elem is None:
        raise ValueError("No subtitle element found")
    iv_b64 = subtitle_elem.find("iv").text
    data_b64 = subtitle_elem.find("data").text
    if not iv_b64 or not data_b64:
        raise ValueError("Missing IV or data in XML")
    
    iv = base64.b64decode(iv_b64)
    data = base64.b64decode(data_b64)
    key = hashlib.md5(str(sub_id).encode()).digest()
    
    backend = default_backend()
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=backend)
    decryptor = cipher.decryptor()
    decrypted_padded = decryptor.update(data) + decryptor.finalize()
    
    unpadder = crypto_padding.PKCS7(algorithms.AES.block_size).unpadder()
    decrypted = unpadder.update(decrypted_padded) + unpadder.finalize()
    
    try:
        subtitle_xml = zlib.decompress(decrypted)
    except zlib.error:
        subtitle_xml = decrypted
    
    try:
        inner_root = ET.fromstring(subtitle_xml)
    except ET.ParseError:
        raise ValueError("Failed to parse decrypted subtitle XML")
    
    events = []
    for event in inner_root.findall(".//event"):
        start = event.get('start')
        end = event.get('end')
        text = event.text.strip() if event.text else ""
        events.append((start, end, text))
    
    # Format to ASS (simple, without styles for now)
    ass_content = "[Script Info]\nTitle: Crunchyroll Subs\nScriptType: v4.00+\n\n[V4+ Styles]\nFormat: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\nStyle: Default,Arial,20,&H00FFFFFF,&H000000FF,&H00000000,&H00000000,0,0,0,0,100,100,0,0,1,2,2,2,10,10,10,1\n\n[Events]\nFormat: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n"
    for i, (start, end, text) in enumerate(events, 1):
        start_ass = start.replace('.', ',') if start else "0:00:00,00"
        end_ass = end.replace('.', ',') if end else "0:00:00,00"
        ass_content += f"Dialogue: 0,{start_ass},{end_ass},Default,,0,0,0,,{text}\n"
    return ass_content

# --- HELPERS ---
async def progress_hook(d, chat_id, message_id, last_update):
    if d['status'] == 'downloading':
        now = time.time()
        if now - last_update['time'] < 4:
            return
        last_update['time'] = now
        try:
            percent = float(d.get('_percent_str', '0%').replace('%', '') or 0)
            filled = int(percent // 5)
            bar = "█" * filled + "░" * (20 - filled)
            eta = d.get('_eta_str', '?')
            speed = d.get('_speed_str', '?')
            total = d.get('_total_bytes_str', '?')
            downloaded = d.get('_downloaded_bytes_str', '?')
            text = f"⬇️ **Downloading...**\n`[{bar}]` {percent:.1f}%\n💾 **Downloaded:** {downloaded} / {total}\n⚡ **Speed:** {speed}\n⏱ **ETA:** {eta}"
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode="MarkdownV2")
        except TelegramBadRequest:
            pass
        except Exception as e:
            logger.warning(f"Progress update failed: {e}")

def is_crunchyroll_url(url):
    return 'crunchyroll.com' in url.lower()

def parse_crunchyroll_id(url):
    parts = url.split('/')
    if 'watch' in parts:
        return parts[parts.index('watch') + 1]
    elif 'series' in parts:
        return parts[parts.index('series') + 1]
    return None

async def fetch_cr_metadata(id, is_episode=True):
    try:
        if is_episode:
            media = await cr_client.get_media(id)
            available_audio = media.audio_locales if hasattr(media, 'audio_locales') else ['ja-JP', 'en-US']  # Assume
            available_subs = media.subtitle_locales if hasattr(media, 'subtitle_locales') else ['en-US', 'ja-JP']  # Assume dict locale: sub_id
            sub_ids = media.subtitle_ids if hasattr(media, 'subtitle_ids') else {}  # {locale: sub_id}
            return {
                'title': media.title,
                'description': media.description,
                'episode': media.episode_number,
                'season': media.season_number,
                'available_audio': available_audio,
                'available_subs': available_subs,
                'sub_ids': sub_ids
            }
        else:
            series = await cr_client.get_series(id)
            return {
                'title': series.title,
                'description': series.description,
                'available_audio': ['ja-JP', 'en-US'],  # Default
                'available_subs': ['en-US', 'ja-JP']
            }
    except Exception as e:
        logger.error(f"Metadata fetch failed: {e}")
        return {'available_audio': ['ja-JP'], 'available_subs': ['en-US'], 'sub_ids': {}}

async def download_crunchyroll_with_drm(job, audio_locale="ja-JP", sub_locale=None, quality="1080", is_audio=False):
    episode_id = parse_crunchyroll_id(job['url'])
    if not episode_id:
        raise ValueError("Invalid Crunchyroll URL")
    
    chat_id = job['chat_id']
    msg_id = job['message_id']
    filename_base = f"dl_{job['user_id']}_{int(time.time())}"
    video_file = f"{filename_base}.mkv"
    final_filename = video_file if not is_audio else f"{filename_base}.mp3"
    
    await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text="⬇️ **Fetching Streams...**", parse_mode="MarkdownV2")
    
    streams = await cr_client.get_streams(episode_id, audio_locale=audio_locale)
    
    # Use no hardsub for soft subs
    stream_url = streams.adaptive_hls.get('', streams.adaptive_hls[list(streams.adaptive_hls.keys())[0]]).url if streams.adaptive_hls else streams.url
    
    manifest = await cr_client.get_manifest(stream_url)
    
    if not os.path.exists(CDM_PATH):
        raise FileNotFoundError("Widevine CDM file not found. Provide device.wvd")
    
    device = Device.load(CDM_PATH)
    cdm = Cdm.from_device(device)
    pssh = PSSH(manifest.content_protection.widevine.pssh)
    
    session_id = cdm.open()
    challenge = cdm.get_license_challenge(session_id, pssh)
    license_resp = await cr_client.get_license(streams.media_id, challenge=challenge, token=streams.token)
    
    cdm.parse_license(session_id, license_resp)
    keys = cdm.get_keys(session_id, "CONTENT")
    cdm.close(session_id)
    
    key_str = [f"{key.kid.hex}:{key.key.hex()}" for key in keys][0]
    
    await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text="⬇️ **Downloading with DRM Decryption...**", parse_mode="MarkdownV2")
    
    headers = json.dumps(dict(cr_client.session.headers))
    cmd = [
        "N_m3u8DL-RE", stream_url,
        "--save-name", filename_base,
        "--key", key_str,
        "--header", headers,
        "--mux-after-done", "format=mkv:muxer=ffmpeg",
        "--thread-count", "5",
        "--log-level", "INFO",
        "--no-ansi-color"
    ]
    if quality:
        cmd += ["--select-video", f"height={quality}"]
    
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT
    )
    
    last_update = {'time': 0}
    pattern = re.compile(r'(\d+\.\d+)% \(([\d\.]+ \w+) / ([\d\.]+ \w+)\) speed=([\d\.]+ \w+/s) ETA=([\d:]+)')
    
    async for line in proc.stdout:
        line = line.decode().strip()
        match = pattern.search(line)
        if match:
            now = time.time()
            if now - last_update['time'] < 4:
                continue
            last_update['time'] = now
            percent = float(match.group(1))
            downloaded = match.group(2)
            total = match.group(3)
            speed = match.group(4)
            eta = match.group(5)
            filled = int(percent // 5)
            bar = "█" * filled + "░" * (20 - filled)
            text = f"⬇️ **Downloading...**\n`[{bar}]` {percent:.1f}%\n💾 **Downloaded:** {downloaded} / {total}\n⚡ **Speed:** {speed}\n⏱ **ETA:** {eta}"
            try:
                await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text, parse_mode="MarkdownV2")
            except TelegramBadRequest:
                pass
        elif "No match" in line:  # Fallback
            text = "⬇️ **Downloading...** (Progress not available)"
            await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text, parse_mode="MarkdownV2")
    
    await proc.wait()
    if proc.returncode != 0:
        raise RuntimeError("Download failed")
    
    # Embed subtitle if selected
    if sub_locale:
        meta = await fetch_cr_metadata(episode_id)
        sub_id = meta['sub_ids'].get(sub_locale)
        if sub_id:
            ass_content = fetch_subtitle_ass(sub_id, sub_locale)
            ass_file = f"{filename_base}.ass"
            with open(ass_file, 'w') as f:
                f.write(ass_content)
            output_file = f"{filename_base}_sub.mkv"
            subprocess.run([
                "ffmpeg", "-i", video_file, "-i", ass_file, 
                "-c", "copy", "-map", "0", "-map", "1", 
                "-metadata:s:s:0", f"language={sub_locale.split('-')[0]}",
                output_file
            ], check=True)
            os.remove(video_file)
            os.remove(ass_file)
            video_file = output_file
    
    await cr_client.delete_active_stream(streams.media_id, token=streams.token)
    
    if is_audio:
        subprocess.run(["ffmpeg", "-i", video_file, "-vn", final_filename], check=True)
        os.remove(video_file)
    
    return final_filename

# --- HANDLERS ---
@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    photo = FSInputFile("intro_photo.jpg")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Search Series/Movies", callback_data="search_anime")],
        [InlineKeyboardButton(text="📺 Browse Popular", callback_data="browse_popular")],
        [InlineKeyboardButton(text="ℹ️ Help", callback_data="help")]
    ])
    await message.answer_photo(
        photo,
        caption="👋 **Welcome to Anime Downloader Bot!**\nPowered by xAI. Send a link or use buttons to search/download from Crunchyroll and more.\n\nNew: Subtitles, Dubs, Quality, Batch!",
        reply_markup=kb,
        parse_mode="MarkdownV2"
    )

@dp.callback_query(F.data == "search_anime")
async def search_anime(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer("Enter anime series/movie name to search:")

@dp.callback_query(F.data == "browse_popular")
async def browse_popular(callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    if user_id in active_users:
        await callback.message.answer("⚠️ Wait: You have a task in progress.")
        return
    active_users.add(user_id)
    try:
        results = await cr_client.search("")  # Assuming empty query for popular or adjust accordingly
        if not results.items:
            await callback.message.answer("No popular results found.")
            return
        kb_rows = [[InlineKeyboardButton(text=item.title, callback_data=f"select_series:{item.id}")] for item in results.items[:10]]
        kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
        await callback.message.answer("Popular series/movies:", reply_markup=kb)
    except Exception as e:
        await callback.message.answer("Browse popular failed. Use search instead.")
    finally:
        active_users.remove(user_id)

@dp.callback_query(F.data == "help")
async def help_callback(callback: CallbackQuery):
    await callback.answer()
    help_text = """**Help Guide:**
- Send a Crunchyroll link to download.
- Search by name to browse series.
- Customize audio, subs, quality before download.
- Batch download seasons.
- For non-Crunchyroll links, select format.
Contact admin for issues."""
    await callback.message.answer(help_text, parse_mode="MarkdownV2")

@dp.message(F.text & ~F.text.startswith("http"))
async def handle_search(message: types.Message):
    user_id = message.from_user.id
    if user_id in active_users:
        await message.reply("⚠️ Wait: You have a task in progress.")
        return
    query = message.text
    active_users.add(user_id)
    try:
        results = await cr_client.search(query)
        if not results.items:
            await message.reply("No results found.")
            return
        kb_rows = [[InlineKeyboardButton(text=item.title, callback_data=f"select_series:{item.id}")] for item in results.items[:10]]
        kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
        await message.answer("Select a series/movie:", reply_markup=kb)
    finally:
        active_users.remove(user_id)

@dp.callback_query(F.data.startswith("select_series:"))
async def select_series(callback: CallbackQuery):
    series_id = callback.data.split(":")[1]
    seasons = await cr_client.get_seasons(series_id)
    kb_rows = []
    for season in seasons:
        kb_rows.append([InlineKeyboardButton(text=season.title, callback_data=f"select_season:{series_id}:{season.id}")])
    if len(seasons) == 0:  # Movie or single
        await select_season(CallbackQuery(from_user=callback.from_user, message=callback.message, data=f"select_season:{series_id}:{series_id}"))
        return
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    await callback.message.edit_text("Select season:", reply_markup=kb)

@dp.callback_query(F.data.startswith("select_season:"))
async def select_season(callback: CallbackQuery):
    parts = callback.data.split(":")
    series_id, season_id = parts[1], parts[2]
    episodes = await cr_client.get_episodes(season_id)
    kb_rows = []
    for ep in episodes[:20]:
        kb_rows.append([InlineKeyboardButton(text=ep.title, callback_data=f"select_episode:{ep.id}")])
    kb_rows.append([InlineKeyboardButton(text="📥 Download All Episodes", callback_data=f"batch_season:{season_id}")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    await callback.message.edit_text("Select episode or batch:", reply_markup=kb)

@dp.callback_query(F.data.startswith("batch_season:"))
async def batch_season(callback: CallbackQuery):
    season_id = callback.data.split(":")[1]
    episodes = await cr_client.get_episodes(season_id)
    user_id = callback.from_user.id
    queued = 0
    for ep in episodes:
        if download_queue.qsize() >= MAX_QUEUE_SIZE:
            await callback.answer("Queue full. Partial batch queued.")
            break
        job = {
            'chat_id': callback.message.chat.id,
            'message_id': callback.message.message_id,
            'url': f"https://www.crunchyroll.com/watch/{ep.id}",
            'format_type': 'best',
            'user_id': user_id,
            'audio_locale': session_cache.get(user_id, {}).get('audio_locale', 'ja-JP'),
            'sub_locale': session_cache.get(user_id, {}).get('sub_locale', 'en-US'),
            'quality': session_cache.get(user_id, {}).get('quality', '1080')
        }
        await download_queue.put(job)
        queued += 1
    await callback.message.edit_text(f"⏳ **Batch Queued:** {queued} episodes.")

@dp.callback_query(F.data.startswith("select_episode:"))
async def select_episode(callback: CallbackQuery):
    episode_id = callback.data.split(":")[1]
    meta = await fetch_cr_metadata(episode_id)
    user_id = callback.from_user.id
    session_cache[user_id] = {'episode_id': episode_id, 'meta': meta, 'timestamp': time.time()}
    info_text = f"> **Title:** {meta['title']}\n> **Episode:** {meta['episode']} (Season {meta['season']})\n> **Description:** {meta['description'][:200]}..." if meta else ""
    kb_rows = []
    # Audio buttons
    audio_row = [InlineKeyboardButton(text=f"Audio: {loc}", callback_data=f"set_audio:{loc}:{episode_id}") for loc in meta['available_audio'][:3]]
    kb_rows.append(audio_row)
    # Sub buttons
    sub_row = [InlineKeyboardButton(text=f"Subs: {loc}", callback_data=f"set_sub:{loc}:{episode_id}") for loc in meta['available_subs'][:3]]
    kb_rows.append(sub_row)
    # Quality buttons
    quality_row = [
        InlineKeyboardButton(text="1080p", callback_data=f"set_quality:1080:{episode_id}"),
        InlineKeyboardButton(text="720p", callback_data=f"set_quality:720:{episode_id}"),
        InlineKeyboardButton(text="480p", callback_data=f"set_quality:480:{episode_id}")
    ]
    kb_rows.append(quality_row)
    # Download options
    dl_row = [
        InlineKeyboardButton(text="🎬 Video", callback_data=f"dl_cr_best:{episode_id}"),
        InlineKeyboardButton(text="🎧 Audio Only", callback_data=f"dl_cr_audio:{episode_id}")
    ]
    kb_rows.append(dl_row)
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    await callback.message.edit_text(f"{info_text}\n\nCustomize & Download:", reply_markup=kb, parse_mode="MarkdownV2")

@dp.callback_query(F.data.startswith("set_audio:"))
async def set_audio(callback: CallbackQuery):
    parts = callback.data.split(":")
    audio_locale = parts[1]
    episode_id = parts[2]
    user_id = callback.from_user.id
    session_cache[user_id]['audio_locale'] = audio_locale
    await callback.answer(f"Audio set to {audio_locale}")
    # Refresh options
    await select_episode(CallbackQuery(from_user=callback.from_user, message=callback.message, data=f"select_episode:{episode_id}"))

@dp.callback_query(F.data.startswith("set_sub:"))
async def set_sub(callback: CallbackQuery):
    parts = callback.data.split(":")
    sub_locale = parts[1]
    episode_id = parts[2]
    user_id = callback.from_user.id
    session_cache[user_id]['sub_locale'] = sub_locale
    await callback.answer(f"Subs set to {sub_locale}")

@dp.callback_query(F.data.startswith("set_quality:"))
async def set_quality(callback: CallbackQuery):
    parts = callback.data.split(":")
    quality = parts[1]
    episode_id = parts[2]
    user_id = callback.from_user.id
    session_cache[user_id]['quality'] = quality
    await callback.answer(f"Quality set to {quality}p")

@dp.callback_query(F.data.startswith("dl_cr_"))
async def process_cr_callback(callback: CallbackQuery):
    parts = callback.data.split(":")
    action = parts[1]
    episode_id = parts[2]
    user_id = callback.from_user.id
    if download_queue.qsize() >= MAX_QUEUE_SIZE:
        await callback.answer("Queue full. Try later.")
        return
    if user_id not in session_cache or time.time() - session_cache[user_id]['timestamp'] > 300:
        await callback.answer("❌ Session expired.")
        return
    await callback.message.edit_text("⏳ **Queued.** Waiting for worker...", parse_mode="MarkdownV2")
    job = {
        'chat_id': callback.message.chat.id,
        'message_id': callback.message.message_id,
        'url': f"https://www.crunchyroll.com/watch/{episode_id}",
        'format_type': action,
        'user_id': user_id,
        'audio_locale': session_cache[user_id].get('audio_locale', 'ja-JP'),
        'sub_locale': session_cache[user_id].get('sub_locale', None),
        'quality': session_cache[user_id].get('quality', None)
    }
    await download_queue.put(job)

@dp.message(F.text.startswith("http"))
async def handle_url(message: types.Message):
    user_id = message.from_user.id
    if user_id in active_users:
        await message.reply("⚠️ **Wait:** You have a download in progress.", parse_mode="MarkdownV2")
        return
    if download_queue.qsize() >= MAX_QUEUE_SIZE:
        await message.reply("🔥 **Server Busy:** Queue is full. Try again in 1 minute.", parse_mode="MarkdownV2")
        return
    url = message.text
    active_users.add(user_id)
    if is_crunchyroll_url(url):
        id = parse_crunchyroll_id(url)
        if 'watch' in url:
            await select_episode(CallbackQuery(from_user=message.from_user, message=message, data=f"select_episode:{id}"))
        elif 'series' in url:
            await select_series(CallbackQuery(from_user=message.from_user, message=message, data=f"select_series:{id}"))
        else:
            await message.reply("Invalid Crunchyroll link.")
            active_users.remove(user_id)
    else:
        session_cache[user_id] = {'url': url, 'timestamp': time.time()}
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎬 Best Quality (MP4)", callback_data="dl_best")],
            [InlineKeyboardButton(text="🎧 Audio Only", callback_data="dl_audio")]
        ])
        await message.answer(f"🔗 **Link Received:**\n`{url}`\n\nSelect format:", reply_markup=kb, parse_mode="MarkdownV2")

@dp.callback_query(F.data.startswith("dl_"))
async def process_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    action = callback.data.split("_")[1]
    if user_id not in session_cache or time.time() - session_cache[user_id]['timestamp'] > 300:
        await callback.answer("❌ Session expired. Send link again.")
        active_users.discard(user_id)
        return
    url = session_cache.pop(user_id)['url']
    await callback.message.edit_text("⏳ **Queued.** Waiting for worker...", parse_mode="MarkdownV2")
    job = {
        'chat_id': callback.message.chat.id,
        'message_id': callback.message.message_id,
        'url': url,
        'format_type': action,
        'user_id': user_id
    }
    await download_queue.put(job)

# --- WORKER ---
async def worker(worker_id):
    logger.info(f"👷 Worker {worker_id} started.")
    while True:
        job = await download_queue.get()
        try:
            await process_job(job)
        except Exception as e:
            logger.error(f"Worker Error: {e}")
            await bot.send_message(job['chat_id'], f"❌ Error: {str(e)}", parse_mode="MarkdownV2")
        finally:
            active_users.discard(job['user_id'])
            download_queue.task_done()

async def process_job(job):
    chat_id = job['chat_id']
    msg_id = job['message_id']
    url = job['url']
    format_type = job['format_type']
    filename_base = f"dl_{job['user_id']}_{int(time.time())}"
    last_update = {'time': 0}
    final_filename = None
    try:
        if is_crunchyroll_url(url):
            is_audio = format_type == 'audio'
            final_filename = await download_crunchyroll_with_drm(
                job, 
                audio_locale=job.get('audio_locale', 'ja-JP'),
                sub_locale=job.get('sub_locale', None),
                quality=job.get('quality', None),
                is_audio=is_audio
            )
        else:
            ydl_opts = {
                'outtmpl': f'{filename_base}.%(ext)s',
                'quiet': True,
                'noplaylist': True,
                'socket_timeout': 30,
                'concurrent_fragment_downloads': 5,
                'progress_hooks': [lambda d: asyncio.create_task(progress_hook(d, chat_id, msg_id, last_update))],
            }
            if format_type == 'audio':
                ydl_opts['format'] = 'bestaudio/best'
                ydl_opts['postprocessors'] = [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3'}]
            else:
                ydl_opts['format'] = 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best'
                ydl_opts['merge_output_format'] = 'mp4'
            await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text="⬇️ **Starting Download...**", parse_mode="MarkdownV2")
            loop = asyncio.get_event_loop()
            def run_yt_dlp():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    return ydl.prepare_filename(info)
            final_filename = await loop.run_in_executor(None, run_yt_dlp)
            if not os.path.exists(final_filename):
                for ext in ['.mp4', '.mkv', '.mp3']:
                    if os.path.exists(f"{filename_base}{ext}"):
                        final_filename = f"{filename_base}{ext}"
                        break

        file_size_mb = os.path.getsize(final_filename) / (1024 * 1024)
        if file_size_mb > TELEGRAM_FILE_LIMIT_MB:
            await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=f"❌ **File Too Large.**\nSize: {file_size_mb:.2f}MB\nLimit: {TELEGRAM_FILE_LIMIT_MB}MB", parse_mode="MarkdownV2")
            os.remove(final_filename)
            return

        await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text="📤 **Uploading...**", parse_mode="MarkdownV2")
        media_file = FSInputFile(final_filename)
        if format_type == 'audio':
            await bot.send_audio(chat_id, media_file, caption="🎵 Downloaded via Bot")
        else:
            await bot.send_video(chat_id, media_file, caption="🎥 Downloaded via Bot", supports_streaming=True)
        await bot.delete_message(chat_id, msg_id)
    except Exception as e:
        await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=f"❌ Failed: {str(e)}", parse_mode="MarkdownV2")
        raise e
    finally:
        if final_filename and os.path.exists(final_filename):
            os.remove(final_filename)

# --- MAIN ---
async def main():
    for i in range(MAX_CONCURRENT_DOWNLOADS):
        asyncio.create_task(worker(i))
    logger.info("🚀 Bot Started")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")