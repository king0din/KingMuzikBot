import os
import re
import sys
import time
import uuid
import json
import random
import logging
import tempfile
import threading
import subprocess
import psutil
from io import BytesIO
from datetime import datetime, timezone, timedelta
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import quote, urljoin
import aiohttp
import aiofiles
import asyncio
import requests
import isodate
import psutil
import pymongo
from pymongo import MongoClient, ASCENDING
from bson import ObjectId
from bson.binary import Binary
from dotenv import load_dotenv
from flask import Flask, request
from PIL import Image, ImageDraw, ImageFont, ImageFilter
from pyrogram import Client, filters, errors
from pyrogram.enums import ChatType, ChatMemberStatus, ParseMode
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    ChatPermissions,
)
from pyrogram.errors import RPCError
from pytgcalls import PyTgCalls, idle
from pytgcalls.types import MediaStream
from pytgcalls import filters as fl
from pytgcalls.types import (
    ChatUpdate,
    UpdatedGroupCallParticipant,
    Update as TgUpdate,
)
from pytgcalls.types.stream import StreamEnded
from typing import Union
import urllib
from FrozenMusic.infra.concurrency.ci import deterministic_privilege_validator
from FrozenMusic.telegram_client.vector_transport import vector_transport_resolver
from FrozenMusic.infra.vector.yt_vector_orchestrator import yt_vector_orchestrator
from FrozenMusic.infra.vector.yt_backup_engine import yt_backup_engine
from FrozenMusic.infra.chrono.chrono_formatter import quantum_temporal_humanizer
from FrozenMusic.vector_text_tools import vectorized_unicode_boldifier
from FrozenMusic.telegram_client.startup_hooks import precheck_channels

load_dotenv()


API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
ASSISTANT_SESSION = os.environ.get("ASSISTANT_SESSION")
OWNER_ID = int(os.getenv("OWNER_ID", "5268762773"))

# ——— Monkey-patch resolve_peer ——————————————
logging.getLogger("pyrogram").setLevel(logging.ERROR)
_original_resolve_peer = Client.resolve_peer
async def _safe_resolve_peer(self, peer_id):
    try:
        return await _original_resolve_peer(self, peer_id)
    except (KeyError, ValueError) as e:
        if "ID not found" in str(e) or "Peer id invalid" in str(e):
            return None
        raise
Client.resolve_peer = _safe_resolve_peer

# ——— Suppress un‐retrieved task warnings —————————
def _custom_exception_handler(loop, context):
    exc = context.get("exception")
    if isinstance(exc, (KeyError, ValueError)) and (
        "ID not found" in str(exc) or "Peer id invalid" in str(exc)
    ):
        return  

    if isinstance(exc, AttributeError) and "has no attribute 'write'" in str(exc):
        return

    loop.default_exception_handler(context)

asyncio.get_event_loop().set_exception_handler(_custom_exception_handler)

session_name = os.environ.get("SESSION_NAME", "music_bot1")
bot = Client(session_name, bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH)
assistant = Client("assistant_account", session_string=ASSISTANT_SESSION)
call_py = PyTgCalls(assistant)


ASSISTANT_USERNAME = None
ASSISTANT_CHAT_ID = None
API_ASSISTANT_USERNAME = os.getenv("API_ASSISTANT_USERNAME")


# ─── MongoDB Setup ─────────────────────────────────────────
mongo_uri = os.environ.get("MongoDB_url")
mongo_client = MongoClient(mongo_uri)
db = mongo_client["music_bot"]


broadcast_collection  = db["broadcast"]


state_backup = db["state_backup"]


chat_containers = {}
playback_tasks = {}  
bot_start_time = time.time()
COOLDOWN = 10
chat_last_command = {}
chat_pending_commands = {}
QUEUE_LIMIT = 20
MAX_DURATION_SECONDS = 480  
LOCAL_VC_LIMIT = 10
playback_mode = {}



async def process_pending_command(chat_id, delay):
    await asyncio.sleep(delay)  
    if chat_id in chat_pending_commands:
        message, cooldown_reply = chat_pending_commands.pop(chat_id)
        await cooldown_reply.delete()  
        await play_handler(bot, message) 



async def skip_to_next_song(chat_id, message):
    """Sıradaki şarkıya geçer ve çalmaya başlar."""
    if chat_id not in chat_containers or not chat_containers[chat_id]:
        await message.edit("❌ Sırada başka şarkı yok.")
        await leave_voice_chat(chat_id)
        return

    await message.edit("⏭ Sonraki şarkıya geçiliyor...")

    # Sıradaki şarkıyı seç
    next_song_info = chat_containers[chat_id][0]
    try:
        await fallback_local_playback(chat_id, message, next_song_info)
    except Exception as e:
        print(f"Error starting next local playback: {e}")
        await bot.send_message(chat_id, f"❌ Sonraki şarkı başlatılamadı: {e}")



def safe_handler(func):
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            # Attempt to extract a chat ID (if available)
            chat_id = "Unknown"
            try:
                # If your function is a message handler, the second argument is typically the Message object.
                if len(args) >= 2:
                    chat_id = args[1].chat.id
                elif "message" in kwargs:
                    chat_id = kwargs["message"].chat.id
            except Exception:
                chat_id = "Unknown"
            error_text = (
                f"Error in handler `{func.__name__}` (chat id: {chat_id}):\n\n{str(e)}"
            )
            print(error_text)
            # Log the error to support
            await bot.send_message(5268762773, error_text)
    return wrapper


async def extract_invite_link(client, chat_id):
    try:
        chat_info = await client.get_chat(chat_id)
        if chat_info.invite_link:
            return chat_info.invite_link
        elif chat_info.username:
            return f"https://t.me/{chat_info.username}"
        return None
    except ValueError as e:
        if "Peer id invalid" in str(e):
            print(f"Invalid peer ID for chat {chat_id}. Skipping invite link extraction.")
            return None
        else:
            raise e  # re-raise if it's another ValueError
    except Exception as e:
        print(f"Error extracting invite link for chat {chat_id}: {e}")
        return None

async def extract_target_user(message: Message):
    # If the moderator replied to someone:
    if message.reply_to_message:
        return message.reply_to_message.from_user.id

    # Otherwise expect an argument like "/ban @user" or "/ban 123456"
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("❌ Bir kullanıcıya yanıt vermeniz veya @kullanıcıadi/user_id belirtmeniz gerekiyor.")
        return None

    target = parts[1]
    # Strip @
    if target.startswith("@"):
        target = target[1:]
    try:
        user = await message._client.get_users(target)
        return user.id
    except:
        await message.reply("❌ Bu kullanıcı bulunamadı.")
        return None



async def is_assistant_in_chat(chat_id):
    try:
        member = await assistant.get_chat_member(chat_id, ASSISTANT_USERNAME)
        return member.status is not None
    except Exception as e:
        error_message = str(e)
        if "USER_BANNED" in error_message or "Banned" in error_message:
            return "banned"
        elif "USER_NOT_PARTICIPANT" in error_message or "Chat not found" in error_message:
            return False
        print(f"Error checking assistant in chat: {e}")
        return False

async def is_api_assistant_in_chat(chat_id):
    try:
        member = await bot.get_chat_member(chat_id, API_ASSISTANT_USERNAME)
        return member.status is not None
    except Exception as e:
        print(f"Error checking API assistant in chat: {e}")
        return False
    
def iso8601_to_seconds(iso_duration):
    try:
        duration = isodate.parse_duration(iso_duration)
        return int(duration.total_seconds())
    except Exception as e:
        print(f"Error parsing duration: {e}")
        return 0


def iso8601_to_human_readable(iso_duration):
    try:
        duration = isodate.parse_duration(iso_duration)
        total_seconds = int(duration.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}:{minutes:02}:{seconds:02}"
        return f"{minutes}:{seconds:02}"
    except Exception as e:
        return "Bilinmeyen süre"

async def fetch_youtube_link(query):
    try:
        url = f"https://search-api.kustbotsweb.workers.dev/search?q={query}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    # Check if the API response contains a playlist
                    if "playlist" in data:
                        return data
                    else:
                        return (
                            data.get("link"),
                            data.get("title"),
                            data.get("duration"),
                            data.get("thumbnail")
                        )
                else:
                    raise Exception(f"API returned status code {response.status}")
    except Exception as e:
        raise Exception(f"Failed to fetch YouTube link: {str(e)}")


    
async def fetch_youtube_link_backup(query):
    if not BACKUP_SEARCH_API_URL:
        raise Exception("Backup Search API URL not configured")
    # Build the correct URL:
    backup_url = (
        f"{BACKUP_SEARCH_API_URL.rstrip('/')}"
        f"/search?title={urllib.parse.quote(query)}"
    )
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(backup_url, timeout=30) as resp:
                if resp.status != 200:
                    raise Exception(f"Backup API returned status {resp.status}")
                data = await resp.json()
                # Mirror primary API’s return:
                if "playlist" in data:
                    return data
                return (
                    data.get("link"),
                    data.get("title"),
                    data.get("duration"),
                    data.get("thumbnail")
                )
    except Exception as e:
        raise Exception(f"Backup Search API error: {e}")
    
BOT_NAME = os.environ.get("BOT_NAME", "Frozen Music")
BOT_LINK = os.environ.get("BOT_LINK", "https://t.me/vcmusiclubot")

from pyrogram.errors import UserAlreadyParticipant, RPCError

async def invite_assistant(chat_id, invite_link, processing_message):
    """
    Asistanı sohbete dahil etmek için asistan istemcisini kullanarak sohbete katılır.
    Asistan zaten sohbet içindeyse başarılı olarak kabul edilir.
    Diğer hatalarda görüntülenir ve False döndürülür.
    """
    try:
        # Davet bağlantısı ile katılmayı dene
        await assistant.join_chat(invite_link)
        return True

    except UserAlreadyParticipant:
        # Asistan zaten sohbet içinde, başka işlem gerekmez
        return True

    except RPCError as e:
        # Diğer Pyrogram RPC hatalarını işle
        error_message = f"❌ Asistan davet edilirken hata: Telegram diyor ki: {e.code} {e.error_message}"
        await processing_message.edit(error_message)
        return False

    except Exception as e:
        # Beklenmeyen istisnalar için genel yakalama
        error_message = f"❌ Asistan davet edilirken beklenmeyen hata: {str(e)}"
        await processing_message.edit(error_message)
        return False


# Helper to convert ASCII letters to Unicode bold
def to_bold_unicode(text: str) -> str:
    bold_text = ""
    for char in text:
        if 'A' <= char <= 'Z':
            bold_text += chr(ord('𝗔') + (ord(char) - ord('A')))
        elif 'a' <= char <= 'z':
            bold_text += chr(ord('𝗮') + (ord(char) - ord('a')))
        else:
            bold_text += char
    return bold_text

@bot.on_message(filters.command("start"))
async def start_handler(_, message):
    user_id = message.from_user.id
    raw_name = message.from_user.first_name or ""
    styled_name = to_bold_unicode(raw_name)
    user_link = f"[{styled_name}](tg://user?id={user_id})"

    add_me_text = to_bold_unicode("Beni Ekle")
    updates_text = to_bold_unicode("Güncellemeler")
    support_text = to_bold_unicode("Destek")
    help_text = to_bold_unicode("Yardım")

    # Fetch from env with fallbacks
    updates_channel = os.getenv("UPDATES_CHANNEL", "https://t.me/vibeshiftbots")
    support_group = os.getenv("SUPPORT_GROUP", "https://t.me/Frozensupport1")
    start_animation = os.getenv(
        "START_ANIMATION",
        "https://frozen-imageapi.lagendplayersyt.workers.dev/file/2e483e17-05cb-45e2-b166-1ea476ce9521.mp4"
    )

    caption = (
        f"👋 Merhaba {user_link} 💠, 🥀\n\n"
        f">🎶 {BOT_NAME.upper()} MÜZİK BOTUNA HOŞ GELDİN! 🎵\n"
        ">🚀 7/24 AKTİF & YÜKSEK KALİTE DESTEĞİ\n"
        ">🔊 KRISTAL BERRAKLIĞINDA SES KALİTESİ\n"
        ">🎧 DESTEKLENEN PLATFORMLAR: YouTube | Spotify | Resso | Apple Music | SoundCloud\n"
        ">✨ SIRA BİTTİĞİNDE OTOMATİK ÖNERİLER\n"
        ">🛠️ YÖNETİCİ KOMUTLARI: Duraklat, Devam Et, Geç, Durdur, Sustur, Susturmayı Kaldır, Geçici Sustur, At, Yasakla, Yasak Kaldır, Çift\n"
        ">❤️ ÇİFT ÖNERİSİ (grupta rastgele iki kişi seçer)\n"
        f"๏ Komut listesi için aşağıdaki {help_text} butonuna tıkla."
    )

    buttons = [
        [
            InlineKeyboardButton(f"➕ {add_me_text}", url=f"{BOT_LINK}?startgroup=true"),
            InlineKeyboardButton(f"📢 {updates_text}", url=updates_channel)
        ],
        [
            InlineKeyboardButton(f"💬 {support_text}", url=support_group),
            InlineKeyboardButton(f"❓ {help_text}", callback_data="show_help")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(buttons)

    await message.reply_animation(
        animation=start_animation,
        caption=caption,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

    # Register chat ID for broadcasting silently
    chat_id = message.chat.id
    chat_type = message.chat.type
    if chat_type == ChatType.PRIVATE:
        if not broadcast_collection.find_one({"chat_id": chat_id}):
            broadcast_collection.insert_one({"chat_id": chat_id, "type": "private"})
    elif chat_type in [ChatType.GROUP, ChatType.SUPERGROUP]:
        if not broadcast_collection.find_one({"chat_id": chat_id}):
            broadcast_collection.insert_one({"chat_id": chat_id, "type": "group"})



@bot.on_callback_query(filters.regex("^go_back$"))
async def go_back_callback(_, callback_query):
    user_id = callback_query.from_user.id
    raw_name = callback_query.from_user.first_name or ""
    styled_name = to_bold_unicode(raw_name)
    user_link = f"[{styled_name}](tg://user?id={user_id})"

    add_me_text = to_bold_unicode("Beni Ekle")
    updates_text = to_bold_unicode("Güncellemeler")
    support_text = to_bold_unicode("Destek")
    help_text = to_bold_unicode("Yardım")

    updates_channel = os.getenv("UPDATES_CHANNEL", "https://t.me/vibeshiftbots")
    support_group = os.getenv("SUPPORT_GROUP", "https://t.me/Frozensupport1")

    caption = (
        f"👋 Merhaba {user_link} 💠, 🥀\n\n"
        f">🎶 {BOT_NAME.upper()} MÜZİK BOTUNA HOŞ GELDİN! 🎵\n"
        ">🚀 7/24 AKTİF & YÜKSEK KALİTE DESTEĞİ\n"
        ">🔊 KRISTAL BERRAKLIĞINDA SES KALİTESİ\n"
        ">🎧 DESTEKLENEN PLATFORMLAR: YouTube | Spotify | Resso | Apple Music | SoundCloud\n"
        ">✨ SIRA BİTTİĞİNDE OTOMATİK ÖNERİLER\n"
        ">🛠️ YÖNETİCİ KOMUTLARI: Duraklat, Devam Et, Geç, Durdur, Sustur, Susturmayı Kaldır, Geçici Sustur, At, Yasakla, Yasak Kaldır, Çift\n"
        ">❤️ ÇİFT ÖNERİSİ (grupta rastgele iki kişi seçer)\n"
        f"๏ Komut listesi için aşağıdaki {help_text} butonuna tıkla."
    )

    buttons = [
        [
            InlineKeyboardButton(f"➕ {add_me_text}", url=f"{BOT_LINK}?startgroup=true"),
            InlineKeyboardButton(f"📢 {updates_text}", url=updates_channel)
        ],
        [
            InlineKeyboardButton(f"💬 {support_text}", url=support_group),
            InlineKeyboardButton(f"❓ {help_text}", callback_data="show_help")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(buttons)

    await callback_query.message.edit_caption(
        caption=caption,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )


@bot.on_callback_query(filters.regex("^show_help$"))
async def show_help_callback(_, callback_query):
    help_text = ">📜 *Komutları keşfetmek için bir kategori seç:*"
    buttons = [
        [
            InlineKeyboardButton("🎵 Müzik Kontrolleri", callback_data="help_music"),
            InlineKeyboardButton("🛡️ Yönetici Araçları", callback_data="help_admin")
        ],
        [
            InlineKeyboardButton("❤️ Çift Önerisi", callback_data="help_couple"),
            InlineKeyboardButton("🔍 Yardımcı Araçlar", callback_data="help_util")
        ],
        [
            InlineKeyboardButton("🏠 Ana Sayfa", callback_data="go_back")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(buttons)
    await callback_query.message.edit_text(help_text, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)


@bot.on_callback_query(filters.regex("^help_music$"))
async def help_music_callback(_, callback_query):
    text = (
        ">🎵 *Müzik & Çalma Komutları*\n\n"
        ">➜ `/play <şarkı adı veya URL>`\n"
        "   • Bir şarkı çalar (YouTube/Spotify/Resso/Apple Music/SoundCloud).\n"
        "   • Bir ses/videoya yanıt verilirse, doğrudan onu çalar.\n\n"
        ">➜ `/playlist`\n"
        "   • Kayıtlı çalma listenizi görüntüleyin veya yönetin.\n\n"
        ">➜ `/skip`\n"
        "   • Şu anda çalan şarkıyı geçer. (Sadece Yöneticiler)\n\n"
        ">➜ `/pause`\n"
        "   • Geçerli akışı duraklatır. (Sadece Yöneticiler)\n\n"
        ">➜ `/resume`\n"
        "   • Duraklatılmış bir akışı devam ettirir. (Sadece Yöneticiler)\n\n"
        ">➜ `/stop` veya `/end`\n"
        "   • Çalmayı durdurur ve sırayı temizler. (Sadece Yöneticiler)"
    )
    buttons = [[InlineKeyboardButton("🔙 Geri", callback_data="show_help")]]
    await callback_query.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(buttons))


@bot.on_callback_query(filters.regex("^help_admin$"))
async def help_admin_callback(_, callback_query):
    text = (
        "🛡️ *Yönetici & Moderatör Komutları*\n\n"
        ">➜ `/mute @kullanıcı`\n"
        "   • Bir kullanıcıyı süresiz olarak susturur. (Sadece Yöneticiler)\n\n"
        ">➜ `/unmute @kullanıcı`\n"
        "   • Daha önce susturulmuş bir kullanıcının susturmasını kaldırır. (Sadece Yöneticiler)\n\n"
        ">➜ `/tmute @kullanıcı <dakika>`\n"
        "   • Belirli bir süre için geçici olarak susturur. (Sadece Yöneticiler)\n\n"
        ">➜ `/kick @kullanıcı`\n"
        "   • Bir kullanıcıyı anında atar (yasaklar ve yasağı kaldırır). (Sadece Yöneticiler)\n\n"
        ">➜ `/ban @kullanıcı`\n"
        "   • Bir kullanıcıyı yasaklar. (Sadece Yöneticiler)\n\n"
        ">➜ `/unban @kullanıcı`\n"
        "   • Daha önce yasaklanmış bir kullanıcının yasağını kaldırır. (Sadece Yöneticiler)"
    )
    buttons = [[InlineKeyboardButton("🔙 Geri", callback_data="show_help")]]
    await callback_query.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(buttons))


@bot.on_callback_query(filters.regex("^help_couple$"))
async def help_couple_callback(_, callback_query):
    text = (
        "❤️ *Çift Önerisi Komutu*\n\n"
        ">➜ `/couple`\n"
        "   • Rastgele iki bot olmayan üye seçer ve isimleriyle bir \"çift\" resmi paylaşır.\n"
        "   • Günlük olarak önbelleğe alır, böylece aynı çift UTC gece yarısına kadar görünür.\n"
        "   • Hız için grup bazlı üye önbelleği kullanır."
    )
    buttons = [[InlineKeyboardButton("🔙 Geri", callback_data="show_help")]]
    await callback_query.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(buttons))


@bot.on_callback_query(filters.regex("^help_util$"))
async def help_util_callback(_, callback_query):
    text = (
        "🔍 *Yardımcı & Ekstra Komutlar*\n\n"
        ">➜ `/ping`\n"
        "   • Botun yanıt süresini ve çalışma süresini kontrol eder.\n\n"
        ">➜ `/clear`\n"
        "   • Tüm sırayı temizler. (Sadece Yöneticiler)\n\n"
        ">➜ Otomatik Öneriler:\n"
        "   • Sıra bittiğinde, bot otomatik olarak yeni şarkıları inline butonlar aracılığıyla önerir.\n\n"
        ">➜ *Ses Kalitesi & Limitler*\n"
        "   • 2 saat 10 dakikaya kadar akış, ancak daha uzun süreliler için otomatik yedekleme. (`MAX_DURATION_SECONDS`'a bakın)\n"
    )
    buttons = [[InlineKeyboardButton("🔙 Geri", callback_data="show_help")]]
    await callback_query.message.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(buttons))


@bot.on_message(filters.group & filters.regex(r'^/play(?:@\w+)?(?:\s+(?P<query>.+))?$'))
async def play_handler(_, message: Message):
    chat_id = message.chat.id

    # If replying to an audio/video message, handle local playback
    if message.reply_to_message and (message.reply_to_message.audio or message.reply_to_message.video):
        processing_message = await message.reply("❄️")

        # Fetch fresh media reference and download
        orig = message.reply_to_message
        fresh = await bot.get_messages(orig.chat.id, orig.id)
        media = fresh.video or fresh.audio
        if fresh.audio and getattr(fresh.audio, 'file_size', 0) > 100 * 1024 * 1024:
            await processing_message.edit("❌ Ses dosyası çok büyük. Maksimum izin verilen boyut 100MB.")
            return

        await processing_message.edit("⏳ Lütfen bekleyin, ses indiriliyor…")
        try:
            file_path = await bot.download_media(media)
        except Exception as e:
            await processing_message.edit(f"❌ Medya indirilemedi: {e}")
            return

        # Download thumbnail if available
        thumb_path = None
        try:
            thumbs = fresh.video.thumbs if fresh.video else fresh.audio.thumbs
            thumb_path = await bot.download_media(thumbs[0])
        except Exception:
            pass

        # Prepare song_info and fallback to local playback
        duration = media.duration or 0
        title = getattr(media, 'file_name', 'İsimsiz')
        song_info = {
            'url': file_path,
            'title': title,
            'duration': format_time(duration),
            'duration_seconds': duration,
            'requester': message.from_user.first_name,
            'thumbnail': thumb_path
        }
        await fallback_local_playback(chat_id, processing_message, song_info)
        return

    # Otherwise, process query-based search
    match = message.matches[0]
    query = (match.group('query') or "").strip()

    try:
        await message.delete()
    except Exception:
        pass

    # Enforce cooldown
    now_ts = time.time()
    if chat_id in chat_last_command and (now_ts - chat_last_command[chat_id]) < COOLDOWN:
        remaining = int(COOLDOWN - (now_ts - chat_last_command[chat_id]))
        if chat_id in chat_pending_commands:
            await bot.send_message(chat_id, f"⏳ Bu sohbet için zaten bir komut sırada. Lütfen {remaining}s bekleyin.")
        else:
            cooldown_reply = await bot.send_message(chat_id, f"⏳ Bekleme süresinde. {remaining}s içinde işlenecek.")
            chat_pending_commands[chat_id] = (message, cooldown_reply)
            asyncio.create_task(process_pending_command(chat_id, remaining))
        return
    chat_last_command[chat_id] = now_ts

    if not query:
        await bot.send_message(
            chat_id,
            "❌ Bir şarkı belirtmediniz.\n\n"
            "Doğru kullanım: /play <şarkı adı>\nÖrnek: /play shape of you"
        )
        return

    # Delegate to query processor
    await process_play_command(message, query)



async def process_play_command(message: Message, query: str):
    chat_id = message.chat.id
    processing_message = await message.reply("❄️")

    # --- ensure assistant is in the chat before we queue/play anything ----
    status = await is_assistant_in_chat(chat_id)
    if status == "banned":
        await processing_message.edit("❌ Asistan bu gruptan yasaklanmış.")
        return
    if status is False:
        # try to fetch an invite link to add the assistant
        invite_link = await extract_invite_link(bot, chat_id)
        if not invite_link:
            await processing_message.edit("❌ Asistanı eklemek için bir davet bağlantısı alınamadı.")
            return
        invited = await invite_assistant(chat_id, invite_link, processing_message)
        if not invited:
            # invite_assistant handles error editing
            return

    # Convert short URLs to full YouTube URLs
    if "youtu.be" in query:
        m = re.search(r"youtu\.be/([^?&]+)", query)
        if m:
            query = f"https://www.youtube.com/watch?v={m.group(1)}"

    # Perform YouTube search and handle results
    try:
        result = await fetch_youtube_link(query)
    except Exception as primary_err:
        await processing_message.edit(
            "⚠️ Birincil arama başarısız. Yedek API kullanılıyor, bu birkaç saniye sürebilir…"
        )
        try:
            result = await fetch_youtube_link_backup(query)
        except Exception as backup_err:
            await processing_message.edit(
                f"❌ Her iki arama API'si de başarısız:\n"
                f"Birincil: {primary_err}\n"
                f"Yedek:  {backup_err}"
            )
            return

    # Handle playlist vs single video
    if isinstance(result, dict) and "playlist" in result:
        playlist_items = result["playlist"]
        if not playlist_items:
            await processing_message.edit("❌ Çalma listesinde video bulunamadı.")
            return

        chat_containers.setdefault(chat_id, [])
        for item in playlist_items:
            secs = isodate.parse_duration(item["duration"]).total_seconds()
            chat_containers[chat_id].append({
                "url": item["link"],
                "title": item["title"],
                "duration": iso8601_to_human_readable(item["duration"]),
                "duration_seconds": secs,
                "requester": message.from_user.first_name if message.from_user else "Bilinmeyen",
                "thumbnail": item["thumbnail"]
            })

        total = len(playlist_items)
        reply_text = (
            f"✨ Çalma listesine eklendi\n"
            f"Kuyruğa eklenen toplam şarkı: {total}\n"
            f"#1 - {playlist_items[0]['title']}"
        )
        if total > 1:
            reply_text += f"\n#2 - {playlist_items[1]['title']}"
        await message.reply(reply_text)

        # If first playlist song, start playback
        if len(chat_containers[chat_id]) == total:
            first_song_info = chat_containers[chat_id][0]
            await fallback_local_playback(chat_id, processing_message, first_song_info)
        else:
            await processing_message.delete()

    else:
        video_url, title, duration_iso, thumb = result
        if not video_url:
            await processing_message.edit(
                "❌ Şarkı bulunamadı. Başka bir sorgu deneyin.\nDestek: @frozensupport1"
            )
            return

        secs = isodate.parse_duration(duration_iso).total_seconds()
        if secs > MAX_DURATION_SECONDS:
            await processing_message.edit(
                "❌ 8 dakikadan uzun akışlara izin verilmiyor. Bu botun sahibiyseniz, planınızı yükseltmek için @xyz09723 ile iletişime geçin"
            )
            return

        readable = iso8601_to_human_readable(duration_iso)
        chat_containers.setdefault(chat_id, [])
        chat_containers[chat_id].append({
            "url": video_url,
            "title": title,
            "duration": readable,
            "duration_seconds": secs,
            "requester": message.from_user.first_name if message.from_user else "Bilinmeyen",
            "thumbnail": thumb
        })

        # If it's the first song, start playback immediately using fallback
        if len(chat_containers[chat_id]) == 1:
            await fallback_local_playback(chat_id, processing_message, chat_containers[chat_id][0])
        else:
            queue_buttons = InlineKeyboardMarkup([
                [InlineKeyboardButton("⏭ Geç", callback_data="skip"),
                 InlineKeyboardButton("🗑 Temizle", callback_data="clear")]
            ])
            await message.reply(
                f"✨ Sıraya eklendi :\n\n"
                f"**❍ Başlık ➥** {title}\n"
                f"**❍ Süre ➥** {readable}\n"
                f"**❍ İsteyen ➥ ** {message.from_user.first_name if message.from_user else 'Bilinmeyen'}\n"
                f"**Sıra numarası:** {len(chat_containers[chat_id]) - 1}",
                reply_markup=queue_buttons
            )
            await processing_message.delete()


# ─── Utility functions ──────────────────────────────────────────────────────────────

MAX_TITLE_LEN = 20

def _one_line_title(full_title: str) -> str:
    """
    Truncate `full_title` to at most MAX_TITLE_LEN chars.
    If truncated, append "…" so it still reads cleanly in one line.
    """
    if len(full_title) <= MAX_TITLE_LEN:
        return full_title
    else:
        return full_title[: (MAX_TITLE_LEN - 1) ] + "…"  # one char saved for the ellipsis

def parse_duration_str(duration_str: str) -> int:
    """
    Convert a duration string to total seconds.
    First, try ISO 8601 parsing (e.g. "PT3M9S"). If that fails,
    fall back to colon-separated formats like "3:09" or "1:02:30".
    """
    try:
        duration = isodate.parse_duration(duration_str)
        return int(duration.total_seconds())
    except Exception as e:
        if ':' in duration_str:
            try:
                parts = [int(x) for x in duration_str.split(':')]
                if len(parts) == 2:
                    minutes, seconds = parts
                    return minutes * 60 + seconds
                elif len(parts) == 3:
                    hours, minutes, seconds = parts
                    return hours * 3600 + minutes * 60 + seconds
            except Exception as e2:
                print(f"Error parsing colon-separated duration '{duration_str}': {e2}")
                return 0
        else:
            print(f"Error parsing duration '{duration_str}': {e}")
            return 0

def format_time(seconds: float) -> str:
    """
    Given total seconds, return "H:MM:SS" or "M:SS" if hours=0.
    """
    secs = int(seconds)
    m, s = divmod(secs, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}:{m:02d}:{s:02d}"
    else:
        return f"{m}:{s:02d}"

def get_progress_bar_styled(elapsed: float, total: float, bar_length: int = 14) -> str:
    """
    Build a progress bar string in the style:
      elapsed_time  <dashes>❄️<dashes>  total_time
    For example: 0:30 —❄️———— 3:09
    """
    if total <= 0:
        return "İlerleme: Yok"
    fraction = min(elapsed / total, 1)
    marker_index = int(fraction * bar_length)
    if marker_index >= bar_length:
        marker_index = bar_length - 1
    left = "━" * marker_index
    right = "─" * (bar_length - marker_index - 1)
    bar = left + "❄️" + right
    return f"{format_time(elapsed)} {bar} {format_time(total)}"


async def update_progress_caption(
    chat_id: int,
    progress_message: Message,
    start_time: float,
    total_duration: float,
    base_caption: str
):
    """
    Periodically update the inline keyboard so that the second row's button text
    shows the current progress bar. The caption remains `base_caption`.
    """
    while True:
        elapsed = time.time() - start_time
        if elapsed > total_duration:
            elapsed = total_duration
        progress_bar = get_progress_bar_styled(elapsed, total_duration)

        # Rebuild the keyboard with updated progress bar in the second row
        control_row = [
            InlineKeyboardButton(text="▷", callback_data="pause"),
            InlineKeyboardButton(text="II", callback_data="resume"),
            InlineKeyboardButton(text="‣‣I", callback_data="skip"),
            InlineKeyboardButton(text="▢", callback_data="stop")
        ]
        progress_button = InlineKeyboardButton(text=progress_bar, callback_data="progress")
        playlist_button = InlineKeyboardButton(text="➕ Çalma Listesine Ekle ➕", callback_data="add_to_playlist")

        new_keyboard = InlineKeyboardMarkup([
            control_row,
            [progress_button],
            [playlist_button]
        ])

        try:
            await bot.edit_message_caption(
                chat_id,
                progress_message.id,
                caption=base_caption,
                reply_markup=new_keyboard
            )
        except Exception as e:
            # Ignore MESSAGE_NOT_MODIFIED, otherwise break
            if "MESSAGE_NOT_MODIFIED" in str(e):
                pass
            else:
                print(f"Error updating progress caption for chat {chat_id}: {e}")
                break

        if elapsed >= total_duration:
            break

        await asyncio.sleep(18)



LOG_CHAT_ID = "@frozenmusiclogs"

async def fallback_local_playback(chat_id: int, message: Message, song_info: dict):
    playback_mode[chat_id] = "local"
    try:
        # Cancel any existing playback task
        if chat_id in playback_tasks:
            playback_tasks[chat_id].cancel()

        # Validate URL
        video_url = song_info.get("url")
        if not video_url:
            print(f"Invalid video URL for song: {song_info}")
            chat_containers[chat_id].pop(0)
            return

        # Notify
        try:
            await message.edit(f"⚡ {song_info['title']} için yerel çalma başlatılıyor...")
        except Exception:
            message = await bot.send_message(
                chat_id,
                f"⚡ {song_info['title']} için yerel çalma başlatılıyor..."
            )

        # Download & play locally
        media_path = await vector_transport_resolver(video_url)
        await call_py.play(
            chat_id,
            MediaStream(media_path, video_flags=MediaStream.Flags.IGNORE)
        )
        playback_tasks[chat_id] = asyncio.current_task()

        # Prepare caption & keyboard
        total_duration = parse_duration_str(song_info.get("duration", "0:00"))
        one_line = _one_line_title(song_info["title"])
        base_caption = (
            "<blockquote>"
            "<b>🎧 Frozen ✘ Music Akışı</b> (Yerel Çalma)\n\n"
            f"❍ <b>Başlık:</b> {one_line}\n"
            f"❍ <b>İsteyen:</b> {song_info['requester']}"
            "</blockquote>"
        )
        initial_progress = get_progress_bar_styled(0, total_duration)

        control_row = [
            InlineKeyboardButton(text="▷", callback_data="pause"),
            InlineKeyboardButton(text="II", callback_data="resume"),
            InlineKeyboardButton(text="‣‣I", callback_data="skip"),
            InlineKeyboardButton(text="▢", callback_data="stop"),
        ]
        progress_button = InlineKeyboardButton(text=initial_progress, callback_data="progress")
        base_keyboard = InlineKeyboardMarkup([control_row, [progress_button]])

        # Use raw thumbnail if available
        thumb_url = song_info.get("thumbnail")
        progress_message = await message.reply_photo(
            photo=thumb_url,
            caption=base_caption,
            reply_markup=base_keyboard,
            parse_mode=ParseMode.HTML
        )

        # Remove "processing" message
        await message.delete()

        # Kick off progress updates
        asyncio.create_task(
            update_progress_caption(
                chat_id,
                progress_message,
                time.time(),
                total_duration,
                base_caption
            )
        )

        # Log start
        asyncio.create_task(
            bot.send_message(
                LOG_CHAT_ID,
                "#akış_başlatıldı\n"
                f"• Başlık: {song_info.get('title','Bilinmeyen')}\n"
                f"• Süre: {song_info.get('duration','Bilinmeyen')}\n"
                f"• İsteyen: {song_info.get('requester','Bilinmeyen')}\n"
                f"• Mod: yerel"
            )
        )

    except Exception as e:
        print(f"Error during fallback local playback in chat {chat_id}: {e}")
        await bot.send_message(
            chat_id,
            f"❌ \"{song_info.get('title','Bilinmeyen')}\" yerel olarak çalınamadı: {e}"
        )

        if chat_id in chat_containers and chat_containers[chat_id]:
            chat_containers[chat_id].pop(0)




@bot.on_callback_query()
async def callback_query_handler(client, callback_query):
    chat_id = callback_query.message.chat.id
    user_id = callback_query.from_user.id
    data = callback_query.data
    user = callback_query.from_user

    # Check admin
    if not await deterministic_privilege_validator(callback_query):
        await callback_query.answer("❌ Bu butonu kullanmak için yönetici olmanız gerekiyor.", show_alert=True)
        return

    # ----------------- PAUSE -----------------
    if data == "pause":
        try:
            await call_py.pause(chat_id)
            await callback_query.answer("⏸ Çalma duraklatıldı.")
            await client.send_message(chat_id, f"⏸ Çalma {user.first_name} tarafından duraklatıldı.")
        except Exception as e:
            await callback_query.answer("❌ Çalma duraklatılırken hata oluştu.", show_alert=True)

    # ----------------- RESUME -----------------
    elif data == "resume":
        try:
            await call_py.resume(chat_id)
            await callback_query.answer("▶️ Çalma devam ettirildi.")
            await client.send_message(chat_id, f"▶️ Çalma {user.first_name} tarafından devam ettirildi.")
        except Exception as e:
            await callback_query.answer("❌ Çalma devam ettirilirken hata oluştu.", show_alert=True)

    # ----------------- SKIP -----------------
    elif data == "skip":
        if chat_id in chat_containers and chat_containers[chat_id]:
            skipped_song = chat_containers[chat_id].pop(0)

            try:
                await call_py.leave_call(chat_id)
            except Exception as e:
                print("Local leave_call error:", e)
            await asyncio.sleep(3)

            try:
                os.remove(skipped_song.get('file_path', ''))
            except Exception as e:
                print(f"Error deleting file: {e}")

            await client.send_message(chat_id, f"⏩ {user.first_name} **{skipped_song['title']}** şarkısını geçti.")

            if chat_id in chat_containers and chat_containers[chat_id]:
                await callback_query.answer("⏩ Geçildi! Sonraki şarkı çalınıyor...")

                # Play next song directly using fallback_local_playback
                next_song_info = chat_containers[chat_id][0]
                try:
                    dummy_msg = await bot.send_message(chat_id, f"🎧 Sonraki şarkı hazırlanıyor: **{next_song_info['title']}** ...")
                    await fallback_local_playback(chat_id, dummy_msg, next_song_info)
                except Exception as e:
                    print(f"Error starting next local playback: {e}")
                    await bot.send_message(chat_id, f"❌ Sonraki şarkı başlatılamadı: {e}")

            else:
                await callback_query.answer("⏩ Geçildi! Sırada başka şarkı yok.")
        else:
            await callback_query.answer("❌ Geçilecek şarkı yok.", show_alert=True)

    # ----------------- CLEAR -----------------
    elif data == "clear":
        if chat_id in chat_containers:
            for song in chat_containers[chat_id]:
                try:
                    os.remove(song.get('file_path', ''))
                except Exception as e:
                    print(f"Error deleting file: {e}")
            chat_containers.pop(chat_id)
            await callback_query.message.edit("🗑️ Sıra temizlendi.")
            await callback_query.answer("🗑️ Sıra temizlendi.")
        else:
            await callback_query.answer("❌ Temizlenecek şarkı yok.", show_alert=True)

    # ----------------- STOP -----------------
    elif data == "stop":
        if chat_id in chat_containers:
            for song in chat_containers[chat_id]:
                try:
                    os.remove(song.get('file_path', ''))
                except Exception as e:
                    print(f"Error deleting file: {e}")
            chat_containers.pop(chat_id)

        try:
            await call_py.leave_call(chat_id)
            await callback_query.answer("🛑 Çalma durduruldu ve sıra temizlendi.")
            await client.send_message(chat_id, f"🛑 Çalma durduruldu ve sıra {user.first_name} tarafından temizlendi.")
        except Exception as e:
            print("Stop error:", e)
            await callback_query.answer("❌ Çalma durdurulurken hata oluştu.", show_alert=True)




@call_py.on_update(fl.stream_end())
async def stream_end_handler(_: PyTgCalls, update: StreamEnded):
    chat_id = update.chat_id

    if chat_id in chat_containers and chat_containers[chat_id]:
        # Remove the finished song from the queue
        skipped_song = chat_containers[chat_id].pop(0)
        await asyncio.sleep(3)  # Delay to ensure the stream has fully ended

        try:
            os.remove(skipped_song.get('file_path', ''))
        except Exception as e:
            print(f"Error deleting file: {e}")

        if chat_id in chat_containers and chat_containers[chat_id]:
            # If there are more songs, play next song directly using fallback_local_playback
            next_song_info = chat_containers[chat_id][0]
            try:
                # Create a fake message object to pass
                dummy_msg = await bot.send_message(chat_id, f"🎧 Sonraki şarkı hazırlanıyor: **{next_song_info['title']}** ...")
                await fallback_local_playback(chat_id, dummy_msg, next_song_info)
            except Exception as e:
                print(f"Error starting next local playback: {e}")
                await bot.send_message(chat_id, f"❌ Sonraki şarkı başlatılamadı: {e}")
        else:
            # Queue empty; leave VC
            await leave_voice_chat(chat_id)
            await bot.send_message(chat_id, "❌ Sırada başka şarkı yok.")
    else:
        # No songs in the queue
        await leave_voice_chat(chat_id)
        await bot.send_message(chat_id, "❌ Sırada başka şarkı yok.")



async def leave_voice_chat(chat_id):
    try:
        await call_py.leave_call(chat_id)
    except Exception as e:
        print(f"Sesli sohbetten ayrılırken hata: {e}")

    if chat_id in chat_containers:
        for song in chat_containers[chat_id]:
            try:
                os.remove(song.get('file_path', ''))
            except Exception as e:
                print(f"Error deleting file: {e}")
        chat_containers.pop(chat_id)

    if chat_id in playback_tasks:
        playback_tasks[chat_id].cancel()
        del playback_tasks[chat_id]



@bot.on_message(filters.group & filters.command(["stop", "end"]))
async def stop_handler(client, message):
    chat_id = message.chat.id

    # Check admin rights
    if not await deterministic_privilege_validator(message):
        await message.reply("❌ Bu komutu kullanmak için yönetici olmanız gerekiyor.")
        return

    try:
        await call_py.leave_call(chat_id)
    except Exception as e:
        if "not in a call" in str(e).lower():
            await message.reply("❌ Bot şu anda bir sesli sohbet içinde değil.")
        else:
            await message.reply(f"❌ Sesli sohbetten ayrılırken bir hata oluştu: {str(e)}\n\nDestek: @frozensupport1")
        return

    # Clear the song queue
    if chat_id in chat_containers:
        for song in chat_containers[chat_id]:
            try:
                os.remove(song.get('file_path', ''))
            except Exception as e:
                print(f"Error deleting file: {e}")
        chat_containers.pop(chat_id)

    # Cancel any playback tasks if present
    if chat_id in playback_tasks:
        playback_tasks[chat_id].cancel()
        del playback_tasks[chat_id]

    await message.reply("⏹ Müzik durduruldu ve sıra temizlendi.")


@bot.on_message(filters.command("song"))
async def song_command_handler(_, message):
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("🎶 Şimdi İndir", url="https://t.me/songdownloader1bot?start=true")]]
    )
    text = (
        "Şarkı indirici botu kullanmak için aşağıdaki butona tıklayın. 🎵\n\n"
        "Şarkı adını veya herhangi bir sorguyu doğrudan indirici bota gönderebilirsiniz, ⬇️\n\n"
        "ve o sizin için şarkıyı getirip indirecektir. 🚀"
    )
    await message.reply(text, reply_markup=keyboard)



@bot.on_message(filters.group & filters.command("pause"))
async def pause_handler(client, message):
    chat_id = message.chat.id

    if not await deterministic_privilege_validator(message):
        await message.reply("❌ Bu komutu kullanmak için yönetici olmanız gerekiyor.")
        return

    try:
        await call_py.pause(chat_id)
        await message.reply("⏸ Akış duraklatıldı.")
    except Exception as e:
        await message.reply(f"❌ Akış duraklatılamadı.\nHata: {str(e)}")


@bot.on_message(filters.group & filters.command("resume"))
async def resume_handler(client, message):
    chat_id = message.chat.id

    if not await deterministic_privilege_validator(message):
        await message.reply("❌ Bu komutu kullanmak için yönetici olmanız gerekiyor.")
        return

    try:
        await call_py.resume(chat_id)
        await message.reply("▶️ Akış devam ettirildi.")
    except Exception as e:
        await message.reply(f"❌ Akış devam ettirilemedi.\nHata: {str(e)}")



@bot.on_message(filters.group & filters.command("skip"))
async def skip_handler(client, message):
    chat_id = message.chat.id

    if not await deterministic_privilege_validator(message):
        await message.reply("❌ Bu komutu kullanmak için yönetici olmanız gerekiyor.")
        return

    status_message = await message.reply("⏩ Geçerli şarkı geçiliyor...")

    if chat_id not in chat_containers or not chat_containers[chat_id]:
        await status_message.edit("❌ Geçilecek şarkı yok.")
        return

    # Remove the current song from the queue
    skipped_song = chat_containers[chat_id].pop(0)

    # Always local mode only
    try:
        await call_py.leave_call(chat_id)
    except Exception as e:
        print("Local leave_call error:", e)

    await asyncio.sleep(3)

    # Delete the local file if exists
    try:
        if skipped_song.get('file_path'):
            os.remove(skipped_song['file_path'])
    except Exception as e:
        print(f"Error deleting file: {e}")

    # Check for next song
    if not chat_containers.get(chat_id):
        await status_message.edit(
            f"⏩ **{skipped_song['title']}** geçildi.\n\n😔 Sırada başka şarkı yok."
        )
    else:
        await status_message.edit(
            f"⏩ **{skipped_song['title']}** geçildi.\n\n💕 Sonraki şarkı çalınıyor..."
        )
        await skip_to_next_song(chat_id, status_message)




@bot.on_message(filters.command("reboot"))
async def reboot_handler(_, message):
    chat_id = message.chat.id

    try:
        # Remove audio files for songs in the queue for this chat.
        if chat_id in chat_containers:
            for song in chat_containers[chat_id]:
                try:
                    os.remove(song.get('file_path', ''))
                except Exception as e:
                    print(f"Error deleting file for chat {chat_id}: {e}")
            # Clear the queue for this chat.
            chat_containers.pop(chat_id, None)
        
        # Cancel any playback tasks for this chat.
        if chat_id in playback_tasks:
            playback_tasks[chat_id].cancel()
            del playback_tasks[chat_id]

        # Remove chat-specific cooldown and pending command entries.
        chat_last_command.pop(chat_id, None)
        chat_pending_commands.pop(chat_id, None)

        # Remove playback mode for this chat.
        playback_mode.pop(chat_id, None)

        # Clear any API playback records for this chat.
        global api_playback_records
        api_playback_records = [record for record in api_playback_records if record.get("chat_id") != chat_id]

        # Leave the voice chat for this chat.
        try:
            await call_py.leave_call(chat_id)
        except Exception as e:
            print(f"Error leaving call for chat {chat_id}: {e}")

        await message.reply("♻️ Bu sohbet için yeniden başlatıldı. Bu sohbet için tüm veriler temizlendi.")
    except Exception as e:
        await message.reply(f"❌ Bu sohbet için yeniden başlatılamadı. Hata: {str(e)}\n\n destek - @frozensupport1")



@bot.on_message(filters.command("ping"))
async def ping_handler(_, message):
    try:
        # Calculate uptime
        current_time = time.time()
        uptime_seconds = int(current_time - bot_start_time)
        uptime_str = str(timedelta(seconds=uptime_seconds))

        # Local system stats
        cpu_usage = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        ram_usage = f"{memory.used // (1024 ** 2)}MB / {memory.total // (1024 ** 2)}MB ({memory.percent}%)"
        disk = psutil.disk_usage('/')
        disk_usage = f"{disk.used // (1024 ** 3)}GB / {disk.total // (1024 ** 3)}GB ({disk.percent}%)"

        # Build the final message
        response = (
            f"🏓 **Pong!**\n\n"
            f"**Yerel Sunucu İstatistikleri:**\n"
            f"• **Çalışma Süresi:** `{uptime_str}`\n"
            f"• **CPU Kullanımı:** `{cpu_usage}%`\n"
            f"• **RAM Kullanımı:** `{ram_usage}`\n"
            f"• **Disk Kullanımı:** `{disk_usage}`"
        )

        await message.reply(response)
    except Exception as e:
        await message.reply(f"❌ Komut çalıştırılamadı.\nHata: {str(e)}\n\nDestek: @frozensupport1")




@bot.on_message(filters.group & filters.command("clear"))
async def clear_handler(_, message):
    chat_id = message.chat.id

    if chat_id in chat_containers:
        # Clear the chat-specific queue
        for song in chat_containers[chat_id]:
            try:
                os.remove(song.get('file_path', ''))
            except Exception as e:
                print(f"Error deleting file: {e}")
        
        chat_containers.pop(chat_id)
        await message.reply("🗑️ Sıra temizlendi.")
    else:
        await message.reply("❌ Temizlenecek şarkı yok.")


@bot.on_message(filters.command("broadcast") & filters.user(OWNER_ID))
async def broadcast_handler(_, message):
    # Ensure the command is used in reply to a message
    if not message.reply_to_message:
        await message.reply("❌ Lütfen yayınlamak istediğiniz mesaja yanıt verin.")
        return

    broadcast_message = message.reply_to_message

    # Retrieve all broadcast chat IDs from the collection
    all_chats = list(broadcast_collection.find({}))
    success = 0
    failed = 0

    # Loop through each chat ID and forward the message
    for chat in all_chats:
        try:
            # Ensure the chat ID is an integer (this will handle group IDs properly)
            target_chat_id = int(chat.get("chat_id"))
        except Exception as e:
            print(f"Error casting chat_id: {chat.get('chat_id')} - {e}")
            failed += 1
            continue

        try:
            await bot.forward_messages(
                chat_id=target_chat_id,
                from_chat_id=broadcast_message.chat.id,
                message_ids=broadcast_message.id
            )
            success += 1
        except Exception as e:
            print(f"Failed to broadcast to {target_chat_id}: {e}")
            failed += 1

        # Wait for 1 second to avoid flooding the server and Telegram
        await asyncio.sleep(1)

    await message.reply(f"Yayın tamamlandı!\n✅ Başarılı: {success}\n❌ Başarısız: {failed}")



@bot.on_message(filters.command("frozen_check"))
async def frozen_check_command(client: Client, message):
    await message.reply_text("frozen kontrolü başarılı ✨")



def save_state_to_db():
    """
    Persist only chat_containers (queues) into MongoDB before restart.
    """
    data = {
        "chat_containers": { str(cid): queue for cid, queue in chat_containers.items() }
    }

    state_backup.replace_one(
        {"_id": "singleton"},
        {"_id": "singleton", "state": data},
        upsert=True
    )

    chat_containers.clear()


def load_state_from_db():
    """
    Load persisted chat_containers (queues) from MongoDB on startup.
    """
    doc = state_backup.find_one_and_delete({"_id": "singleton"})
    if not doc or "state" not in doc:
        return

    data = doc["state"]

    for cid_str, queue in data.get("chat_containers", {}).items():
        try:
            chat_containers[int(cid_str)] = queue
        except ValueError:
            continue



logger = logging.getLogger(__name__)

RESTART_CHANNEL_ID = -1001849376366  # Your channel/chat ID

async def heartbeat():
    while True:
        await asyncio.sleep(3 * 3600)  # every 10 hours
        try:
            logger.info("💤 Heartbeat: performing full restart to prevent MTProto freeze...")

            # Notify channel before restart
            pre_msg = None
            try:
                pre_msg = await bot.send_message(RESTART_CHANNEL_ID, "⚡ Bot yeniden başlatılıyor (planlı heartbeat)")
            except Exception as e:
                logger.warning(f"Failed to notify channel about restart: {e}")

            # Save state to DB
            save_state_to_db()
            logger.info("✅ Bot state saved to DB")

            # Fully restart the process (like /restart endpoint)
            os.execl(sys.executable, sys.executable, *sys.argv)

        except Exception as e:
            logger.error(f"❌ Heartbeat restart failed: {e}")

# ─── Main Entry ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("Loading persisted state from MongoDB...")
    load_state_from_db()
    logger.info("State loaded successfully.")

    logger.info("→ Starting PyTgCalls client...")
    call_py.start()
    logger.info("PyTgCalls client started.")

    logger.info("→ Starting Telegram bot client (bot.start)...")
    try:
        bot.start()
        logger.info("Telegram bot has started.")
    except Exception as e:
        logger.error(f"❌ Failed to start Pyrogram client: {e}")
        sys.exit(1)

    me = bot.get_me()
    BOT_NAME = me.first_name or "Frozen Music"
    BOT_USERNAME = me.username or os.getenv("BOT_USERNAME", "vcmusiclubot")
    BOT_LINK = f"https://t.me/{BOT_USERNAME}"

    logger.info(f"✅ Bot Name: {BOT_NAME!r}")
    logger.info(f"✅ Bot Username: {BOT_USERNAME}")
    logger.info(f"✅ Bot Link: {BOT_LINK}")

    if not assistant.is_connected:
        logger.info("Assistant not connected; starting assistant client...")
        assistant.start()
        logger.info("Assistant client connected.")

    try:
        assistant_user = assistant.get_me()
        ASSISTANT_USERNAME = assistant_user.username
        ASSISTANT_CHAT_ID = assistant_user.id
        logger.info(f"✨ Assistant Username: {ASSISTANT_USERNAME}")
        logger.info(f"💕 Assistant Chat ID: {ASSISTANT_CHAT_ID}")

        asyncio.get_event_loop().run_until_complete(precheck_channels(assistant))
        logger.info("✅ Assistant precheck completed.")

    except Exception as e:
        logger.error(f"❌ Failed to fetch assistant info: {e}")

    # Start the heartbeat task
    logger.info("→ Starting heartbeat task (auto-restart every 2.5 hours)")
    asyncio.get_event_loop().create_task(heartbeat())

    logger.info("→ Entering idle() (long-polling)")
    idle()  # keep the bot alive

    try:
        bot.stop()
        logger.info("Bot stopped.")
    except Exception as e:
        logger.warning(f"Bot stop failed or already stopped: {e}")

    logger.info("✅ All services are up and running. Bot started successfully.")
