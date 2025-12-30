import sqlite3
import asyncio
from datetime import datetime, timedelta
from telegram import Update, Bot, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.error import TelegramError, TimedOut, NetworkError
import logging
from contextlib import contextmanager
import sys
import re
import os
import pytz

# TIMEZONE CONFIGURATION - All times stored in UTC, displayed in IST
IST = pytz.timezone('Asia/Kolkata')

if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# UTC HELPER FUNCTIONS
def utc_now():
    """Get current UTC time (naive)"""
    return datetime.utcnow()

def ist_to_utc(ist_dt):
    """Convert IST naive datetime to UTC naive datetime"""
    ist_aware = IST.localize(ist_dt) if ist_dt.tzinfo is None else ist_dt
    utc_aware = ist_aware.astimezone(pytz.UTC)
    return utc_aware.replace(tzinfo=None)

def utc_to_ist(utc_dt):
    """Convert UTC naive datetime to IST naive datetime"""
    utc_aware = pytz.UTC.localize(utc_dt) if utc_dt.tzinfo is None else utc_dt
    ist_aware = utc_aware.astimezone(IST)
    return ist_aware.replace(tzinfo=None)

def get_ist_now():
    """Get current time in IST (naive)"""
    return utc_to_ist(utc_now())


class ThreeModeScheduler:
    def __init__(self, bot_token, admin_id, db_path='posts.db', auto_cleanup_minutes=30):
        self.bot_token = bot_token
        self.admin_id = admin_id
        self.db_path = db_path
        self.auto_cleanup_minutes = auto_cleanup_minutes
        self.channel_ids = []
        self.init_database()
        self.load_channels()
        self.user_sessions = {}
        self.posting_lock = asyncio.Lock()
        self.stats = {'restarts': 0}
    
    @contextmanager
    def get_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def init_database(self):
        with self.get_db() as conn:
            c = conn.cursor()
            
            c.execute('''
                CREATE TABLE IF NOT EXISTS posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT,
                    media_type TEXT,
                    media_file_id TEXT,
                    caption TEXT,
                    scheduled_time TIMESTAMP NOT NULL,
                    posted INTEGER DEFAULT 0,
                    total_channels INTEGER DEFAULT 0,
                    successful_posts INTEGER DEFAULT 0,
                    posted_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            c.execute('''
                CREATE TABLE IF NOT EXISTS channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_id TEXT UNIQUE NOT NULL,
                    channel_name TEXT,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    active INTEGER DEFAULT 1
                )
            ''')
            
            c.execute('CREATE INDEX IF NOT EXISTS idx_scheduled_posted ON posts(scheduled_time, posted)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_posted_at ON posts(posted_at)')
            c.execute('CREATE INDEX IF NOT EXISTS idx_channel_active ON channels(active)')
            
            conn.commit()
            logger.info(f"✅ Database initialized")
    
    def load_channels(self):
        with self.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT channel_id FROM channels WHERE active = 1')
            self.channel_ids = [row[0] for row in c.fetchall()]
        logger.info(f"📢 Loaded {len(self.channel_ids)} active channels")
    
    def add_channel(self, channel_id, channel_name=None):
        with self.get_db() as conn:
            c = conn.cursor()
            try:
                c.execute('INSERT INTO channels (channel_id, channel_name, active) VALUES (?, ?, 1)',
                         (channel_id, channel_name))
                conn.commit()
                self.load_channels()
                logger.info(f"✅ Added channel: {channel_id}")
                return True
            except sqlite3.IntegrityError:
                c.execute('UPDATE channels SET active = 1 WHERE channel_id = ?', (channel_id,))
                conn.commit()
                self.load_channels()
                return True
    
    def remove_channel(self, channel_id):
        with self.get_db() as conn:
            c = conn.cursor()
            c.execute('UPDATE channels SET active = 0 WHERE channel_id = ?', (channel_id,))
            deleted = c.rowcount > 0
            conn.commit()
            if deleted:
                self.load_channels()
                logger.info(f"🗑️ Removed channel: {channel_id}")
            return deleted
    
    def get_all_channels(self):
        with self.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT channel_id, channel_name, active, added_at FROM channels ORDER BY added_at DESC')
            return c.fetchall()
    
    def schedule_post(self, scheduled_time_utc, message=None, media_type=None, 
                     media_file_id=None, caption=None):
        """Schedule a post. scheduled_time_utc MUST be UTC datetime"""
        with self.get_db() as conn:
            c = conn.cursor()
            c.execute('''
                INSERT INTO posts (message, media_type, media_file_id, caption, 
                                 scheduled_time, total_channels)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (message, media_type, media_file_id, caption, 
                  scheduled_time_utc.isoformat(), len(self.channel_ids)))
            conn.commit()
            return c.lastrowid
    
    
    async def send_to_all_channels(self, bot, post):
        successful = 0
    
    # Helper function to send to one channel with retry
        async def send_to_channel(channel_id, max_retries=5):
            for attempt in range(max_retries):
                try:
                    if post['media_type'] == 'photo':
                        await bot.send_photo(
                            chat_id=channel_id, 
                            photo=post['media_file_id'], 
                            caption=post['caption'],
                            read_timeout=60,
                            write_timeout=60,
                            connect_timeout=60
                    )
                    elif post['media_type'] == 'video':
                        await bot.send_video(
                            chat_id=channel_id, 
                            video=post['media_file_id'], 
                            caption=post['caption'],
                            read_timeout=60,
                            write_timeout=60,
                            connect_timeout=60
                        )
                    elif post['media_type'] == 'document':
                        await bot.send_document(
                            chat_id=channel_id, 
                            document=post['media_file_id'], 
                            caption=post['caption'],
                            read_timeout=60,
                            write_timeout=60,
                            connect_timeout=60
                        )
                    else:
                        await bot.send_message(
                            chat_id=channel_id, 
                            text=post['message'],
                            read_timeout=60,
                            write_timeout=60,
                            connect_timeout=60
                        )
                    return True
                except (TimedOut, NetworkError) as e:
                     if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 3  # 3, 6, 9, 12, 15 seconds
                        logger.warning(f"⏳ Retry {attempt+1}/{max_retries} for {channel_id} in {wait_time}s: {e}")
                        await asyncio.sleep(wait_time)
                     else:
                        logger.error(f"❌ Failed {channel_id} after {max_retries} attempts: {e}")
                        return False
                except TelegramError as e:
                    logger.error(f"❌ Failed channel {channel_id}: {e}")
                    return False    
    # Send to channels in batches of 20 to avoid rate limits
        batch_size = 20
        for i in range(0, len(self.channel_ids), batch_size):
            batch = self.channel_ids[i:i + batch_size]
            tasks = [send_to_channel(ch_id) for ch_id in batch]
            results = await asyncio.gather(*tasks)
            successful += sum(results)
        
        # Delay between batches to respect rate limits
            if i + batch_size < len(self.channel_ids):
                await asyncio.sleep(2.0)  # ✅ Changed from 0.5 to 2.0
    
        with self.get_db() as conn:
            c = conn.cursor()
            c.execute('UPDATE posts SET posted = 1, posted_at = ?, successful_posts = ? WHERE id = ?',
                     (datetime.utcnow().isoformat(), successful, post['id']))
            conn.commit()
    
        logger.info(f"📊 Post {post['id']}: {successful}/{len(self.channel_ids)} channels")
        return successful

    async def process_due_posts(self, bot):
        """Check for posts due (UTC comparison)"""
        async with self.posting_lock:
            with self.get_db() as conn:
                c = conn.cursor()
                now_utc = datetime.utcnow().isoformat()
                c.execute('SELECT * FROM posts WHERE scheduled_time <= ? AND posted = 0 ORDER BY scheduled_time LIMIT 200',
                         (now_utc,))
                posts = c.fetchall()
            
            for post in posts:
                await self.send_to_all_channels(bot, post)
                await asyncio.sleep(1)

    
    
    def cleanup_posted_content(self):
        with self.get_db() as conn:
            c = conn.cursor()
            cutoff = (datetime.utcnow() - timedelta(minutes=self.auto_cleanup_minutes)).isoformat()
            
            c.execute('SELECT COUNT(*) FROM posts WHERE posted = 1 AND posted_at < ?', (cutoff,))
            count_to_delete = c.fetchone()[0]
            
            if count_to_delete > 0:
                c.execute('DELETE FROM posts WHERE posted = 1 AND posted_at < ?', (cutoff,))
                conn.commit()
                c.execute('VACUUM')
                
                c.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
                db_size = c.fetchone()[0] / 1024 / 1024
                
                logger.info(f"🧹 Auto-cleanup: Removed {count_to_delete} old posts | DB size: {db_size:.2f} MB")
                return count_to_delete
            return 0
    
    def get_pending_posts(self):
        with self.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT * FROM posts WHERE posted = 0 ORDER BY scheduled_time')
            return c.fetchall()
    
    def get_database_stats(self):
        with self.get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT COUNT(*) FROM posts')
            total_posts = c.fetchone()[0]
            c.execute('SELECT COUNT(*) FROM posts WHERE posted = 0')
            pending_posts = c.fetchone()[0]
            c.execute('SELECT COUNT(*) FROM posts WHERE posted = 1')
            posted_posts = c.fetchone()[0]
            c.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
            db_size = c.fetchone()[0] / 1024 / 1024
            
            return {'total': total_posts, 'pending': pending_posts, 'posted': posted_posts, 'db_size_mb': db_size}
    
    def delete_post(self, post_id):
        with self.get_db() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM posts WHERE id = ?', (post_id,))
            conn.commit()
            return c.rowcount > 0


scheduler = None

# KEYBOARD FUNCTIONS
def get_mode_keyboard():
    keyboard = [
        [KeyboardButton("📦 Bulk Posts (Auto-Space)")],
        [KeyboardButton("🎯 Bulk Posts (Batches)")],
        [KeyboardButton("📅 Exact Time/Date")],
        [KeyboardButton("⏱️ Duration (Wait Time)")],
        [KeyboardButton("📋 View Pending"), KeyboardButton("📊 Stats")],
        [KeyboardButton("📢 Channels"), KeyboardButton("❌ Cancel")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_bulk_collection_keyboard():
    keyboard = [
        [KeyboardButton("✅ Done - Schedule All Posts")],
        [KeyboardButton("❌ Cancel")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_confirmation_keyboard():
    keyboard = [
        [KeyboardButton("✅ Confirm & Schedule")],
        [KeyboardButton("❌ Cancel")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_duration_keyboard():
    keyboard = [
        [KeyboardButton("2h"), KeyboardButton("6h"), KeyboardButton("12h")],
        [KeyboardButton("1d"), KeyboardButton("today")],
        [KeyboardButton("❌ Cancel")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_quick_time_keyboard():
    keyboard = [
        [KeyboardButton("5m"), KeyboardButton("30m"), KeyboardButton("1h")],
        [KeyboardButton("2h"), KeyboardButton("now")],
        [KeyboardButton("❌ Cancel")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_exact_time_keyboard():
    keyboard = [
        [KeyboardButton("today 18:00"), KeyboardButton("tomorrow 9am")],
        [KeyboardButton("❌ Cancel")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_batch_size_keyboard():
    keyboard = [
        [KeyboardButton("10"), KeyboardButton("20"), KeyboardButton("30")],
        [KeyboardButton("50"), KeyboardButton("100")],
        [KeyboardButton("❌ Cancel")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)


# TIME PARSING FUNCTIONS (User inputs in IST)
def parse_duration_to_minutes(text):
    text = text.strip().lower()
    
    if text == 'today':
        now = get_ist_now()
        midnight = datetime.combine(now.date() + timedelta(days=1), datetime.min.time())
        return int((midnight - now).total_seconds() / 60)
    
    if text[-1] == 'm':
        return int(text[:-1])
    elif text[-1] == 'h':
        return int(text[:-1]) * 60
    elif text[-1] == 'd':
        return int(text[:-1]) * 1440
    
    raise ValueError("Invalid format")

def parse_user_time_input(text):
    """Parse user time input (assumes IST) and return IST datetime"""
    text = text.strip().lower()
    now_ist = get_ist_now()
    
    if text == 'now':
        return now_ist
    
    # Duration format (30m, 2h, 1d)
    if text[-1] in ['m', 'h', 'd']:
        if text[-1] == 'm':
            return now_ist + timedelta(minutes=int(text[:-1]))
        elif text[-1] == 'h':
            return now_ist + timedelta(hours=int(text[:-1]))
        elif text[-1] == 'd':
            return now_ist + timedelta(days=int(text[:-1]))
    
    # "tomorrow" keyword
    if text.startswith('tomorrow'):
        tomorrow = now_ist + timedelta(days=1)
        time_part = text.replace('tomorrow', '').strip()
        if time_part:
            hour = parse_hour(time_part)
            return datetime.combine(tomorrow.date(), datetime.min.time()) + timedelta(hours=hour)
        return tomorrow
    
    # "today" keyword
    if text.startswith('today'):
        time_part = text.replace('today', '').strip()
        if time_part:
            hour = parse_hour(time_part)
            return datetime.combine(now_ist.date(), datetime.min.time()) + timedelta(hours=hour)
    
    # Exact date-time formats
    try:
        return datetime.strptime(text, '%Y-%m-%d %H:%M')
    except:
        pass
    
    try:
        dt = datetime.strptime(text, '%m/%d %H:%M')
        return dt.replace(year=now_ist.year)
    except:
        pass
    
    raise ValueError("Invalid format! Use: 2025-12-31 23:59 or 12/31 23:59 or tomorrow 9am")

def parse_hour(text):
    text = text.strip().lower()
    
    if 'am' in text or 'pm' in text:
        hour = int(re.findall(r'\d+', text)[0])
        if 'pm' in text and hour != 12:
            hour += 12
        if 'am' in text and hour == 12:
            hour = 0
        return hour
    
    if ':' in text:
        return int(text.split(':')[0])
    
    return int(text)


# COMMAND HANDLERS
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != scheduler.admin_id:
        return
    
    user_id = update.effective_user.id
    scheduler.user_sessions[user_id] = {'mode': None, 'step': 'choose_mode'}
    
    stats = scheduler.get_database_stats()
    ist_now = get_ist_now()
    
    await update.message.reply_text(
        "🤖 <b>Telegram Multi-Channel Scheduler</b>\n\n"
        f"🕐 Current Time (IST): <b>{ist_now.strftime('%Y-%m-%d %H:%M:%S')}</b>\n"
        f"📢 Managing {len(scheduler.channel_ids)} channels\n"
        f"📊 Pending: {stats['pending']} | DB: {stats['db_size_mb']:.2f} MB\n"
        f"🧹 Auto-cleanup: {scheduler.auto_cleanup_minutes} min after posting\n\n"
        "<b>Choose a mode:</b>",
        reply_markup=get_mode_keyboard(),
        parse_mode='HTML'
    )

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != scheduler.admin_id:
        return
    
    stats = scheduler.get_database_stats()
    ist_now = get_ist_now()
    
    response = "📊 <b>DATABASE STATISTICS</b>\n\n"
    response += f"🕐 Current Time (IST): <b>{ist_now.strftime('%Y-%m-%d %H:%M:%S')}</b>\n\n"
    response += f"📦 Total Posts: <b>{stats['total']}</b>\n"
    response += f"⏳ Pending: <b>{stats['pending']}</b>\n"
    response += f"✅ Posted (awaiting cleanup): <b>{stats['posted']}</b>\n"
    response += f"💾 Database Size: <b>{stats['db_size_mb']:.2f} MB</b>\n"
    response += f"📢 Active Channels: <b>{len(scheduler.channel_ids)}</b>\n\n"
    response += f"🧹 Auto-cleanup runs every 30 seconds\n"
    response += f"⏰ Posted content removed after <b>{scheduler.auto_cleanup_minutes} minutes</b>\n"
    
    await update.message.reply_text(response, reply_markup=get_mode_keyboard(), parse_mode='HTML')

async def channels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != scheduler.admin_id:
        return
    
    channels = scheduler.get_all_channels()
    
    if not channels:
        await update.message.reply_text(
            "📢 <b>No channels configured!</b>\n\n"
            "Use /addchannel to add your first channel.\n\n"
            "<b>Usage:</b>\n"
            "<code>/addchannel -1001234567890</code>\n"
            "<code>/addchannel -1001234567890 My Channel Name</code>",
            reply_markup=get_mode_keyboard(),
            parse_mode='HTML'
        )
        return
    
    response = f"📢 <b>CHANNELS ({len(channels)} total)</b>\n\n"
    
    active_count = 0
    for channel in channels:
        status = "✅" if channel['active'] else "❌"
        name = channel['channel_name'] or "Unnamed"
        response += f"{status} <code>{channel['channel_id']}</code>\n"
        response += f"   📝 {name}\n\n"
        
        if channel['active']:
            active_count += 1
    
    response += f"<b>Active:</b> {active_count} | <b>Inactive:</b> {len(channels) - active_count}\n\n"
    response += "<b>Commands:</b>\n"
    response += "• /addchannel [id] [name] - Add channel\n"
    response += "• /removechannel [id] - Remove channel\n"
    
    await update.message.reply_text(response, reply_markup=get_mode_keyboard(), parse_mode='HTML')

async def add_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != scheduler.admin_id:
        return
    
    if not context.args:
        await update.message.reply_text(
            "❌ <b>Usage:</b>\n\n"
            "<code>/addchannel -1001234567890</code>\n"
            "<code>/addchannel -1001234567890 My Channel Name</code>\n\n"
            "<b>How to get Channel ID:</b>\n"
            "1. Forward a message from your channel to @userinfobot\n"
            "2. It will show you the channel ID",
            reply_markup=get_mode_keyboard(),
            parse_mode='HTML'
        )
        return
    
    channel_id = context.args[0]
    channel_name = " ".join(context.args[1:]) if len(context.args) > 1 else None
    
    if scheduler.add_channel(channel_id, channel_name):
        await update.message.reply_text(
            f"✅ <b>Channel Added Successfully!</b>\n\n"
            f"📢 Channel ID: <code>{channel_id}</code>\n"
            f"📝 Name: {channel_name or 'Unnamed'}\n"
            f"📊 Total Active Channels: <b>{len(scheduler.channel_ids)}</b>",
            reply_markup=get_mode_keyboard(),
            parse_mode='HTML'
        )
    else:
        await update.message.reply_text(
            f"⚠️ Channel already exists or error occurred",
            reply_markup=get_mode_keyboard()
        )

async def remove_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != scheduler.admin_id:
        return
    
    if not context.args:
        await update.message.reply_text(
            "❌ <b>Usage:</b>\n\n"
            "<code>/removechannel -1001234567890</code>",
            reply_markup=get_mode_keyboard(),
            parse_mode='HTML'
        )
        return
    
    channel_id = context.args[0]
    
    if scheduler.remove_channel(channel_id):
        await update.message.reply_text(
            f"✅ <b>Channel Removed!</b>\n\n"
            f"🗑️ Channel ID: <code>{channel_id}</code>\n"
            f"📊 Remaining Active Channels: <b>{len(scheduler.channel_ids)}</b>",
            reply_markup=get_mode_keyboard(),
            parse_mode='HTML'
        )
    else:
        await update.message.reply_text(
            f"❌ Channel not found: <code>{channel_id}</code>",
            reply_markup=get_mode_keyboard(),
            parse_mode='HTML'
        )

async def list_posts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != scheduler.admin_id:
        return
    
    posts = scheduler.get_pending_posts()
    
    if not posts:
        await update.message.reply_text("✅ No pending posts!", reply_markup=get_mode_keyboard())
        return
    
    response = f"📋 <b>Pending Posts ({len(posts)} total):</b>\n\n"
    
    for post in posts[:10]:
        scheduled_utc = datetime.fromisoformat(post['scheduled_time'])
        scheduled_ist = utc_to_ist(scheduled_utc)
        content = post['message'] or post['caption'] or f"[{post['media_type']}]"
        preview = content[:25] + "..." if len(content) > 25 else content
        
        response += f"🆔 {post['id']} - {scheduled_ist.strftime('%m/%d %H:%M')} IST\n"
        response += f"   {preview}\n\n"
    
    if len(posts) > 10:
        response += f"\n<i>...and {len(posts) - 10} more</i>\n"
    
    response += f"\nUse /delete [id] to remove a post"
    
    await update.message.reply_text(response, parse_mode='HTML', reply_markup=get_mode_keyboard())

async def delete_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != scheduler.admin_id:
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /delete [id]\nExample: /delete 5")
        return
    
    try:
        post_id = int(context.args[0])
        if scheduler.delete_post(post_id):
            await update.message.reply_text(f"✅ Deleted post #{post_id}", reply_markup=get_mode_keyboard())
        else:
            await update.message.reply_text(f"❌ Post #{post_id} not found", reply_markup=get_mode_keyboard())
    except ValueError:
        await update.message.reply_text("Invalid ID", reply_markup=get_mode_keyboard())

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != scheduler.admin_id:
        return
    
    user_id = update.effective_user.id
    scheduler.user_sessions[user_id] = {'mode': None, 'step': 'choose_mode'}
    
    await update.message.reply_text("❌ Cancelled. Choose a new mode:", reply_markup=get_mode_keyboard())

async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != scheduler.admin_id:
        return
    
    if not context.args or context.args[0].lower() != 'confirm':
        await update.message.reply_text(
            "⚠️ <b>WARNING: This will delete ALL pending posts!</b>\n\n"
            "To confirm, use:\n"
            "<code>/reset confirm</code>",
            reply_markup=get_mode_keyboard(),
            parse_mode='HTML'
        )
        return
    
    with scheduler.get_db() as conn:
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM posts WHERE posted = 0')
        count = c.fetchone()[0]
        
        c.execute('DELETE FROM posts WHERE posted = 0')
        conn.commit()
    
    await update.message.reply_text(
        f"✅ <b>Reset Complete!</b>\n\n"
        f"🗑️ Deleted {count} pending posts\n\n"
        f"You can now schedule new posts with correct UTC/IST timezone.",
        reply_markup=get_mode_keyboard(),
        parse_mode='HTML'
    )

def extract_content(message):
    content = {}
    
    if message.text and not message.text.startswith('/'):
        button_keywords = ["✅ Done", "❌ Cancel", "✅ Confirm", "📦 Bulk", "📅 Exact", 
                          "⏱️ Duration", "📋 View", "📊 Stats", "📢 Channels", 
                          "Schedule All", "Confirm & Schedule", "🎯 Bulk"]
        if not any(keyword in message.text for keyword in button_keywords):
            content['message'] = message.text
    
    if message.photo:
        content['media_type'] = 'photo'
        content['media_file_id'] = message.photo[-1].file_id
        content['caption'] = message.caption
    elif message.video:
        content['media_type'] = 'video'
        content['media_file_id'] = message.video.file_id
        content['caption'] = message.caption
    elif message.document:
        content['media_type'] = 'document'
        content['media_file_id'] = message.document.file_id
        content['caption'] = message.caption
    
    return content if content else None

# MESSAGE HANDLER - Main conversation flow
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # FIX: Check if update has a user before accessing it
    if not update.effective_user:
        return  # Ignore updates without a user (channel posts, system messages, etc.)
    
    if update.effective_user.id != scheduler.admin_id:
        return
    
    user_id = update.effective_user.id
    
    if user_id not in scheduler.user_sessions:
        scheduler.user_sessions[user_id] = {'mode': None, 'step': 'choose_mode'}
    
    session = scheduler.user_sessions[user_id]
    message_text = update.message.text if update.message.text else ""
    
    # ... rest of your code stays the same ...
    
    # Handle button presses for commands
    if "📊 Stats" in message_text or "stats" in message_text.lower():
        await stats_command(update, context)
        return
    
    if "📢 Channels" in message_text or "channels" in message_text.lower():
        await channels_command(update, context)
        return
    
    # ============ STEP 1: CHOOSE MODE ============
    if session['step'] == 'choose_mode':
        
        if "📦 Bulk" in message_text:
            if len(scheduler.channel_ids) == 0:
                await update.message.reply_text(
                    "❌ <b>No channels configured!</b>\n\n"
                    "Please add at least one channel first:\n"
                    "<code>/addchannel -1001234567890</code>",
                    reply_markup=get_mode_keyboard(),
                    parse_mode='HTML'
                )
                return
            
            session['mode'] = 'bulk'
            session['step'] = 'bulk_get_start_time'
            session['posts'] = []
            
            ist_now = get_ist_now()
            
            await update.message.reply_text(
                "📦 <b>BULK MODE ACTIVATED</b>\n\n"
                f"🕐 Current Time (IST): <b>{ist_now.strftime('%H:%M:%S')}</b>\n\n"
                "📅 <b>Step 1:</b> When should the FIRST post go out?\n\n"
                "<b>Examples (all times in IST):</b>\n"
                "• <code>now</code> - Start immediately\n"
                "• <code>30m</code> - In 30 minutes\n"
                "• <code>2h</code> - In 2 hours\n"
                "• <code>today 18:00</code> - Today at 6 PM\n"
                "• <code>tomorrow 9am</code> - Tomorrow at 9 AM",
                reply_markup=get_exact_time_keyboard(),
                parse_mode='HTML'
            )
            return

        elif "🎯 Bulk" in message_text and "Batches" in message_text:
            if len(scheduler.channel_ids) == 0:
                await update.message.reply_text(
                    "❌ <b>No channels configured!</b>\n\n"
                    "Please add at least one channel first:\n"
                    "<code>/addchannel -1001234567890</code>",
                    reply_markup=get_mode_keyboard(),
                    parse_mode='HTML'
                )
                return
            
            session['mode'] = 'batch'
            session['step'] = 'batch_get_start_time'
            session['posts'] = []
            
            ist_now = get_ist_now()
            
            await update.message.reply_text(
                "🎯 <b>BATCH MODE ACTIVATED</b>\n\n"
                f"🕐 Current Time (IST): <b>{ist_now.strftime('%H:%M:%S')}</b>\n\n"
                "📅 <b>Step 1:</b> When should the FIRST batch go out?\n\n"
                "<b>Examples (all times in IST):</b>\n"
                "• <code>now</code> - Start immediately\n"
                "• <code>30m</code> - In 30 minutes\n"
                "• <code>2h</code> - In 2 hours\n"
                "• <code>today 18:00</code> - Today at 6 PM\n"
                "• <code>tomorrow 9am</code> - Tomorrow at 9 AM",
                reply_markup=get_exact_time_keyboard(),
                parse_mode='HTML'
            )
            return
        
        elif "📅 Exact" in message_text:
            if len(scheduler.channel_ids) == 0:
                await update.message.reply_text(
                    "❌ <b>No channels configured!</b>\n\n"
                    "Please add at least one channel first:\n"
                    "<code>/addchannel -1001234567890</code>",
                    reply_markup=get_mode_keyboard(),
                    parse_mode='HTML'
                )
                return
            
            session['mode'] = 'exact'
            session['step'] = 'exact_get_time'
            
            ist_now = get_ist_now()
            
            await update.message.reply_text(
                "📅 <b>EXACT TIME MODE</b>\n\n"
                f"🕐 Current Time (IST): <b>{ist_now.strftime('%H:%M:%S')}</b>\n\n"
                "When should I post? (Times in IST)\n\n"
                "<b>Examples:</b>\n"
                "• <code>2025-12-31 23:59</code>\n"
                "• <code>12/25 09:00</code>\n"
                "• <code>tomorrow 2pm</code>\n"
                "• <code>today 18:00</code>",
                reply_markup=get_exact_time_keyboard(),
                parse_mode='HTML'
            )
            return
        
        elif "⏱️ Duration" in message_text:
            if len(scheduler.channel_ids) == 0:
                await update.message.reply_text(
                    "❌ <b>No channels configured!</b>\n\n"
                    "Please add at least one channel first:\n"
                    "<code>/addchannel -1001234567890</code>",
                    reply_markup=get_mode_keyboard(),
                    parse_mode='HTML'
                )
                return
            
            session['mode'] = 'duration'
            session['step'] = 'duration_get_time'
            
            ist_now = get_ist_now()
            
            await update.message.reply_text(
                "⏱️ <b>DURATION MODE</b>\n\n"
                f"🕐 Current Time (IST): <b>{ist_now.strftime('%H:%M:%S')}</b>\n\n"
                "How long to wait before posting?\n\n"
                "<b>Examples:</b>\n"
                "• <code>15m</code> - 15 minutes\n"
                "• <code>3h</code> - 3 hours\n"
                "• <code>2d</code> - 2 days",
                reply_markup=get_quick_time_keyboard(),
                parse_mode='HTML'
            )
            return
        
        elif "📋 View" in message_text:
            await list_posts(update, context)
            return
        
        elif "❌" in message_text or "cancel" in message_text.lower():
            await cancel(update, context)
            return
        
        else:
            await update.message.reply_text(
                "Please choose a mode from the menu:",
                reply_markup=get_mode_keyboard()
            )
            return
    
    # ============ MODE 1: BULK POSTS ============
    elif session['mode'] == 'bulk':
        
        if "❌" in message_text or "cancel" in message_text.lower():
            await cancel(update, context)
            return
        
        if session['step'] == 'bulk_get_start_time':
            try:
                ist_time = parse_user_time_input(message_text)
                utc_time = ist_to_utc(ist_time)
                session['bulk_start_time_utc'] = utc_time
                session['step'] = 'bulk_get_duration'
                
                await update.message.reply_text(
                    f"✅ Start time set: <b>{ist_time.strftime('%Y-%m-%d %H:%M:%S')} IST</b>\n\n"
                    f"⏱️ <b>Step 2:</b> How long to space ALL posts?\n\n"
                    "Select or type duration:\n"
                    "• <code>2h</code> - Over 2 hours\n"
                    "• <code>6h</code> - Over 6 hours\n"
                    "• <code>12h</code> - Over 12 hours\n"
                    "• <code>1d</code> - Over 24 hours\n"
                    "• <code>today</code> - Until midnight",
                    reply_markup=get_duration_keyboard(),
                    parse_mode='HTML'
                )
                
            except ValueError as e:
                await update.message.reply_text(
                    f"❌ Invalid time format!\n\n{str(e)}",
                    reply_markup=get_exact_time_keyboard()
                )
            return
        
        elif session['step'] == 'bulk_get_duration':
            try:
                duration_minutes = parse_duration_to_minutes(message_text)
                session['duration_minutes'] = duration_minutes
                session['step'] = 'bulk_collect_posts'
                
                await update.message.reply_text(
                    f"✅ Duration set: <b>{duration_minutes} minutes</b>\n\n"
                    f"📤 <b>Step 3:</b> Now send/forward all posts\n\n"
                    f"When done, click the button below:",
                    reply_markup=get_bulk_collection_keyboard(),
                    parse_mode='HTML'
                )
                
            except ValueError:
                await update.message.reply_text(
                    "❌ Invalid duration!\n\nUse: 2h, 6h, 12h, 1d, or today",
                    reply_markup=get_duration_keyboard()
                )
            return
        
        elif session['step'] == 'bulk_collect_posts':
            
            if "✅ Done" in message_text:
                posts = session.get('posts', [])
                
                if not posts:
                    await update.message.reply_text(
                        "❌ No posts collected! Send at least one post.",
                        reply_markup=get_bulk_collection_keyboard()
                    )
                    return
                
                session['step'] = 'bulk_confirm'
                
                duration_minutes = session['duration_minutes']
                num_posts = len(posts)
                interval = duration_minutes / num_posts if num_posts > 1 else 0
                start_utc = session['bulk_start_time_utc']
                start_ist = utc_to_ist(start_utc)
                end_ist = start_ist + timedelta(minutes=duration_minutes)
                
                response = f"📋 <b>CONFIRMATION REQUIRED</b>\n\n"
                response += f"📦 Total Posts: <b>{num_posts}</b>\n"
                response += f"📢 Channels: <b>{len(scheduler.channel_ids)}</b>\n"
                response += f"🕐 Start: <b>{start_ist.strftime('%Y-%m-%d %H:%M')} IST</b>\n"
                response += f"🕐 End: <b>{end_ist.strftime('%Y-%m-%d %H:%M')} IST</b>\n"
                response += f"⏱️ Duration: <b>{duration_minutes} min</b>\n"
                response += f"⏳ Interval: <b>{interval:.1f} min between posts</b>\n\n"
                response += "<b>First 5 posts:</b>\n"
                
                for i in range(min(5, num_posts)):
                    scheduled_utc = start_utc + timedelta(minutes=interval * i)
                    scheduled_ist = utc_to_ist(scheduled_utc)
                    response += f"• Post #{i+1}: {scheduled_ist.strftime('%H:%M:%S')} IST\n"
                
                if num_posts > 5:
                    response += f"\n<i>...and {num_posts - 5} more</i>\n"
                
                response += f"\n⚠️ Click <b>Confirm & Schedule</b> to proceed"
                
                await update.message.reply_text(
                    response,
                    reply_markup=get_confirmation_keyboard(),
                    parse_mode='HTML'
                )
                return
            
            content = extract_content(update.message)
            
            if content:
                session['posts'].append(content)
                count = len(session['posts'])
                await update.message.reply_text(
                    f"✅ Post #{count} added!\n\n"
                    f"📊 Total: <b>{count}</b>\n\n"
                    f"Send more or click <b>Done</b>",
                    reply_markup=get_bulk_collection_keyboard(),
                    parse_mode='HTML'
                )
            return
        
        elif session['step'] == 'bulk_confirm':
            if "✅ Confirm" in message_text:
                await schedule_bulk_posts(update, context)
                return
            elif "❌" in message_text:
                await cancel(update, context)
                return
            else:
                await update.message.reply_text(
                    "⚠️ Click <b>✅ Confirm & Schedule</b> or <b>❌ Cancel</b>",
                    reply_markup=get_confirmation_keyboard(),
                    parse_mode='HTML'
                )
                return

    # ============ MODE 2: BATCH POSTS ============
    elif session['mode'] == 'batch':
        
        if "❌" in message_text or "cancel" in message_text.lower():
            await cancel(update, context)
            return
        
        if session['step'] == 'batch_get_start_time':
            try:
                ist_time = parse_user_time_input(message_text)
                utc_time = ist_to_utc(ist_time)
                session['batch_start_time_utc'] = utc_time
                session['step'] = 'batch_get_duration'
                
                await update.message.reply_text(
                    f"✅ Start time set: <b>{ist_time.strftime('%Y-%m-%d %H:%M:%S')} IST</b>\n\n"
                    f"⏱️ <b>Step 2:</b> Total duration for ALL batches?\n\n"
                    "Select or type:\n"
                    "• <code>2h</code> - Over 2 hours\n"
                    "• <code>6h</code> - Over 6 hours\n"
                    "• <code>12h</code> - Over 12 hours\n"
                    "• <code>1d</code> - Over 24 hours",
                    reply_markup=get_duration_keyboard(),
                    parse_mode='HTML'
                )
                
            except ValueError as e:
                await update.message.reply_text(
                    f"❌ Invalid time format!\n\n{str(e)}",
                    reply_markup=get_exact_time_keyboard()
                )
            return
        
        elif session['step'] == 'batch_get_duration':
            try:
                duration_minutes = parse_duration_to_minutes(message_text)
                session['duration_minutes'] = duration_minutes
                session['step'] = 'batch_get_batch_size'
                
                await update.message.reply_text(
                    f"✅ Duration: <b>{duration_minutes} min</b>\n\n"
                    f"📦 <b>Step 3:</b> Posts per batch?\n\n"
                    "Select or type:\n"
                    "• <code>10</code> - 10 posts\n"
                    "• <code>20</code> - 20 posts\n"
                    "• <code>50</code> - 50 posts",
                    reply_markup=get_batch_size_keyboard(),
                    parse_mode='HTML'
                )
                
            except ValueError:
                await update.message.reply_text(
                    "❌ Invalid duration!\n\nUse: 2h, 6h, 12h, 1d",
                    reply_markup=get_duration_keyboard()
                )
            return
        
        elif session['step'] == 'batch_get_batch_size':
            try:
                batch_size = int(message_text.strip())
                if batch_size < 1:
                    raise ValueError("Must be at least 1")
                
                session['batch_size'] = batch_size
                session['step'] = 'batch_collect_posts'
                
                await update.message.reply_text(
                    f"✅ Batch size: <b>{batch_size} posts</b>\n\n"
                    f"📤 <b>Step 4:</b> Send/forward all posts\n\n"
                    f"Click button when done:",
                    reply_markup=get_bulk_collection_keyboard(),
                    parse_mode='HTML'
                )
                
            except ValueError:
                await update.message.reply_text(
                    "❌ Invalid! Enter a number (e.g., 10, 20, 30)",
                    reply_markup=get_batch_size_keyboard()
                )
            return
        
        elif session['step'] == 'batch_collect_posts':
            
            if "✅ Done" in message_text:
                posts = session.get('posts', [])
                
                if not posts:
                    await update.message.reply_text(
                        "❌ No posts! Send at least one.",
                        reply_markup=get_bulk_collection_keyboard()
                    )
                    return
                
                session['step'] = 'batch_confirm'
                
                duration_minutes = session['duration_minutes']
                batch_size = session['batch_size']
                num_posts = len(posts)
                num_batches = (num_posts + batch_size - 1) // batch_size
                batch_interval = duration_minutes / num_batches if num_batches > 1 else 0
                start_utc = session['batch_start_time_utc']
                start_ist = utc_to_ist(start_utc)
                
                response = f"📋 <b>CONFIRMATION REQUIRED</b>\n\n"
                response += f"📦 Total Posts: <b>{num_posts}</b>\n"
                response += f"🎯 Batch Size: <b>{batch_size} posts</b>\n"
                response += f"📊 Batches: <b>{num_batches}</b>\n"
                response += f"📢 Channels: <b>{len(scheduler.channel_ids)}</b>\n"
                response += f"🕐 Start: <b>{start_ist.strftime('%Y-%m-%d %H:%M')} IST</b>\n"
                response += f"⏱️ Duration: <b>{duration_minutes} min</b>\n"
                response += f"⏳ Batch Interval: <b>{batch_interval:.1f} min</b>\n\n"
                response += "<b>Schedule Preview:</b>\n"
                
                for i in range(min(5, num_batches)):
                    batch_utc = start_utc + timedelta(minutes=batch_interval * i)
                    batch_ist = utc_to_ist(batch_utc)
                    batch_start = i * batch_size + 1
                    batch_end = min((i + 1) * batch_size, num_posts)
                    response += f"• Batch #{i+1}: {batch_ist.strftime('%H:%M')} IST - Posts #{batch_start}-{batch_end}\n"
                
                if num_batches > 5:
                    response += f"\n<i>...and {num_batches - 5} more batches</i>\n"
                
                response += f"\n⚠️ Click <b>Confirm & Schedule</b>"
                
                await update.message.reply_text(
                    response,
                    reply_markup=get_confirmation_keyboard(),
                    parse_mode='HTML'
                )
                return
            
            content = extract_content(update.message)
            
            if content:
                session['posts'].append(content)
                count = len(session['posts'])
                await update.message.reply_text(
                    f"✅ Post #{count} added!\n\n"
                    f"📊 Total: <b>{count}</b>\n\n"
                    f"Send more or click <b>Done</b>",
                    reply_markup=get_bulk_collection_keyboard(),
                    parse_mode='HTML'
                )
            return
        
        elif session['step'] == 'batch_confirm':
            if "✅ Confirm" in message_text:
                await schedule_batch_posts(update, context)
                return
            elif "❌" in message_text:
                await cancel(update, context)
                return
            else:
                await update.message.reply_text(
                    "⚠️ Click <b>✅ Confirm & Schedule</b> or <b>❌ Cancel</b>",
                    reply_markup=get_confirmation_keyboard(),
                    parse_mode='HTML'
                )
                return
            
            # ============ MODE 3: EXACT TIME ============
    elif session['mode'] == 'exact':
        
        if "❌" in message_text or "cancel" in message_text.lower():
            await cancel(update, context)
            return
        
        if session['step'] == 'exact_get_time':
            try:
                ist_time = parse_user_time_input(message_text)
                utc_time = ist_to_utc(ist_time)
                session['scheduled_time_utc'] = utc_time
                session['step'] = 'exact_get_content'
                
                await update.message.reply_text(
                    f"✅ Time set: <b>{ist_time.strftime('%Y-%m-%d %H:%M:%S')} IST</b>\n\n"
                    f"📤 Now send/forward the content to post",
                    reply_markup=ReplyKeyboardMarkup([[KeyboardButton("❌ Cancel")]], resize_keyboard=True),
                    parse_mode='HTML'
                )
                
            except ValueError as e:
                await update.message.reply_text(
                    f"❌ {str(e)}",
                    reply_markup=get_exact_time_keyboard()
                )
            return
        
        elif session['step'] == 'exact_get_content':
            content = extract_content(update.message)
            
            if not content:
                await update.message.reply_text(
                    "❌ Please send valid content (text, photo, video, or document)",
                    reply_markup=ReplyKeyboardMarkup([[KeyboardButton("❌ Cancel")]], resize_keyboard=True)
                )
                return
            
            session['content'] = content
            session['step'] = 'exact_confirm'
            
            scheduled_utc = session['scheduled_time_utc']
            scheduled_ist = utc_to_ist(scheduled_utc)
            time_diff = scheduled_utc - utc_now()
            minutes = int(time_diff.total_seconds() / 60)
            
            content_preview = content.get('message', '')[:50] if content.get('message') else f"[{content.get('media_type', 'media')}]"
            
            response = f"📋 <b>CONFIRMATION REQUIRED</b>\n\n"
            response += f"📅 Scheduled: <b>{scheduled_ist.strftime('%Y-%m-%d %H:%M:%S')} IST</b>\n"
            response += f"⏱️ Posts in: <b>{minutes} minutes</b>\n"
            response += f"📢 Channels: <b>{len(scheduler.channel_ids)}</b>\n"
            response += f"📝 Content: <i>{content_preview}...</i>\n\n"
            response += f"⚠️ Click <b>Confirm & Schedule</b>"
            
            await update.message.reply_text(
                response,
                reply_markup=get_confirmation_keyboard(),
                parse_mode='HTML'
            )
            return
        
        elif session['step'] == 'exact_confirm':
            if "✅ Confirm" in message_text:
                content = session['content']
                scheduled_utc = session['scheduled_time_utc']
                scheduled_ist = utc_to_ist(scheduled_utc)
                
                post_id = scheduler.schedule_post(
                    scheduled_time_utc=scheduled_utc,
                    message=content.get('message'),
                    media_type=content.get('media_type'),
                    media_file_id=content.get('media_file_id'),
                    caption=content.get('caption')
                )
                
                await update.message.reply_text(
                    f"✅ <b>SCHEDULED SUCCESSFULLY!</b>\n\n"
                    f"🆔 Post ID: {post_id}\n"
                    f"📅 Time: {scheduled_ist.strftime('%Y-%m-%d %H:%M:%S')} IST\n"
                    f"📢 Channels: {len(scheduler.channel_ids)}\n"
                    f"🧹 Auto-cleanup: {scheduler.auto_cleanup_minutes} min after posting\n\n"
                    f"Choose another mode:",
                    reply_markup=get_mode_keyboard(),
                    parse_mode='HTML'
                )
                
                scheduler.user_sessions[user_id] = {'mode': None, 'step': 'choose_mode'}
                return
            elif "❌" in message_text:
                await cancel(update, context)
                return
            else:
                await update.message.reply_text(
                    "⚠️ Click <b>✅ Confirm & Schedule</b> or <b>❌ Cancel</b>",
                    reply_markup=get_confirmation_keyboard(),
                    parse_mode='HTML'
                )
                return
    
    # ============ MODE 4: DURATION ============
    elif session['mode'] == 'duration':
        
        if "❌" in message_text or "cancel" in message_text.lower():
            await cancel(update, context)
            return
        
        if session['step'] == 'duration_get_time':
            try:
                ist_time = parse_user_time_input(message_text)
                utc_time = ist_to_utc(ist_time)
                session['scheduled_time_utc'] = utc_time
                session['step'] = 'duration_get_content'
                
                await update.message.reply_text(
                    f"✅ Will post at: <b>{ist_time.strftime('%Y-%m-%d %H:%M:%S')} IST</b>\n\n"
                    f"📤 Now send/forward the content",
                    reply_markup=ReplyKeyboardMarkup([[KeyboardButton("❌ Cancel")]], resize_keyboard=True),
                    parse_mode='HTML'
                )
                
            except ValueError:
                await update.message.reply_text(
                    "❌ Invalid duration!\n\nUse: 5m, 30m, 2h, 1d, or now",
                    reply_markup=get_quick_time_keyboard()
                )
            return
        
        elif session['step'] == 'duration_get_content':
            content = extract_content(update.message)
            
            if not content:
                await update.message.reply_text(
                    "❌ Please send valid content (text, photo, video, or document)",
                    reply_markup=ReplyKeyboardMarkup([[KeyboardButton("❌ Cancel")]], resize_keyboard=True)
                )
                return
            
            session['content'] = content
            session['step'] = 'duration_confirm'
            
            scheduled_utc = session['scheduled_time_utc']
            scheduled_ist = utc_to_ist(scheduled_utc)
            time_diff = scheduled_utc - utc_now()
            minutes = int(time_diff.total_seconds() / 60)
            
            content_preview = content.get('message', '')[:50] if content.get('message') else f"[{content.get('media_type', 'media')}]"
            
            response = f"📋 <b>CONFIRMATION REQUIRED</b>\n\n"
            response += f"⏱️ Posts in: <b>{minutes} minutes</b>\n"
            response += f"📅 At: {scheduled_ist.strftime('%Y-%m-%d %H:%M:%S')} IST\n"
            response += f"📢 Channels: <b>{len(scheduler.channel_ids)}</b>\n"
            response += f"📝 Content: <i>{content_preview}...</i>\n\n"
            response += f"⚠️ Click <b>Confirm & Schedule</b>"
            
            await update.message.reply_text(
                response,
                reply_markup=get_confirmation_keyboard(),
                parse_mode='HTML'
            )
            return
        
        elif session['step'] == 'duration_confirm':
            if "✅ Confirm" in message_text:
                content = session['content']
                scheduled_utc = session['scheduled_time_utc']
                scheduled_ist = utc_to_ist(scheduled_utc)
                
                post_id = scheduler.schedule_post(
                    scheduled_time_utc=scheduled_utc,
                    message=content.get('message'),
                    media_type=content.get('media_type'),
                    media_file_id=content.get('media_file_id'),
                    caption=content.get('caption')
                )
                
                time_diff = scheduled_utc - utc_now()
                minutes = int(time_diff.total_seconds() / 60)
                
                await update.message.reply_text(
                    f"✅ <b>SCHEDULED SUCCESSFULLY!</b>\n\n"
                    f"🆔 Post ID: {post_id}\n"
                    f"⏱️ Posts in: {minutes} minutes\n"
                    f"📅 At: {scheduled_ist.strftime('%H:%M:%S')} IST\n"
                    f"📢 Channels: {len(scheduler.channel_ids)}\n"
                    f"🧹 Auto-cleanup: {scheduler.auto_cleanup_minutes} min after posting\n\n"
                    f"Choose another mode:",
                    reply_markup=get_mode_keyboard(),
                    parse_mode='HTML'
                )
                
                scheduler.user_sessions[user_id] = {'mode': None, 'step': 'choose_mode'}
                return
            elif "❌" in message_text:
                await cancel(update, context)
                return
            else:
                await update.message.reply_text(
                    "⚠️ Click <b>✅ Confirm & Schedule</b> or <b>❌ Cancel</b>",
                    reply_markup=get_confirmation_keyboard(),
                    parse_mode='HTML'
                )
                return


# SCHEDULING FUNCTIONS
async def schedule_bulk_posts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    session = scheduler.user_sessions[user_id]
    
    posts = session.get('posts', [])
    duration_minutes = session['duration_minutes']
    start_utc = session['bulk_start_time_utc']
    num_posts = len(posts)
    interval = duration_minutes / num_posts if num_posts > 1 else 0
    
    scheduled_info = []
    
    for i, post in enumerate(posts):
        scheduled_utc = start_utc + timedelta(minutes=interval * i)
        post_id = scheduler.schedule_post(
            scheduled_time_utc=scheduled_utc,
            message=post.get('message'),
            media_type=post.get('media_type'),
            media_file_id=post.get('media_file_id'),
            caption=post.get('caption')
        )
        scheduled_info.append((post_id, scheduled_utc))
    
    start_ist = utc_to_ist(start_utc)
    
    response = f"✅ <b>BULK SCHEDULED SUCCESSFULLY!</b>\n\n"
    response += f"📦 Total Posts: {num_posts}\n"
    response += f"📢 Channels: {len(scheduler.channel_ids)}\n"
    response += f"🕐 Start: {start_ist.strftime('%Y-%m-%d %H:%M')} IST\n"
    response += f"⏱️ Duration: {duration_minutes} min\n"
    response += f"⏳ Interval: {interval:.1f} min\n"
    response += f"🧹 Auto-cleanup: {scheduler.auto_cleanup_minutes} min\n\n"
    response += "<b>Schedule Summary:</b>\n"
    
    for post_id, utc_time in scheduled_info[:5]:
        ist_time = utc_to_ist(utc_time)
        response += f"• {ist_time.strftime('%H:%M')} IST - Post #{post_id}\n"
    
    if num_posts > 5:
        response += f"\n<i>...and {num_posts - 5} more</i>\n"
    
    response += f"\nChoose another mode:"
    
    await update.message.reply_text(
        response,
        reply_markup=get_mode_keyboard(),
        parse_mode='HTML'
    )
    
    scheduler.user_sessions[user_id] = {'mode': None, 'step': 'choose_mode'}

async def schedule_batch_posts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    session = scheduler.user_sessions[user_id]
    
    posts = session.get('posts', [])
    duration_minutes = session['duration_minutes']
    batch_size = session['batch_size']
    start_utc = session['batch_start_time_utc']
    num_posts = len(posts)
    num_batches = (num_posts + batch_size - 1) // batch_size
    batch_interval = duration_minutes / num_batches if num_batches > 1 else 0
    
    scheduled_info = []
    
    for i, post in enumerate(posts):
        batch_number = i // batch_size
        post_in_batch = i % batch_size
        scheduled_utc = start_utc + timedelta(minutes=batch_interval * batch_number, seconds=post_in_batch * 2)
        
        post_id = scheduler.schedule_post(
            scheduled_time_utc=scheduled_utc,
            message=post.get('message'),
            media_type=post.get('media_type'),
            media_file_id=post.get('media_file_id'),
            caption=post.get('caption')
        )
        scheduled_info.append((post_id, scheduled_utc, batch_number + 1))
    
    start_ist = utc_to_ist(start_utc)
    
    response = f"✅ <b>BATCH SCHEDULED SUCCESSFULLY!</b>\n\n"
    response += f"📦 Total Posts: {num_posts}\n"
    response += f"🎯 Batch Size: {batch_size} posts\n"
    response += f"📊 Batches: {num_batches}\n"
    response += f"📢 Channels: {len(scheduler.channel_ids)}\n"
    response += f"🕐 Start: {start_ist.strftime('%Y-%m-%d %H:%M')} IST\n"
    response += f"⏱️ Duration: {duration_minutes} min\n"
    response += f"⏳ Batch Interval: {batch_interval:.1f} min\n"
    response += f"🧹 Auto-cleanup: {scheduler.auto_cleanup_minutes} min\n\n"
    response += "<b>Batch Schedule:</b>\n"
    
    current_batch = 0
    for post_id, utc_time, batch_num in scheduled_info[:10]:
        ist_time = utc_to_ist(utc_time)
        if batch_num != current_batch:
            if current_batch > 0:
                response += "\n"
            response += f"<b>Batch #{batch_num}</b> at {ist_time.strftime('%H:%M')} IST:\n"
            current_batch = batch_num
        response += f"  • Post #{post_id}\n"
    
    if num_posts > 10:
        response += f"\n<i>...and {num_posts - 10} more</i>\n"
    
    response += f"\nChoose another mode:"
    
    await update.message.reply_text(
        response,
        reply_markup=get_mode_keyboard(),
        parse_mode='HTML'
    )
    
    scheduler.user_sessions[user_id] = {'mode': None, 'step': 'choose_mode'}


# BACKGROUND TASKS
async def background_poster(application):
    bot = application.bot
    cleanup_counter = 0
    
    while True:
        try:
            await scheduler.process_due_posts(bot)
            
            cleanup_counter += 1
            if cleanup_counter >= 2:
                scheduler.cleanup_posted_content()
                cleanup_counter = 0
                
        except Exception as e:
            logger.error(f"Background task error: {e}")
        
        await asyncio.sleep(15)

async def post_init(application):
    asyncio.create_task(background_poster(application))


async def export_channels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Export all channels as addchannel commands"""
    if update.effective_user.id != scheduler.admin_id:
        return
    
    channels = scheduler.get_all_channels()
    
    if not channels:
        await update.message.reply_text("No channels to export!", reply_markup=get_mode_keyboard())
        return
    
    # Create addchannel commands for all active channels
    commands = []
    for channel in channels:
        if channel['active']:
            name = channel['channel_name'] or ""
            if name:
                commands.append(f"/addchannel {channel['channel_id']} {name}")
            else:
                commands.append(f"/addchannel {channel['channel_id']}")
    
    export_text = "🔖 <b>CHANNEL BACKUP</b>\n\n"
    export_text += "Copy these commands and save them:\n\n"
    export_text += "<code>" + "\n".join(commands) + "</code>\n\n"
    export_text += f"📊 Total: {len(commands)} channels\n\n"
    export_text += "After redeployment, paste these back to restore channels."
    
    await update.message.reply_text(export_text, parse_mode='HTML', reply_markup=get_mode_keyboard())

async def backup_posts_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Export all pending posts as commands"""
    if update.effective_user.id != scheduler.admin_id:
        return
    
    posts = scheduler.get_pending_posts()
    
    if not posts:
        await update.message.reply_text("No pending posts to backup!", reply_markup=get_mode_keyboard())
        return
    
    # Group posts by scheduled time for better readability
    backup_text = "📦 <b>POSTS BACKUP</b>\n\n"
    backup_text += f"Total pending posts: <b>{len(posts)}</b>\n\n"
    backup_text += "⚠️ <b>IMPORTANT:</b> Copy this entire message and save it somewhere safe!\n\n"
    backup_text += "=" * 30 + "\n\n"
    
    for post in posts[:50]:  # Limit to 50 to avoid message too long
        scheduled_utc = datetime.fromisoformat(post['scheduled_time'])
        scheduled_ist = utc_to_ist(scheduled_utc)
        
        backup_text += f"🆔 Post #{post['id']}\n"
        backup_text += f"📅 Time: {scheduled_ist.strftime('%Y-%m-%d %H:%M')} IST\n"
        
        if post['message']:
            preview = post['message'][:50] + "..." if len(post['message']) > 50 else post['message']
            backup_text += f"📝 Text: {preview}\n"
        elif post['media_type']:
            backup_text += f"📎 Media: {post['media_type']}\n"
            if post['caption']:
                preview = post['caption'][:50] + "..." if len(post['caption']) > 50 else post['caption']
                backup_text += f"📝 Caption: {preview}\n"
        
        backup_text += "\n"
    
    if len(posts) > 50:
        backup_text += f"\n⚠️ Showing first 50 posts. Total: {len(posts)}\n"
    
    backup_text += "\n💡 To restore: Schedule posts manually using the bot after restart.\n"
    
    await update.message.reply_text(backup_text, parse_mode='HTML', reply_markup=get_mode_keyboard())

# MAIN FUNCTION
def main():
    global scheduler
    
    BOT_TOKEN = os.environ.get('BOT_TOKEN')
    ADMIN_ID = int(os.environ.get('ADMIN_ID'))
    
    if not BOT_TOKEN or not ADMIN_ID:
        logger.error("❌ BOT_TOKEN and ADMIN_ID must be set in environment variables!")
        sys.exit(1)
    
    CHANNEL_IDS_STR = os.environ.get('CHANNEL_IDS', '')
    CHANNEL_IDS = [ch.strip() for ch in CHANNEL_IDS_STR.split(',') if ch.strip()]
    
    scheduler = ThreeModeScheduler(BOT_TOKEN, ADMIN_ID, auto_cleanup_minutes=30)
    
    for channel_id in CHANNEL_IDS:
        scheduler.add_channel(channel_id)
    
    logger.info(f"📢 Loaded {len(CHANNEL_IDS)} channels from environment variables")
    
    from telegram.request import HTTPXRequest

    request = HTTPXRequest(
    connection_pool_size=20,
    connect_timeout=90.0,
    read_timeout=90.0,
    write_timeout=90.0,
    pool_timeout=90.0
    )
    
    app = Application.builder().token(BOT_TOKEN).request(request).post_init(post_init).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("list", list_posts))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("channels", channels_command))
    app.add_handler(CommandHandler("addchannel", add_channel_command))
    app.add_handler(CommandHandler("removechannel", remove_channel_command))
    app.add_handler(CommandHandler("delete", delete_post))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CommandHandler("reset", reset_command))
    app.add_handler(CommandHandler("exportchannels", export_channels_command))
    app.add_handler(CommandHandler("backup", backup_posts_command))
    
    app.add_handler(MessageHandler(filters.ALL, handle_message))
    
    logger.info("="*60)
    logger.info(f"✅ TELEGRAM SCHEDULER WITH UTC STARTED")
    logger.info(f"📢 Active Channels: {len(scheduler.channel_ids)}")
    logger.info(f"🧹 Auto-cleanup: {scheduler.auto_cleanup_minutes} min")
    logger.info(f"👤 Admin ID: {ADMIN_ID}")
    logger.info(f"🌍 Timezone: All times stored in UTC, displayed in IST")
    logger.info("="*60)
    
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
