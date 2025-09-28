import os
import re
import sqlite3
import pytz  # For timezone support
from functools import wraps
from datetime import datetime, timedelta
from telegram import Update, ChatPermissions
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.error import BadRequest

# === CONFIGURATION ===
# IMPORTANT: PASTE THE FILE ID FOR YOUR GIF HERE
# To get this, send your GIF to @JsonDumpBot and copy the correct 'file_id' value.
OPEN_GIF_FILE_ID = 'CgACAgQAAxkBAAEYVE1o1mcYvHtot7qjiudO6nXaCBbw3wAC4AIAAhhPDVOzldaDFsYkKjYE'
# Default link to be used by /tracking if no custom link is set
DEFAULT_TRACKING_LINK = "x.com/your_default_username"

SESSION_PHASES = {}
conn = sqlite3.connect("group_data.db", check_same_thread=False)
c = conn.cursor()

# === DATABASE SETUP ===
def setup_database():
    """Initializes and migrates the database schema."""
    c.execute("""CREATE TABLE IF NOT EXISTS group_connections (
        chat_id INTEGER PRIMARY KEY,
        target_chat_id INTEGER NOT NULL
    )""")
    
    c.execute("""CREATE TABLE IF NOT EXISTS group_settings (
        chat_id INTEGER PRIMARY KEY,
        tracking_link TEXT NOT NULL
    )""")

    for table in ["links", "status", "srlist", "whitelist"]:
        try:
            c.execute(f"PRAGMA table_info({table})")
            columns = [info[1] for info in c.fetchall()]
            if 'chat_id' not in columns:
                c.execute(f"ALTER TABLE {table} ADD COLUMN chat_id INTEGER")
        except sqlite3.OperationalError:
            pass  # Table doesn't exist, will be created below

    c.execute("""CREATE TABLE IF NOT EXISTS links (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL, telegram_user TEXT,
        telegram_name TEXT, twitter_user TEXT, full_link TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS status (
        chat_id INTEGER NOT NULL, telegram_user TEXT NOT NULL, completed INTEGER DEFAULT 0,
        last_done DATETIME, PRIMARY KEY (chat_id, telegram_user)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS srlist (
        chat_id INTEGER NOT NULL, telegram_user TEXT NOT NULL, telegram_name TEXT,
        added_on DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (chat_id, telegram_user)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS whitelist (
        chat_id INTEGER NOT NULL, telegram_user TEXT NOT NULL,
        PRIMARY KEY (chat_id, telegram_user)
    )""")
    conn.commit()

setup_database()


# === REGEX & HELPERS ===
twitter_regex = re.compile(
    r"(https?://(?:www\.)?(?:twitter|x)\.com/([A-Za-z0-9_]+)/status/\d+)", re.IGNORECASE
)
done_regex = re.compile(r"\b(done|completed|ad|all done|dn)\b", re.IGNORECASE)

def get_effective_chat_id(chat_id):
    c.execute("SELECT target_chat_id FROM group_connections WHERE chat_id = ?", (chat_id,))
    row = c.fetchone()
    return row[0] if row else chat_id

def tg_mention(name, user_id):
    return f"<a href='tg://user?id={int(user_id)}'>{name}</a>"

def get_main_link(chat_id, telegram_user):
    c.execute("""
        SELECT twitter_user, full_link FROM links
        WHERE chat_id = ? AND telegram_user = ?
        ORDER BY id DESC LIMIT 1
    """, (chat_id, telegram_user,))
    row = c.fetchone()
    return row if row else None

async def delete_message_job(context: ContextTypes.DEFAULT_TYPE):
    job_data = context.job.data
    try:
        await context.bot.delete_message(chat_id=job_data['chat_id'], message_id=job_data['message_id'])
    except BadRequest:
        pass

async def enable_chat(chat):
    permissions = ChatPermissions(can_send_messages=True, can_send_audios=True, can_send_documents=True, can_send_photos=True, can_send_videos=True, can_send_video_notes=True, can_send_voice_notes=True, can_send_polls=True, can_send_other_messages=True, can_add_web_page_previews=True, can_change_info=False, can_invite_users=True, can_pin_messages=False)
    await chat.set_permissions(permissions)

async def disable_chat(chat):
    permissions = ChatPermissions(can_send_messages=False, can_send_audios=False, can_send_documents=False, can_send_photos=False, can_send_videos=False, can_send_video_notes=False, can_send_voice_notes=False, can_send_polls=False, can_send_other_messages=False, can_add_web_page_previews=False, can_change_info=False, can_invite_users=False, can_pin_messages=False)
    await chat.set_permissions(permissions)

# === ADMIN DECORATOR ===
def admin_only(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if not update.effective_user or not update.effective_chat: return
        user = update.effective_user
        chat = update.effective_chat
        try:
            member = await chat.get_member(user.id)
            if member.status not in ["administrator", "creator"]:
                await update.message.reply_text("⚠️ **Only admins can use this command.**")
                return
        except BadRequest:
            await update.message.reply_text("⚠️ This command can only be used in a group where I am an admin.")
            return
        return await func(update, context, *args, **kwargs)
    return wrapper

# === SAFELIST COMMANDS ===
@admin_only
async def save_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id)
    if not update.message.reply_to_message:
        await update.message.reply_text("⚠️ Reply to a user's message to add them to the safelist.")
        return
    target_user = update.message.reply_to_message.from_user
    c.execute("INSERT OR IGNORE INTO whitelist (chat_id, telegram_user) VALUES (?, ?)", (effective_chat_id, str(target_user.id)))
    conn.commit()
    await update.message.reply_text(f"✅ {tg_mention(target_user.full_name, target_user.id)} has been added to the safelist.", parse_mode="HTML")

@admin_only
async def unsave_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id)
    if not update.message.reply_to_message:
        await update.message.reply_text("⚠️ Reply to a user's message to remove them from the safelist.")
        return
    target_user = update.message.reply_to_message.from_user
    c.execute("DELETE FROM whitelist WHERE chat_id = ? AND telegram_user = ?", (effective_chat_id, str(target_user.id)))
    conn.commit()
    await update.message.reply_text(f"🗑️ {tg_mention(target_user.full_name, target_user.id)} has been removed from the safelist.", parse_mode="HTML")

@admin_only
async def list_saved_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id)
    c.execute("SELECT telegram_user FROM whitelist WHERE chat_id = ?", (effective_chat_id,))
    rows = c.fetchall()
    if not rows:
        await update.message.reply_text("📝 The safelist is empty.")
        return
    msg = "📝 **Safelisted Users:**\n\n"
    for idx, (tg_user,) in enumerate(rows, 1):
        c.execute("SELECT telegram_name FROM links WHERE chat_id = ? AND telegram_user=? ORDER BY id DESC LIMIT 1", (effective_chat_id, tg_user,))
        name_row = c.fetchone()
        name = name_row[0] if name_row else f"ID: {tg_user}"
        msg += f"{idx}. {tg_mention(name, tg_user)}\n"
    await update.message.reply_text(msg, parse_mode="HTML")

# === CORE COMMANDS ===
@admin_only
async def open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id)
    SESSION_PHASES[effective_chat_id] = "links"
    chat = update.effective_chat
    await enable_chat(chat)
    if "[OPEN]" not in chat.title:
        new_title = chat.title.replace("[CLOSED]", "").strip() + " [OPEN]"
        await chat.set_title(new_title)
    await context.bot.send_animation(chat.id, animation=OPEN_GIF_FILE_ID)
    message_text = "<b>🚀 Start dropping your post links!</b>"
    try:
        sent_message = await context.bot.send_message(chat_id=chat.id, text=message_text, parse_mode="HTML")
        await context.bot.pin_chat_message(chat_id=chat.id, message_id=sent_message.message_id)
    except BadRequest as e:
        error_text = "⚠️ Could not pin message. Please grant 'Pin Messages' admin rights." if "rights" in str(e).lower() else "⚠️ An error occurred while pinning the message."
        await context.bot.send_message(chat_id=chat.id, text=error_text, parse_mode="HTML")

@admin_only
async def users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id)
    c.execute("SELECT COUNT(DISTINCT telegram_user) FROM links WHERE chat_id = ?", (effective_chat_id,))
    total_unique = c.fetchone()[0]
    await update.message.reply_text(f"📊 **Total unique users:** {total_unique}")

@admin_only
async def multiple_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id)
    c.execute("SELECT telegram_user, telegram_name, twitter_user, full_link FROM links WHERE chat_id = ? ORDER BY telegram_user, id", (effective_chat_id,))
    rows = c.fetchall()
    
    user_links = {}
    for tg_user, tg_name, tw_user, link in rows:
        user_links.setdefault((tg_user, tg_name), []).append((tw_user, link))

    msg = "🔗 **Multiple Links by Same User**:\n\n"
    count_multi = 0
    for (tg_user, tg_name), links in user_links.items():
        if len(links) > 1:
            count_multi += 1
            msg += f"{count_multi}. 🙍🏻‍♂️ {tg_mention(tg_name, tg_user)}\n"
            for idx, (tw_user, link) in enumerate(links, 1):
                msg += f"    {idx}. 𝕏 <a href='{link}'>@{tw_user}</a>\n"
            msg += "\n"
    if count_multi == 0:
        msg += "✅ No user shared multiple links.\n\n"

    tw_map = {}
    for tg_user, tg_name, tw_user, link in rows:
        tw_map.setdefault(tw_user, []).append((tg_user, tg_name, link))

    msg += "🚨 **Fraud (Same X Username by Different Users)**:\n\n"
    count_fraud = 0
    for tw_user, tg_list in tw_map.items():
        if len({u[0] for u in tg_list}) > 1:
            count_fraud += 1
            msg += f"{count_fraud}. 𝕏 @{tw_user}\n"
            for i, (tg_user, tg_name, link) in enumerate(tg_list, 1):
                msg += f"    {i}. 🙍🏻‍♂️ {tg_mention(tg_name, tg_user)} → <a href='{link}'>Link</a>\n"
            msg += "\n"
    if count_fraud == 0:
        msg += "✅ No fraud cases found."
        
    await update.message.reply_text(msg, parse_mode="HTML")

@admin_only
async def unsafe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id)
    c.execute("SELECT DISTINCT telegram_user FROM links WHERE chat_id = ?", (effective_chat_id,))
    all_users = {u[0] for u in c.fetchall()}
    c.execute("SELECT telegram_user FROM status WHERE chat_id = ? AND completed=1", (effective_chat_id,))
    completed_users = {u[0] for u in c.fetchall()}
    c.execute("SELECT telegram_user FROM whitelist WHERE chat_id = ?", (effective_chat_id,))
    whitelisted_users = {row[0] for row in c.fetchall()}
    unsafe_users = all_users - completed_users - whitelisted_users
    if not unsafe_users:
        await update.message.reply_text("✅ All users are safe.")
        return
    msg = "⚠️ **Unsafe users:**\n\n"
    for idx, tg_user in enumerate(unsafe_users, 1):
        c.execute("SELECT telegram_name FROM links WHERE chat_id = ? AND telegram_user=? LIMIT 1", (effective_chat_id, tg_user,))
        name = (c.fetchone() or ["Unknown"])[0]
        main = get_main_link(effective_chat_id, tg_user)
        main_msg = f"→ 𝕏 @{main[0]}" if main else ""
        msg += f"{idx}. 🙍🏻‍♂️ {tg_mention(name, tg_user)} {main_msg}\n"
    await update.message.reply_text(msg, parse_mode="HTML")

@admin_only
async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id)
    c.execute("SELECT DISTINCT telegram_user, telegram_name FROM links WHERE chat_id = ?", (effective_chat_id,))
    all_users = c.fetchall()
    if not all_users:
        await update.message.reply_text("• No users have shared links yet.")
        return
    msg = "📋 **User List:**\n\n"
    for idx, (tg_user, name) in enumerate(all_users, 1):
        main = get_main_link(effective_chat_id, tg_user)
        main_msg = f"→ 𝕏 @{main[0]}" if main else ""
        msg += f"{idx}. 🙍🏻‍♂️ {tg_mention(name, tg_user)} {main_msg}\n"
    await update.message.reply_text(msg, parse_mode="HTML")

@admin_only
async def get_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id)
    if update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
        main = get_main_link(effective_chat_id, str(target_user.id))
        if main:
            msg = f"🔗 Link for {tg_mention(target_user.full_name, target_user.id)}:\n<a href='{main[1]}'>@{main[0]}</a>"
        else:
            msg = f"⚠️ No link found for {tg_mention(target_user.full_name, target_user.id)}."
        await update.message.reply_text(msg, parse_mode="HTML")
        return

    c.execute("SELECT DISTINCT telegram_user, telegram_name FROM links WHERE chat_id = ?", (effective_chat_id,))
    all_users = c.fetchall()
    if not all_users:
        await update.message.reply_text("• No links found.")
        return
    msg = "📋 **User Links:**\n\n"
    for idx, (tg_user, name) in enumerate(all_users, 1):
        main = get_main_link(effective_chat_id, tg_user)
        main_msg = f"→ 𝕏 <a href='{main[1]}'>@{main[0]}</a>" if main else ""
        msg += f"{idx}. 🙍🏻‍♂️ {tg_mention(name, tg_user)} {main_msg}\n"
    await update.message.reply_text(msg, parse_mode="HTML")

@admin_only
async def clean_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    count = int(context.args[0]) if context.args and context.args[0].isdigit() else 100
    chat_id = update.effective_chat.id
    command_message_id = update.message.message_id
    deleted_count = 0
    for message_id in range(command_message_id, command_message_id - count - 1, -1):
        if message_id <= 0: continue
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            deleted_count += 1
        except BadRequest:
            pass
    confirmation_msg = await context.bot.send_message(chat_id=chat_id, text=f"✅️ Chat cleaned. {deleted_count} messages deleted.")
    context.job_queue.run_once(delete_message_job, 5, data={'chat_id': chat_id, 'message_id': confirmation_msg.message_id})

async def track_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat: return
    effective_chat_id = get_effective_chat_id(update.effective_chat.id)
    current_phase = SESSION_PHASES.get(effective_chat_id, "links")
    user = update.message.from_user
    text = (update.message.text or "") + " " + (update.message.caption or "")
    
    if current_phase == "links":
        if twitter_regex.search(text):
            for match in twitter_regex.finditer(text):
                c.execute(
                    "INSERT INTO links (chat_id, telegram_user, telegram_name, twitter_user, full_link) VALUES (?, ?, ?, ?, ?)",
                    (effective_chat_id, str(user.id), user.full_name, match.group(2), match.group(1))
                )
            conn.commit()
    elif current_phase == "done":
        if done_regex.search(text):
            c.execute(
                "INSERT OR REPLACE INTO status (chat_id, telegram_user, completed, last_done) VALUES (?, ?, 1, CURRENT_TIMESTAMP)",
                (effective_chat_id, str(user.id))
            )
            conn.commit()
            main = get_main_link(effective_chat_id, str(user.id))
            reply_text = f"✅️ 𝕏 :- @{main[0]}" if main else f"⚠️ {tg_mention(user.full_name, user.id)} No link shared"
            await update.message.reply_text(reply_text, parse_mode="HTML")

@admin_only
async def set_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id)
    if not context.args:
        await update.message.reply_text("**Usage:** `/set <link>`\n**Example:** `/set x.com/your_user`")
        return
    new_link = context.args[0]
    c.execute("INSERT OR REPLACE INTO group_settings (chat_id, tracking_link) VALUES (?, ?)", (effective_chat_id, new_link))
    conn.commit()
    await update.message.reply_text(f"✅ Tracking link set to: `{new_link}`", parse_mode="MarkdownV2")

@admin_only
async def tracking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id)
    SESSION_PHASES[effective_chat_id] = "done"
    chat = update.effective_chat
    if "[OPEN]" in chat.title:
        await chat.set_title(chat.title.replace("[OPEN]", "[CLOSED]"))
    elif "[CLOSED]" not in chat.title:
        await chat.set_title(chat.title.strip() + " [CLOSED]")
    await enable_chat(chat)
    c.execute("SELECT tracking_link FROM group_settings WHERE chat_id = ?", (effective_chat_id,))
    tracking_link = (c.fetchone() or [DEFAULT_TRACKING_LINK])[0]
    
    ist_tz = pytz.timezone('Asia/Kolkata')
    deadline_time = datetime.now(ist_tz) + timedelta(hours=1)
    deadline_str = deadline_time.strftime("%I:%M %p IST")

    message_text = (
        " Timeline Updated 👇\n\n"
        f" {tracking_link}\n\n"
        "Like all posts of the TL account and\n"
        "Drop 'done' (or 'ad', 'completed') to be marked safe ✅\n\n"
        f"⚠️ DEADLINE: {deadline_str}"
    )
    try:
        sent_message = await context.bot.send_message(chat_id=chat.id, text=message_text, disable_web_page_preview=False)
        await context.bot.pin_chat_message(chat_id=chat.id, message_id=sent_message.message_id)
    except BadRequest as e:
        error_text = "⚠️ Could not pin message. Please grant 'Pin Messages' admin rights." if "rights" in str(e).lower() else "⚠️ An error occurred while pinning the message."
        await context.bot.send_message(chat_id=chat.id, text=error_text, parse_mode="HTML")

@admin_only
async def mark_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id)
    if not update.message.reply_to_message:
        await update.message.reply_text("⚠️ Please use this command by replying to a user's message.")
        return
    target_user = update.message.reply_to_message.from_user
    c.execute("INSERT OR REPLACE INTO status (chat_id, telegram_user, completed, last_done) VALUES (?, ?, 1, CURRENT_TIMESTAMP)", (effective_chat_id, str(target_user.id)))
    conn.commit()
    main = get_main_link(effective_chat_id, str(target_user.id))
    reply_text = f"✅️ Manually marked {tg_mention(target_user.full_name, target_user.id)} as done."
    if main:
        reply_text += f"\n𝕏 :- @{main[0]}"
    await update.message.reply_text(reply_text, parse_mode="HTML")

@admin_only
async def sr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id)
    if not update.message.reply_to_message:
        await update.message.reply_text("⚠️ Reply to a user's message to add them to the SR list.")
        return
    user = update.message.reply_to_message.from_user
    c.execute("INSERT OR REPLACE INTO srlist (chat_id, telegram_user, telegram_name) VALUES (?, ?, ?)", (effective_chat_id, str(user.id), user.full_name))
    conn.commit()
    await update.message.reply_text(f"⚠️ {tg_mention(user.full_name, user.id)} your likes are not visible.\nSend a screen recording with a visible profile.", parse_mode="HTML")

@admin_only
async def srlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id)
    c.execute("SELECT telegram_user, telegram_name FROM srlist WHERE chat_id = ?", (effective_chat_id,))
    rows = c.fetchall()
    if not rows:
        await update.message.reply_text("✅ SR list is empty.")
        return
    msg = "📹 **SR List (pending recordings):**\n\n"
    for idx, (tg_user, name) in enumerate(rows, 1):
        msg += f"{idx}. 🙍🏻‍♂️ {tg_mention(name, tg_user)}\n"
    await update.message.reply_text(msg, parse_mode="HTML")

async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat: return
    effective_chat_id = get_effective_chat_id(update.effective_chat.id)
    user = update.message.from_user
    c.execute("SELECT 1 FROM srlist WHERE chat_id = ? AND telegram_user=?", (effective_chat_id, str(user.id)))
    is_in_srlist = c.fetchone()
    c.execute("INSERT OR REPLACE INTO status (chat_id, telegram_user, completed, last_done) VALUES (?, ?, 1, CURRENT_TIMESTAMP)",(effective_chat_id, str(user.id)))
    if is_in_srlist:
        c.execute("DELETE FROM srlist WHERE chat_id = ? AND telegram_user=?", (effective_chat_id, str(user.id)))
        conn.commit()
        await update.message.reply_text("✅ Screen recording received. You are marked as 'done' and removed from the SR list.")
    else:
        conn.commit()
        main = get_main_link(effective_chat_id, str(user.id))
        reply_text = f"✅️ 𝕏 :- @{main[0]}" if main else "✅️ Marked as done."
        await update.message.reply_text(reply_text, parse_mode="HTML")

@admin_only
async def muteall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id)
    duration_str = context.args[0] if context.args else None
    until_timestamp = None
    if duration_str:
        match = re.match(r"(\d+)([dhm])", duration_str.lower())
        if match:
            value, unit = int(match.group(1)), match.group(2)
            delta = {'d': timedelta(days=value), 'h': timedelta(hours=value), 'm': timedelta(minutes=value)}.get(unit)
            if delta:
                until_timestamp = int((datetime.utcnow() + delta).timestamp())
        else:
            await update.message.reply_text("⚠️ Invalid duration format. Use 1d, 2h, 30m etc.")
            return
    c.execute("SELECT DISTINCT telegram_user, telegram_name FROM links WHERE chat_id = ?", (effective_chat_id,))
    all_users = {u[0]: u[1] for u in c.fetchall()}
    c.execute("SELECT telegram_user FROM status WHERE chat_id = ? AND completed=1", (effective_chat_id,))
    completed = {u[0] for u in c.fetchall()}
    c.execute("SELECT telegram_user, telegram_name FROM srlist WHERE chat_id = ?", (effective_chat_id,))
    sr_users = {u[0]: u[1] for u in c.fetchall()}
    c.execute("SELECT telegram_user FROM whitelist WHERE chat_id = ?", (effective_chat_id,))
    whitelisted_users = {row[0] for row in c.fetchall()}
    unsafe_users = set(all_users.keys()) - completed
    final_users_to_mute = (unsafe_users.union(sr_users.keys())) - whitelisted_users
    if not final_users_to_mute:
        await update.message.reply_text("✅ No users to mute.")
        return
    chat = update.effective_chat
    muted_list_msgs, failed = [], []
    for tg_user in final_users_to_mute:
        try:
            await chat.restrict_member(int(tg_user), ChatPermissions(can_send_messages=False), until_date=until_timestamp)
            name = all_users.get(tg_user) or sr_users.get(tg_user, f"ID: {tg_user}")
            muted_list_msgs.append(f"🙍🏻‍♂️ {tg_mention(name, tg_user)}")
        except Exception as e:
            failed.append((tg_user, str(e)))
    msg = "🔇 **Muted users (unsafe + SR list):**\n\n" + "\n".join(muted_list_msgs)
    if duration_str: msg += f"\n\n⏱ **Duration:** {duration_str}"
    if failed: msg += "\n\n❌ **Failed to mute:**\n" + "\n".join([f"- {tg} ({err})" for tg, err in failed])
    await update.message.reply_text(msg, parse_mode="HTML")

@admin_only
async def close_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    effective_chat_id = get_effective_chat_id(update.effective_chat.id)
    c.execute("DELETE FROM links WHERE chat_id = ?", (effective_chat_id,))
    c.execute("DELETE FROM status WHERE chat_id = ?", (effective_chat_id,))
    c.execute("DELETE FROM srlist WHERE chat_id = ?", (effective_chat_id,))
    conn.commit()
    chat = update.effective_chat
    await disable_chat(chat)
    new_title = chat.title.replace("[OPEN]", "").replace("[CLOSED]", "").strip() + " [CLOSED]"
    await chat.set_title(new_title)
    if effective_chat_id in SESSION_PHASES:
        del SESSION_PHASES[effective_chat_id]
    await update.message.reply_text("🗑️ **Session closed. All data cleared!** 🔒 Chat is now OFF.")

@admin_only
async def lock_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await disable_chat(update.effective_chat)
    await update.message.reply_text("🔒 Chat is now OFF. Wait for TL update.")

# === GROUP CONNECTION COMMANDS ===
@admin_only
async def connect_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Usage: `/connect <target_group_id>`", parse_mode="MarkdownV2")
        return
    try:
        target_chat_id = int(context.args[0])
        if target_chat_id > 0:
            await update.message.reply_text("⚠️ Error: Target ID must be a valid group ID (e.g., -100123456).")
            return
    except (ValueError, IndexError):
        await update.message.reply_text("⚠️ Invalid Target Group ID. It must be a number.")
        return
    c.execute("INSERT OR REPLACE INTO group_connections (chat_id, target_chat_id) VALUES (?, ?)", (chat_id, target_chat_id))
    conn.commit()
    await update.message.reply_text(f"🔗 This group's data is now connected to group ` {target_chat_id}`.", parse_mode="HTML")

@admin_only
async def disconnect_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    c.execute("DELETE FROM group_connections WHERE chat_id = ?", (chat_id,))
    conn.commit()
    await update.message.reply_text("🔌 This group is now disconnected and will use its own local data.")

@admin_only
async def connection_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    c.execute("SELECT target_chat_id FROM group_connections WHERE chat_id = ?", (chat_id,))
    row = c.fetchone()
    if row:
        await update.message.reply_text(f"🔗 This group shares data with group ` {row[0]}`.", parse_mode="HTML")
    else:
        await update.message.reply_text("🏠 This group is using its own local data.")

# === MAIN FUNCTION ===
def main():
    BOT_TOKEN = os.getenv("BOT_TOKEN") or "8374636357:AAFfnTJxR7P33lpsPHoOjclZTR1igKhXaNw" 
    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("ERROR: Please replace 'YOUR_BOT_TOKEN_HERE' with your actual bot token.")
        return

    app = Application.builder().token(BOT_TOKEN).build()

    # Register all handlers
    app.add_handler(CommandHandler("open", open))
    app.add_handler(CommandHandler("tracking", tracking))
    app.add_handler(CommandHandler("close", close_session))
    app.add_handler(CommandHandler("l", lock_chat))
    app.add_handler(CommandHandler("users", users))
    app.add_handler(CommandHandler("list", list_users))
    app.add_handler(CommandHandler("link", get_links))
    app.add_handler(CommandHandler("unsafe", unsafe))
    app.add_handler(CommandHandler("multiple_link", multiple_link))
    app.add_handler(CommandHandler("muteall", muteall))
    app.add_handler(CommandHandler("clean", clean_chat))
    app.add_handler(CommandHandler("sr", sr))
    app.add_handler(CommandHandler("srlist", srlist))
    app.add_handler(CommandHandler("ad", mark_done))
    app.add_handler(CommandHandler("save", save_user))
    app.add_handler(CommandHandler("unsave", unsave_user))
    app.add_handler(CommandHandler("savelist", list_saved_users))
    app.add_handler(CommandHandler("set", set_link))
    app.add_handler(CommandHandler("connect", connect_group))
    app.add_handler(CommandHandler("disconnect", disconnect_group))
    app.add_handler(CommandHandler("connection_status", connection_status))

    # Message handlers
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, track_message))
    app.add_handler(MessageHandler(filters.VIDEO, handle_video))

    print("Bot is running...")
    app.run_polling()

if __name__ == "__main__":
    main()