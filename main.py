import firebase_admin
from firebase_admin import credentials, firestore
from collections import Counter
from zoneinfo import ZoneInfo
from telegram import Update, ReplyKeyboardMarkup, InputFile
from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, filters, ContextTypes
import asyncio
import io
import os
import json
# -------- CONFIG --------
BOT_TOKEN = "8310408838:AAHl5v7134Vl1zefwHAllR8mymZjKPpgFlc"
COLLECTION = "Hotmail"
CAIRO = ZoneInfo("Africa/Cairo")
PASSWORD = "@2468@As"

# ---------- Firebase ----------
cred_json = os.getenv("Firebase_CREDENTIALS")
cred_dict = json.loads(cred_json)

cred = credentials.Certificate(cred_dict)
firebase_admin.initialize_app(cred)

db = firestore.client()

# ---------- keyboard ----------
keyboard = [
    ["📊 Statistics", "📈 Activity Chart"],
    ["📁 Extract Data", "📡 Start Live Counter", "🛑 Stop Live Counter"],
    ["🗑 Delete Data"]
]
markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

# ---------- authenticated users ----------
authenticated_users = {}

# ---------- live counter tasks ----------
live_tasks = {}  # user_id -> asyncio.Task

# ---------- fast stream ----------
def fast_stream(batch=2000):
    query = db.collection(COLLECTION).limit(batch)
    while True:
        docs = list(query.stream())
        if not docs:
            break
        for d in docs:
            yield d
        query = db.collection(COLLECTION).start_after(docs[-1]).limit(batch)

# ---------- hour format ----------
def hour12(h):
    suffix = "AM" if h < 12 else "PM"
    h = h % 12
    if h == 0:
        h = 12
    return f"{h}{suffix}"

# ---------- start ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user.id
    if authenticated_users.get(user):
        await update.message.reply_text("Welcome back to Firestore Manager Bot", reply_markup=markup)
    else:
        await update.message.reply_text("Welcome! Please enter the password to continue:")

# ---------- statistics ----------
async def statistics():
    total = 0
    servers = Counter()
    for doc in fast_stream():
        data = doc.to_dict()
        total += 1
        server_name = data.get("PrimarySource", "Unknown")
        servers[server_name] += 1

    msg = f"📊 Total Records: {total}\n\nServer Counts:\n"
    for s, c in servers.items():
        msg += f"{s} : {c}\n"

    return msg

# ---------- chart ----------
async def chart():
    hours = Counter()
    for doc in fast_stream():
        created = doc.to_dict().get("created_at")
        if created:
            hours[created.astimezone(CAIRO).hour] += 1
    max_v = max(hours.values()) if hours else 1
    text = "📈 Activity Chart\n\n"
    for h in range(24):
        count = hours[h]
        bar = int((count / max_v) * 20) if max_v else 0
        text += f"{hour12(h):>4} | {'█'*bar} {count}\n"
    return text

# ---------- extract (send file to user) ----------
async def extract(update: Update):
    buffer = io.StringIO()
    count = 0
    for doc in fast_stream():
        fs = doc.to_dict().get("final_string")
        if fs:
            buffer.write(fs + "\n")
            count += 1
    buffer.seek(0)
    await update.message.reply_document(document=InputFile(buffer, filename="extracted_data.txt"),
                                        caption=f"✅ Extracted {count} records")

# ---------- live counter ----------
async def live_counter(update: Update):
    user = update.message.from_user.id

    if user in live_tasks:
        await update.message.reply_text("⚠ Live counter is already running! Press 🛑 Stop Live Counter to stop it.")
        return

    msg = await update.message.reply_text("📡 Live counter started. Press 🛑 Stop Live Counter to stop.")

    async def counter_task():
        last_count = db.collection(COLLECTION).count().get()[0][0].value
        try:
            while True:
                await asyncio.sleep(1)  # تحديث كل ثانية
                now_count = db.collection(COLLECTION).count().get()[0][0].value
                if now_count > last_count:
                    diff = now_count - last_count
                    await msg.edit_text(f"➕ {diff} new records | Total: {now_count}")
                    last_count = now_count
        except asyncio.CancelledError:
            await msg.edit_text("🛑 Live counter stopped.")

    task = asyncio.create_task(counter_task())
    live_tasks[user] = task

async def stop_live(update: Update):
    user = update.message.from_user.id
    task = live_tasks.get(user)
    if task:
        task.cancel()
        del live_tasks[user]
    else:
        await update.message.reply_text("No live counter is running.")

# ---------- delete with batch (simulate deleting collection) ----------
delete_confirmed = {}  # user_id -> bool

async def delete_warning(update, context):
    user = update.message.from_user.id
    delete_confirmed[user] = False
    await update.message.reply_text(
        "⚠ WARNING: You are about to DELETE the entire FC DATA collection!\n"
        "Type CONFIRM DELETE once to continue."
    )

async def delete_confirm(update):
    user = update.message.from_user.id
    if delete_confirmed.get(user) is False:
        delete_confirmed[user] = True
        await update.message.reply_text(
            "⚠ Are you absolutely sure? This action CANNOT be undone!\n"
            "Type CONFIRM DELETE again to actually delete the entire collection."
        )
    else:
        coll_ref = db.collection("FC DATA")
        deleted = 0
        # حذف بالـ batch لضمان كل المستندات تمسح → الكولكشن تختفي بعد حذف كل مستند
        while True:
            docs = list(coll_ref.limit(500).stream())
            if not docs:
                break
            batch = db.batch()
            for doc in docs:
                batch.delete(doc.reference)
            batch.commit()
            deleted += len(docs)

        delete_confirmed[user] = False
        await update.message.reply_text(f"🗑 Deleted {deleted} documents. FC DATA collection is now gone!")

# ---------- message handler ----------
async def handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user.id
    text = update.message.text

    # أول مرة: تحقق من الباسورد
    if not authenticated_users.get(user):
        if text == PASSWORD:
            authenticated_users[user] = True
            await update.message.reply_text("✅ Password correct!", reply_markup=markup)
        else:
            await update.message.reply_text("❌ Wrong password. Please try again:")
        return

    # بعد المصادقة
    if text == "📊 Statistics":
        msg = await statistics()
        await update.message.reply_text(msg)
    elif text == "📈 Activity Chart":
        msg = await chart()
        await update.message.reply_text(msg)
    elif text == "📁 Extract Data":
        await extract(update)
    elif text == "📡 Start Live Counter":
        await live_counter(update)
    elif text == "🛑 Stop Live Counter":
        await stop_live(update)
    elif text == "🗑 Delete Data":
        await delete_warning(update, context)
    elif text == "CONFIRM DELETE":
        await delete_confirm(update)

# ---------- main ----------
app = ApplicationBuilder().token(BOT_TOKEN).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle))

print("Bot started...")
app.run_polling()
