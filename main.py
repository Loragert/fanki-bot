# ==============================
# FANki BOT — SUPABASE VERSION
# =============================

import re
import logging
import traceback
import asyncio
from datetime import datetime
import os

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)

from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters
)

from supabase import create_client

# ==============================
# ПЕРЕКЛАД ТИПІВ ЗАВДАНЬ
# ==============================

TASK_TEXT = {
    "like": "👍 Лайкнути пост",
    "follow": "➕ Підписатися",
    "video_view": "📺 Переглянути відео",
    "comment": "💬 Залишити коментар"
}
SOCIALS = {
    "instagram": "Instagram",
    "tiktok": "TikTok",
    "facebook": "Facebook",
    "youtube": "YouTube",
    "telegram": "Telegram",
    "google": "Google Maps"
}

# ==============================
# CONFIG
# ==============================

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не знайдено")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE credentials not found")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

ADMIN_ID = [6699691752]
TASK_MODERATOR_ID = 877030342
TASK_AUTO_ID = 7489327594

def is_admin(user_id):
    return user_id in ADMIN_ID

# ==============================
# ERROR NOTIFYy
# ==============================

def notify_admin_async(error_text):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(send_admin_error(error_text))
    except:
        pass


async def send_admin_error(error_text):
    try:
        await app.bot.send_message(
            ADMIN_ID[0],
            f"🚨 ПОМИЛКА В БОТІ:\n\n{error_text[:3500]}"
        )
    except:
        pass

# ==============================
# STATE
# ==============================

user_state = {}
admin_state = {}
user_selected_social = {}
user_selected_account = {}
user_binance_id = {}
user_withdraw_amount = {}
current_task = {}
skipped_tasks = {}


# ==============================
# DATABASE HELPERS
# ==============================

def get_users():
    return supabase.table("Users").select("*").execute().data


def get_tasks():
    return supabase.table("Tasks").select("*").execute().data


def get_templates():
    return supabase.table("TaskTemplates").select("*").execute().data


def get_accounts():
    accounts = (
        supabase
        .table("Accounts")
        .select("*")
        .eq("in_cabinet", True)
        .execute()
        .data
    )
    return accounts


def get_withdrawals():
    return supabase.table("Withdrawals").select("*").execute().data


def get_comments():
    return supabase.table("Comment_Pool").select("*").execute().data


# ==============================
# BALANCE FUNCTIONS
# ==============================

def get_user_data(user_id):

    users = get_users()

    for row in users:

        if str(row.get("telegram_id")) == str(user_id):

            balance = int(row.get("balance") or 0)
            total = int(row.get("total") or 0)
            status = row.get("status") or "Active"

            return balance, total, status

    return 0, 0, "Active"


def update_user_balance(user_id, amount):

    users = get_users()

    for row in users:

        if str(row.get("telegram_id")) == str(user_id):

            balance = int(row.get("balance") or 0)

            supabase.table("Users").update({
                "balance": balance + amount
            }).eq("telegram_id", user_id).execute()

            return


def deduct_user_balance(user_id, amount):

    users = get_users()

    for row in users:

        if str(row.get("telegram_id")) == str(user_id):

            balance = int(row.get("balance") or 0)

            supabase.table("Users").update({
                "balance": balance - amount
            }).eq("telegram_id", user_id).execute()

            return


def add_to_user_total(user_id, amount):

    users = get_users()

    for row in users:

        if str(row.get("telegram_id")) == str(user_id):

            total = int(row.get("total") or 0)

            supabase.table("Users").update({
                "total": total + amount
            }).eq("telegram_id", user_id).execute()

            return
# ==============================
# USER STATS
# ==============================

def get_user_stats(user_id):

    users = get_users()
    reg_date = "_"
    for row in users:
        if str(row.get("telegram_id")) == str(user_id):
            reg_date = row.get("register") or "_"
            break
    res = (
        supabase
        .table("Tasks")
        .select("id", count="exact")
        .eq("telegram_id", user_id)
        .eq("status", "Approved")
        .execute()
    )
    completed_tasks = res.count if res.count else 0


    return reg_date, completed_tasks

# ==============================
# 👤 CABINET UI
# ==============================

def generate_profile_text(
    fanki_balance,
    tasks_total,
    tasks_today,
    earned_total,
    earned_today,
    reg_date,
    status="Активний",
    min_withdraw_fanki=1000
):

    usd_balance = round(fanki_balance / 1000, 2)

    withdraw_available = fanki_balance >= min_withdraw_fanki

    if withdraw_available:
        withdraw_status = "🟢 Мінімальний вивід доступний"
    else:
        withdraw_status = "🔴 Мінімальний вивід недоступний"

    progress = min(fanki_balance / min_withdraw_fanki, 1)
    percent = int(progress * 100)

    bars = int(progress * 10)
    progress_bar = "🟩" * bars + "⬜" * (10 - bars)

    text = (
f"👤 Ваш кабінет\n\n"
f"💰 Баланс: {fanki_balance} Fanki (≈ {usd_balance}$)\n\n"
f"💸 Доступно до виводу: {usd_balance}$\n"
f"{withdraw_status}\n\n"
f"📈 Прогрес до виводу\n"
f"{progress_bar} {percent}%\n\n"
f"📊 Завдання\n"
f"✅ Виконано всього: {tasks_total}\n"
f"⚡ Виконано сьогодні: {tasks_today}\n\n"
f"🏆 Заробіток\n"
f"💰 Зароблено всього: {earned_total} Fanki\n"
f"🔥 Зароблено сьогодні: {earned_today} Fanki\n\n"
f"📅 Дата реєстрації: {reg_date}\n"
f"🟢 Статус: {status}"
    )

    return text
    
# ==============================
# USER PROFILE DATA (CABINET)
# ==============================

async def get_user_profile_data(user_id):

    user = supabase.table("Users")\
        .select("*")\
        .eq("telegram_id", int(user_id))\
        .execute().data

    if not user:
        return None

    user = user[0]

    from datetime import datetime

    # --- БАЛАНС ---
    fanki_balance = int(user.get("balance", 0))
    from datetime import datetime

    reg_date_raw = user.get("register")

    if reg_date_raw:
        dt = datetime.fromisoformat(reg_date_raw)
        reg_date = dt.strftime("%d.%m.%Y")
    else:
        reg_date = "—"

    # --- ВСІ ЗАВДАННЯ ---
    tasks = supabase.table("Tasks")\
        .select("status, task_id, assign_date, approve_date")\
        .eq("telegram_id", user_id)\
        .execute().data

    today = datetime.utcnow().date()

    tasks_total = 0
    tasks_today = 0
    earned_total = 0
    earned_today = 0

    templates = supabase.table("TaskTemplates")\
        .select("task_id, reward")\
        .execute().data

    reward_map = {
        int(t["task_id"]): int(t["reward"])
        for t in templates
    }

    for t in tasks:

        if t.get("status") == "Approved":

            tasks_total += 1

            task_id = int(t.get("task_id") or 0)
            reward = reward_map.get(task_id, 0)
            earned_total += reward

            date_str = t.get("assign_date") or t.get("approve_date")

            if date_str:
                try:
                    d = datetime.fromisoformat(date_str).date()

                    if d == today:
                        tasks_today += 1
                        earned_today += reward
                except:
                    pass

    return {
        "fanki_balance": fanki_balance,
        "tasks_total": tasks_total,
        "tasks_today": tasks_today,
        "earned_total": earned_total,
        "earned_today": earned_today,
        "reg_date": reg_date
    }

# ==============================
# MENU
# ==============================

async def show_main_menu(update: Update):

    user_id = update.effective_user.id
    balance, total, status = get_user_data(user_id)

    if status == "Banned":

        await update.message.reply_text(
            "🚫 Ваш акаунт заблоковано адміністрацією."
        )
        return

    if status == "Under Review":

        await update.message.reply_text(
            "⏳ Ваш акаунт тимчасово на перевірці."
        )
        return

    if is_admin(user_id):

        markup = ReplyKeyboardMarkup(
            [
                ["📢 Розсилка"],
                ["📊 Статистика"],
                ["⬅️ Назад"],
                ["💰 Змінити баланс"],
                ["🔒 Бан користувача"]
            ],
            resize_keyboard=True
        )

        await update.message.reply_text(
            "🛠 Адмін панель\nВітаємо в головному меню, оберіть пункт.",
            reply_markup=markup
        )
        return

    res = supabase.table("Users")\
        .select("id", count="exact")\
        .eq("status", "Active")\
        .execute()

    active_users = res.count if res.count else 0

    markup = ReplyKeyboardMarkup(
        [
            ["👤Мій кабінет"],
            ["➕Реєстрація акаунту"],
            ["ℹ️Інформація про бот"],
            ["📋Завдання"],
            ["🛠Підтримка"],
            ["💸Вивід"]
        ],
        resize_keyboard=True
    )

    text = (
    "👋 Ласкаво просимо до головного меню!\n\n"
    "📌 Тут ви можете:\n\n"
    "➕ Зареєструвати акаунт для роботи\n"
    "ℹ️ Дізнатися інформацію про бот, валюту та методи виводу\n"
    "🛠 Звернутися до підтримки\n"
    "💸 Подати заявку на вивід коштів\n"
    "📋 Отримати завдання\n\n"
    "📢 Новини, оновлення та інструкції:https://t.me/+XnLg96cCpKpkMjA8 \n"
    "💬 Спільнота користувачів\n"
    "⚠️ Для отримання завдань необхідно спочатку зареєструвати акаунт.\n\n"
    f"👥 Активних користувачів: {active_users}\n\n"
    "👇 Оберіть потрібний пункт нижче"
    )

    await update.message.reply_text(text, reply_markup=markup)


# ==============================
# START
# ==============================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):

    user_id = update.effective_user.id
    username = update.effective_user.username or update.effective_user.first_name

    markup = ReplyKeyboardMarkup(
        [["Приймаю"]],
        resize_keyboard=True
    )

    user_state[user_id] = "await_accept"

    text = f"""
👋Привіт, {username}! Раді вітати вас у FankiBoti.

📌 Правила роботи:

1️⃣ Виконуйте завдання чесно та додавайте повний скріншот виконання  
2️⃣ Використовуйте лише власні акаунти  
3️⃣ Чітко дотримуйтесь інструкцій  
4️⃣ Не надавайте чужі акаунти  

⚠️ За порушення правил або спробу обману доступ буде закрито.
Натискаючи кнопку «Приймаю», ви погоджуєтесь із правилами платформи.
"""

    await update.message.reply_text(
        text,
        reply_markup=markup
    )


# ==============================
# SAFE EDIT
# ==============================

async def safe_edit_caption(query, text):

    try:
        await query.edit_message_caption(text)
    except:
        pass


# ==============================
# CALLBACK HANDLER
# ==============================

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):

    query = update.callback_query
    if not query:
        return

    await query.answer()

    data_raw = query.data or ""

    if "|" not in data_raw:
        return

    action, record_id = data_raw.split("|", 1)

    try:

        # =========================
        # ACCOUNT APPROVE / REJECT
        # =========================

        if action in ["account_approve", "account_reject"]:

            res = supabase.table("Accounts").select("*").eq("id", record_id).execute()
            if not res.data:
                return

            row = res.data[0]

            if row["status"] != "Pending":
                return

            user_id = row["telegram_id"]
            social = row["social_network"]
            nickname = row["username"]

            if action == "account_approve":

                supabase.table("Accounts").update({
                    "status": "Approved"
                }).eq("id", record_id).execute()

                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=f"✅ Ваш акаунт {nickname} ({social}) підтверджено."
                )

                await query.edit_message_text("✅ Акаунт підтверджено")

            else:

                supabase.table("Accounts").update({
                    "status": "Rejected"
                }).eq("id", record_id).execute()

                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=f"❌ Ваш акаунт {nickname} ({social}) відхилено."
                )

                await query.edit_message_text("❌ Акаунт відхилено")

        # =========================
        # TASK APPROVE / REJECT
        # =========================

        elif action in ["task_approve", "task_reject"]:

            res = supabase.table("Tasks").select("*").eq("id", record_id).execute()
            if not res.data:
                return

            row = res.data[0]

            if row["status"] != "Pending":
                return

            user_id = row["telegram_id"]
            task_id = row["task_id"]

            template = supabase.table("TaskTemplates").select("*").eq("task_id", task_id).execute().data

            task_type = template[0]["task_type"] if template else ""
            link = template[0]["link"] if template else ""

            action_text = TASK_TEXT.get(str(task_type).lower(), task_type)

            if action == "task_approve":

                supabase.table("Tasks").update({
                    "status": "Approved",
                    "paid": "Paid",
                    "approve_date": datetime.utcnow().isoformat()
                }).eq("id", record_id).execute()

                template = supabase.table("TaskTemplates").select("*").eq("task_id", task_id).execute().data

                reward = int(template[0]["reward"]) if template else 0

                task_type = template[0]["task_type"]
                link = template[0]["link"]
                action_text = TASK_TEXT.get(str(task_type).lower(), task_type)

                update_user_balance(user_id, reward)
                add_to_user_total(user_id, reward)

                msg = (
                    "✅ Дякуємо за виконання завдання!\n\n"
                    f"🎯 Дія:\n{action_text}\n\n"
                    f"🔗 Посилання:\n{link}\n\n"
                    f"💰 На ваш баланс зараховано:\n{reward} Fanki\n\n"
                    "🚀 Виконуйте більше завдань, щоб заробити більше Fanki!"
                )

                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=msg
                )

                old_caption = query.message.caption or ""
                new_caption = old_caption + "\n\n✅ Підтверджено"

                await safe_edit_caption(query, new_caption)

            else:

                supabase.table("Tasks").update({
                    "status": "Rejected"
                }).eq("id", record_id).execute()

                msg = (
                    "❌ Завдання відхилено\n\n"
                    f"🎯 Дія:\n{action_text}\n\n"
                    f"🔗 Посилання:\n{link}\n\n"
                    "⚠️ Причина: дія виконана неправильно або скріншот не підтверджує виконання.\n\n"
                    "🔄 Ви можете виконати інше завдання."
                )

                await context.bot.send_message(
                  chat_id=int(user_id),
                  text=msg
                )

                old_caption = query.message.caption or ""
                new_caption = old_caption + "\n\n❌ Відхилено"

                await safe_edit_caption(query, new_caption)

        # =========================
        # WITHDRAW APPROVE / REJECT
        # =========================

        elif action in ["withdraw_approve", "withdraw_reject"]:

            res = supabase.table("Withdrawals").select("*").eq("id", record_id).execute()
            if not res.data:
                return

            row = res.data[0]

            if row["status"] != "Pending":
                return

            user_id = row["telegram_id"]
            amount = int(row["amount"])

            if action == "withdraw_approve":

                supabase.table("Withdrawals").update({
                    "status": "Approved"
                }).eq("id", record_id).execute()

                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=(
                        "✅ Ваш вивід підтверджено!\n\n"
                        f"💰 Сума: {amount} Fanki\n"
                        f"🏦 Binance ID: {row['binance_id']}\n\n"
                        "🙏 Дякуємо за участь у Fanki!"
                    )
                )

                await query.edit_message_text("✅ Вивід підтверджено")

            else:

                supabase.table("Withdrawals").update({
                    "status": "Rejected"
                }).eq("id", record_id).execute()

                update_user_balance(user_id, amount)

                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=(
                        "❌ Ваш запит на вивід відхилено\n\n"
                        f"💰 Сума: {amount} Fanki\n"
                        f"🏦 Binance ID: {row['binance_id']}\n\n"
                        "💰 Кошти повернено на ваш баланс."
                    )
                )

                await query.edit_message_text("❌ Вивід відхилено")

    except Exception as e:
        logging.error(f"Callback error: {e}")
# =============================
# SEND NEXT TASK
# =============================

async def send_next_task(update: Update, user_id: str):

    templates = (
        supabase
        .table("TaskTemplates")
        .select("*")
        .order("id")
        .execute()
        .data
    )

    comments = (
        supabase
        .table("Comment_Pool")
        .select("*")
        .eq("active", True)
        .execute()
    ).data

    accounts = supabase.table("Accounts").select("*").execute().data

    if user_id not in user_selected_social:
        await update.message.reply_text("Помилка: соцмережа не вибрана.")
        return

    social_network = user_selected_social[user_id]
    account_name = user_selected_account.get(user_id)
    
    # --- PRELOAD TASK STATS ---
    all_tasks = supabase.table("Tasks")\
        .select("task_id, status, assign_date")\
        .execute().data

    total_done_map = {}
    day_done_map = {}

    from datetime import datetime
    today = datetime.utcnow().date()

    for t in all_tasks:
        if t["status"] != "Approved":
            continue

        tid = int(t["task_id"])

        total_done_map[tid] = total_done_map.get(tid, 0) + 1

        if t.get("assign_date"):
            try:
                d = datetime.fromisoformat(t["assign_date"]).date()
                if d == today:
                    day_done_map[tid] = day_done_map.get(tid, 0) + 1
            except:
                pass


    tasks = (
        supabase
        .table("Tasks")
        .select("*")
        .eq("account", account_name)
        .eq("social_network", social_network)
        .execute()
    ).data

    account_row = next(
        (
            r for r in accounts
            if str(r.get("telegram_id")) == str(user_id)
            and r.get("username") == account_name
            and r.get("status") == "Approved"
        ),
        None
    )

    if not account_row:
        await update.message.reply_text("Акаунт не підтверджений.")
        return

    # ✅ ГЕНДЕР КОРИСТУВАЧА
    user_gender = str(account_row.get("gender") or "all").strip().lower()

    # ✅ РЕГІОН КОРИСТУВАЧА
    user_region = str(account_row.get("region") or "all").strip().lower()

    # already used task_ids
    done_task_ids = set()

    for r in tasks:
        try:
            done_task_ids.add(int(r.get("task_id")))
        except:
            pass

    for template in templates:

        if not template.get("active"):
            continue

        if str(template.get("social_network")).lower() != str(social_network).lower():
            continue

        # ✅ ФІЛЬТР ПО ГЕНДЕРУ
        task_gender = str(template.get("gender_target") or "all").strip().lower()
        if task_gender != "all" and task_gender != user_gender:
            continue

        # ✅ ФІЛЬТР ПО РЕГІОНУ
        task_region = str(template.get("region_target") or "all").strip().lower()
        if task_region != "all" and task_region != user_region:
            continue

        try:
            task_id = int(template.get("task_id"))
        except:
            continue
            
        # =============================
        # LIMITS (швидка версія)
        # =============================

        max_total = template.get("max_total")
        max_per_day = template.get("max_per_day")

        if max_total:
            if total_done_map.get(task_id, 0) >= max_total:
                continue

        if max_per_day:
            if day_done_map.get(task_id, 0) >= max_per_day:
                continue
                
        if task_id in done_task_ids:
            continue

        if task_id in skipped_tasks.get((user_id, account_name), set()):
            continue

        task_type = template.get("task_type")

        # --- COMMENT TIMER (НЕ ЧІПАЄМО) ---
        if str(task_type).lower() == "comment":
            recent_comments = supabase.table("Tasks")\
                .select("account")\
                .neq("comment_text", "")\
                .order("assign_date", desc=True)\
                .limit(180)\
                .execute()

            recent_accounts = [r["account"] for r in recent_comments.data]

            if account_name in recent_accounts:
                continue

            last_comment = supabase.table("Tasks").select("assign_date") \
                .eq("task_id", task_id) \
                .neq("comment_text", "") \
                .order("assign_date", desc=True) \
                .limit(1).execute()

            if last_comment.data:
                from datetime import datetime, timedelta

                last_time = datetime.fromisoformat(last_comment.data[0]["assign_date"])
                now_time = datetime.now(last_time.tzinfo)

                if (now_time - last_time) < timedelta(minutes=40):
                    continue

        link = (template.get("link") or "").strip()
        reward = template.get("reward")
        action_text = TASK_TEXT.get(str(task_type).lower(), task_type)

        comment_text = ""
        comment_row_id = None

        if str(task_type).lower() == "comment":

    # ✅ 1. СПОЧАТКУ: гендер + регіон
            available_comments = [
                c for c in comments
                if int(c.get("task_id")) == int(task_id)
                and c.get("active") == True
                and str(c.get("gender") or "all").strip().lower() == user_gender
                and str(c.get("region") or "all").strip().lower() == user_region
            ]

    # ✅ 2. FALLBACK: гендер + all регіон
            if not available_comments:
                available_comments = [
                    c for c in comments
                    if int(c.get("task_id")) == int(task_id)
                    and c.get("active") == True
                    and str(c.get("gender") or "all").strip().lower() == user_gender
                    and str(c.get("region") or "all").strip().lower() == "all"
            ]

    # ✅ 3. FALLBACK: all гендер + all регіон
            if not available_comments:
                available_comments = [
                    c for c in comments
                    if int(c.get("task_id")) == int(task_id)
                    and c.get("active") == True
                    and str(c.get("gender") or "all").strip().lower() == "all"
                    and str(c.get("region") or "all").strip().lower() == "all"
            ]

            if not available_comments:
                continue

            comment = available_comments[0]
            comment_text = comment.get("comment")
            comment_row_id = comment.get("id")

            # РЕЗЕРВАЦІЯ
            from datetime import datetime

            supabase.table("Tasks").insert({
                "telegram_id": user_id,
                "social_network": social_network,
                "account": account_name,
                "task_id": task_id,
                "link": link,
                "status": "Reserved",
                "assign_date": datetime.utcnow().isoformat(),
                "comment_text": comment_text
            }).execute()

            # ВИМИКАЄМО КОМЕНТАР
            supabase.table("Comment_Pool").update(
                {"active": False}
            ).eq("id", comment_row_id).execute()

        current_task[user_id] = {
            "task_id": task_id,
            "social": social_network,
            "type": task_type,
            "link": link,
            "reward": reward,
            "comment": comment_text,
            "comment_row_id": comment_row_id
        }

        if str(task_type).lower() == "comment":

            msg = (
                "📌 Завдання\n\n"
                "⚠️ Обов'язково зробіть скрін виконаної дії!\n\n"
                "❗ Важливо:\n"
                "• Скопіюйте текст коментаря під завданням 👇\n"
                "• Не залишайте два однакових коментарі\n"
                "• Обов'язково поставити 5 зірок✨(для гугл мапс)\n"
                "• Перед публікацією перевірте, чи такого коментаря ще немає під постом\n\n"
                f"🔗 Посилання:\n{link}\n\n"
                f"🎯 Дія:\n{action_text}\n\n"
                f"💰 Нагорода:\n{reward} Fanki\n\n"
            )
            await update.message.reply_text(msg)
            await update.message.reply_text(comment_text)

        else:

            msg = (
                "📌 Завдання\n\n"
                "⚠️ Обов'язково зробіть скрін виконаної дії!\n\n"
                f"🔗 Посилання:\n{link}\n\n"
                f"🎯 Дія:\n{action_text}\n\n"
                f"💰 Нагорода:\n{reward} Fanki\n\n"
            )
            await update.message.reply_text(msg)

        buttons = [
            ["✅ Виконано"],
            ["⏭ Пропустити"],
            ["⬅️ Назад"]
        ]

        markup = ReplyKeyboardMarkup(
            buttons,
            resize_keyboard=True
        )

        await update.message.reply_text(
            "✅Після виконання натисніть кнопку нижче.",
            reply_markup=markup
        )

        user_state[user_id] = "working"
        return

    user_state[user_id] = "select_account"

    await update.message.reply_text(
        "Немає доступних завдань.",
        reply_markup=ReplyKeyboardMarkup([["⬅️ Назад"]], resize_keyboard=True)
    )



# ==============================
# USER MESSAGE HANDLER
# ==============================

async def handle_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE):

    user_id = update.effective_user.id
    username = update.effective_user.username or update.effective_user.first_name
    text = update.message.text if update.message.text else ""
    now = datetime.utcnow().isoformat()

    balance, total, status = get_user_data(user_id)

    if status == "Banned":
        await update.message.reply_text("🚫 Ваш акаунт заблоковано адміністрацією.")
        return

    if status == "Under Review":
        await update.message.reply_text("⏳ Ваш акаунт тимчасово на перевірці.")
        return
    # --------  ACCOUNT ----------
    if text.startswith("/remove_"):
        
        account_id = text.replace("/remove_", "").strip()

        supabase.table("Accounts").update({
            "in_cabinet": False
        }).eq("id", account_id).execute()

        await update.message.reply_text("✅ Акаунт видалено з кабінету.")

        return
    # ---------------- BACK ----------------

    if text in ["⬅️ Назад", "Назад", "/cancel"]:

        user_state.pop(user_id, None)
        admin_state.pop(user_id, None)
        user_selected_social.pop(user_id, None)
        user_selected_account.pop(user_id, None)
        user_binance_id.pop(user_id, None)
        user_withdraw_amount.pop(user_id, None)
        current_task.pop(user_id, None)

        await show_main_menu(update)
        return

    # ---------------- WITHDRAW BUTTON ----------------

    if text == "💸 Вивід":
        user_state.pop(user_id, None)
        return await handle_withdraw(update, context)

    state = user_state.get(user_id)

    # ---------------- ACCEPT RULES ----------------

    if state == "await_accept" and text == "Приймаю":

        users = get_users()

        exists = any(str(r.get("telegram_id")) == str(user_id) for r in users)

        if not exists:

            supabase.table("Users").insert({
                "telegram_id": user_id,
                "username": username,
                "register": now,
                "balance": 0,
                "total": 0,
                "status": "Active"
            }).execute()

        user_state[user_id] = None

        await show_main_menu(update)
        return

    # ---------------- CABINET ----------------

    if text == "👤Мій кабінет":
        
        data = await get_user_profile_data(user_id)
        
        if not data:
            await update.message.reply_text("🧱Помилка завантаження профілю")
            return
            
        profile_text = generate_profile_text(**data)

        await update.message.reply_text(profile_text)
        return

    # ---------------- INFO ----------------

    if text == "ℹ️Інформація про бот":

        await update.message.reply_text(
            "🤖Про платформу🚀\n"
            "Ми допомагаємо блогерам підтримувати активність у соціальних мережах.\n\n"
            "Працюємо з платформами:\n"
            "• TikTok\n"
            "• Instagram\n"
            "• Facebook\n\n"
            "• YouTube\n\n"
            "• Telegram\n\n"
            "• Google Maps\n\n"
            "💼 Ви можете працювати з декількох власних акаунтів кожної соціальної мережі.\n\n"
            "📢 У нашому офіційному каналі публікуються:\n\n"
            "• новини\n\n"
            "• оновлення\n\n"
            "• інструкції\n\n"
            "• корисна інформація по роботі\n\n"

           "👇 Канал:https://t.me/+XnLg96cCpKpkMjA8\n\n"


           "💬 Група обговорення та допомоги:\n\n"
           "https://t.me/+5_xjkinfaaw3OGQ0"
        )
        return

    # ---------------- SUPPORT ----------------

    if text == "🛠Підтримка":

        await update.message.reply_text(
            "📩 Підтримка:\n"
            "Якщо у вас виникли питання або проблеми під час роботи — зверніться до адміністратора.\n\n"

            "💬 Також ви можете поставити питання у спільноті користувачів,\n\n"
            "де учасники та адміністрація допомагають вирішувати проблеми та діляться досвідом."
        )
        return

    # ---------------- REGISTER ACCOUNT ----------------

    if text == "➕Реєстрація акаунту":

        buttons = [[name] for name in SOCIALS.values()]
        buttons.append(["⬅️ Назад"])

        markup = ReplyKeyboardMarkup(
            buttons,
            resize_keyboard=True
        )

        user_state[user_id] = "await_social"

        await update.message.reply_text(
            "📋 Перед реєстрацією акаунту ознайомтесь з вимогами.\n\n"

            "Instagram / TikTok / Facebook:\n"
            "• Акаунт має бути відкритий\n"
            "• Мінімум 50 підписників\n"
            "• Мінімум 5 постів\n\n"

            "Google Maps (Google акаунт):\n"
            "Профіль має бути активним та виглядати природно. Бажано, щоб він містив:\n"
            "• Фото профілю\n"
            "• Ім’я та прізвище (можна вигадані)\n"
            "• 8–15 відгуків\n"
            "• Відгуки в різних містах\n"
            "• Різні оцінки (не лише 5⭐)\n"
            "• Кілька фото у відгуках\n"
            "• Лайки чужих відгуків\n\n"

            "⚠️ Акаунти, що не відповідають вимогам, будуть відхилені.\n\n"
            "Оберіть соціальну мережу:",
            reply_markup=markup
        )

    # ---------------- SELECT SOCIAL ----------------

    if state == "await_social":

        allowed_socials = list(SOCIALS.values())

        if text not in allowed_socials:
            await update.message.reply_text("🌍Оберіть соцмережу кнопкою")
            return

        user_selected_social[user_id] = text
        user_state[user_id] = "await_nick"

        await update.message.reply_text("✍️Введіть нік без @:")
        return

    # ---------------- ENTER NICK ----------------

    if state == "await_nick":

        if len(text) < 2:
            await update.message.reply_text("🤷‍♂️Занадто коротке ім’я.")
            return

        accounts = get_accounts()

        if any(
            str(row.get("username")).lower() == text.lower()
            for row in accounts
        ):
            await update.message.reply_text("🤷‍♂️Це ім’я вже зареєстроване.")
            return

        user_selected_account[user_id] = text
        user_state[user_id] = "await_link"

        await update.message.reply_text("✍️Введіть посилання на профіль:")
        return

    # ---------------- ENTER LINK ----------------

    if state == "await_link":

        link = text.strip()

        if not link.startswith("http"):
            await update.message.reply_text("💡Посилання має починатися з http або https.")
            return

        accounts = get_accounts()

        for row in accounts:

            if (row.get("profile_link") or "").lower() == link.lower():
                await update.message.reply_text("🤷‍♂️Цей профіль вже зареєстрований.")
                return

        res = supabase.table("Accounts").insert({
            "telegram_id": user_id,
            "social_network": user_selected_social[user_id],
            "username": user_selected_account[user_id],
            "status": "Pending",
            "request_date": now,
            "profile_link": link
        }).execute()

        record_id = res.data[0]["id"]

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "✅ Підтвердити",
                    callback_data=f"account_approve|{record_id}"
                ),
                InlineKeyboardButton(
                    "❌ Відхилити",
                    callback_data=f"account_reject|{record_id}"
                )
            ]
        ])

        await context.bot.send_message(
            ADMIN_ID[0],
            f"Новий акаунт\n\n"
            f"User ID: {user_id}\n"
            f"Соцмережа: {user_selected_social[user_id]}\n"
            f"Ім'я: {user_selected_account[user_id]}\n"
            f"Посилання: {link}",
            reply_markup=keyboard
        )

        user_state[user_id] = None

        await update.message.reply_text("🔎Акаунт відправлено на модерацію.")
        return
# ---------------- TASKS MENU ----------------

    if text == "📋Завдання":

        accounts = get_accounts()

        approved = [
            row for row in accounts
            if str(row.get("telegram_id")) == str(user_id)
            and row.get("status") == "Approved"
        ]

        if not approved:
            await update.message.reply_text("⚙️Немає підтверджених акаунтів.")
            return

        socials = {}

        for row in approved:
            socials.setdefault(row.get("social_network"), []).append((row.get("username"), row.get("id")))

        msg = ""

        for social, accs in socials.items():
            msg += f"{social}\n"
            for i, (acc, acc_id) in enumerate(accs, start=1):
                msg += f"{i}. {acc} /remove_{acc_id}\n"
            msg += "\n"

        msg += "Оберіть соціальну мережу."

        markup = ReplyKeyboardMarkup(
            [[s] for s in socials.keys()] + [["⬅️ Назад"]],
            resize_keyboard=True
        )

        user_state[user_id] = "select_social"

        await update.message.reply_text(msg, reply_markup=markup)
        return

    # ---------------- SELECT SOCIAL ----------------

    if state == "select_social":

        user_selected_social[user_id] = text

        accounts = get_accounts()

        approved_accounts = [
            row.get("username")
            for row in accounts
            if str(row.get("telegram_id")) == str(user_id)
            and row.get("social_network") == text
            and row.get("status") == "Approved"
        ]

        if not approved_accounts:
            await update.message.reply_text("⚙️Немає акаунтів у цій мережі.")
            return

        buttons = []
        row = []

        for i, acc in enumerate(approved_accounts, start=1):

            row.append(f"{i}. {acc}")

            if len(row) == 2:
                buttons.append(row)
                row = []

        if row:
            buttons.append(row)

        buttons.append(["⬅️ Назад"])

        markup = ReplyKeyboardMarkup(
            buttons,
            resize_keyboard=True
        )

        user_state[user_id] = "select_account"


        await update.message.reply_text("Оберіть акаунт:", reply_markup=markup)
        return

    # ---------------- SELECT ACCOUNT ----------------

    if state == "select_account":
        if ". " in text:
            text = text.split(". ", 1)[1]


        user_selected_account[user_id] = text
        skipped_tasks[(user_id, text)] = set()
        user_state[user_id] = "working"

        await send_next_task(update, user_id)
        return

    # ---------------- TASK DONE BUTTON ----------------

    if text == "⏭ Пропустити" and state == "working":

        task = current_task.get(user_id)

        if task:
            account_name = user_selected_account.get(user_id)
            skipped_tasks.setdefault((user_id, account_name), set()).add(task["task_id"])

        current_task.pop(user_id, None)

        await send_next_task(update, user_id)
        return

    if text == "✅ Виконано" and state == "working":

        if user_id not in current_task:
            await update.message.reply_text("🤷‍♂️Немає активного завдання.")
            return

        user_state[user_id] = "await_screenshot"

        await update.message.reply_text("📸Надішліть скрін.")
        return

    if state == "await_screenshot":
        return await handle_single_screenshot(update, context)
    
# ---------------- SCREENSHOT ----------------    

async def handle_single_screenshot(update, context, file_id=None, file_id_2=None):

    user_id = update.effective_user.id
    now = datetime.utcnow().isoformat()

    # ======================
    # ОТРИМАННЯ СКРІНУ
    # ======================
    if not file_id:
        if not update.message.photo:
            await update.message.reply_text("📸Будь ласка, надішліть скріншот.")
            return
        file_id = update.message.photo[-1].file_id

    # ======================
    # АНТИДУБЛЬ (1 СКРІН)
    # ======================
    existing_1 = supabase.table("Tasks").select("id").eq("screenfile_id", file_id).execute()

    if existing_1.data:
        await update.message.reply_text(
            "⚠️ Цей скріншот вже був використаний раніше.\n\n"
            "📸 Зробіть новий скріншот."
        )
        return

    # ======================
    # АНТИДУБЛЬ (2 СКРІН)
    # ======================
    if file_id_2:
        existing_2 = supabase.table("Tasks").select("id").eq("screenfile_id_2", file_id_2).execute()

        if existing_2.data:
            await update.message.reply_text(
                "⚠️ Другий скріншот вже використовувався раніше.\n\n"
                "📸 Зробіть новий скріншот."
            )
            return

    # ======================
    # TASK DATA
    # ======================
    task = current_task.get(user_id)
    account_name = user_selected_account.get(user_id)

    if not task:
        await update.message.reply_text("⚠️Помилка. Спробуйте ще раз.")
        user_state[user_id] = "working"
        return

    task_id = task["task_id"]

    # ======================
    # INSERT В БАЗУ
    # ======================
    res = supabase.table("Tasks").insert({
        "telegram_id": user_id,
        "social_network": task["social"],
        "account": account_name,
        "task_id": task_id,
        "link": task["link"],
        "status": "Pending",
        "assign_date": now,
        "screenfile_id": file_id,
        "screenfile_id_2": file_id_2,
        "comment_text": task.get("comment", "")
    }).execute()

    task_record_id = res.data[0]["id"]

    # ======================
    # COMMENT POOL
    # ======================
    if task.get("comment_row_id"):
        supabase.table("Comment_Pool").update({
            "active": False
        }).eq("id", task["comment_row_id"]).execute()

    # ======================
    # КНОПКИ
    # ======================
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Підтвердити", callback_data=f"task_approve|{task_record_id}"),
            InlineKeyboardButton("❌ Відхилити", callback_data=f"task_reject|{task_record_id}")
        ]
    ])

    # ======================
    # AUTO CHECK
    # ======================
    user_row = supabase.table("Users").select("is_top_auto").eq("telegram_id", user_id).execute()

    is_auto = False
    if user_row.data:
        is_auto = user_row.data[0]["is_top_auto"]

    # ======================
    # AUTO
    # ======================
    if is_auto:

        await asyncio.sleep(0.5)

        supabase.table("Tasks").update({
            "status": "Approved",
            "paid": "Paid",
            "approve_date": now
        }).eq("id", task_record_id).execute()

        template = supabase.table("TaskTemplates").select("*").eq("task_id", task_id).execute().data
        reward = int(template[0]["reward"]) if template else 0

        update_user_balance(user_id, reward)
        add_to_user_total(user_id, reward)

        await context.bot.send_message(
            chat_id=int(user_id),
            text=f"✅ Завдання підтверджено!\n💰 Нараховано {reward} Fanki."
        )

        # перший скрін
        await context.bot.send_photo(
            TASK_AUTO_ID,
            file_id,
            caption="🤖 AUTO APPROVE\n\n" +
            f"👤 User ID: {user_id}\n"
            f"📱 Соцмережа: {task['social']}\n"
            f"👤 Акаунт: {account_name}\n"
            f"🧩 Тип: {task['type']}\n"
            f"📄 Завдання: {task['task_id']}\n"
            f"🔗 Посилання: {task['link']}"
        )

        # другий скрін
        if file_id_2:
            await context.bot.send_photo(TASK_AUTO_ID, file_id_2)

    # ======================
    # MANUAL
    # ======================
    else:

        await context.bot.send_photo(
            TASK_MODERATOR_ID,
            file_id,
            caption=(
                f"👤 User ID: {user_id}\n"
                f"📱 Соцмережа: {task['social']}\n"
                f"👤 Акаунт: {account_name}\n"
                f"⚙️ Тип: {task['type']}\n"
                f"📋 Завдання: {task['task_id']}\n"
                f"🔗 Посилання: {task['link']}"
            ),
            reply_markup=keyboard
        )

        # другий скрін
        if file_id_2:
            await context.bot.send_photo(TASK_MODERATOR_ID, file_id_2)

        await update.message.reply_text("Скрін відправлено на перевірку.")

    # ======================
    # CLEANUP
    # ======================
    current_task.pop(user_id, None)

    if task["social"] == "Google Maps":
        user_state[user_id] = None
        await show_main_menu(update)
        return

    user_state[user_id] = None
    await send_next_task(update, user_id)
# ==============================
# WITHDRAW (USER SIDE)
# ==============================

async def handle_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):

    user_id = update.effective_user.id
    text = update.message.text if update.message and update.message.text else ""
    state = user_state.get(user_id, None)

    if text in ["⬅️ Назад", "Назад"]:

        user_state.pop(user_id, None)
        user_binance_id.pop(user_id, None)
        user_withdraw_amount.pop(user_id, None)

        await show_main_menu(update)
        return True

    withdrawals = get_withdrawals()

    # ---------------- START WITHDRAW ----------------

    if text == "💸Вивід":

        balance, _, _ = get_user_data(user_id)

        pending = any(
            str(r.get("telegram_id")) == str(user_id)
            and r.get("status") == "Pending"
            for r in withdrawals
        )

        if pending:
            await update.message.reply_text("У вас вже є заявка на розгляді.")
            return True

        if balance < 1000:
            await update.message.reply_text("Мінімум для виводу 1000 Fanki.")
            return True

        user_state[user_id] = "await_binance"

        markup = ReplyKeyboardMarkup(
            [["⬅️ Назад"]],
            resize_keyboard=True
        )

        await update.message.reply_text(
            "Введіть Binance ID:",
            reply_markup=markup
        )
        return True

    # ---------------- BINANCE ID ----------------
    print("USER:", user_id)
    print("STATE:", user_state.get(user_id))
    print("TEXT:", text)
    if user_state.get(user_id) == "await_binance":

        if not text.isdigit():
            await update.message.reply_text(
                "Binance ID повинен містити тільки цифри."
            )
            return True

        user_binance_id[user_id] = text
        user_state[user_id] = "await_amount"

        markup = ReplyKeyboardMarkup(
            [["⬅️ Назад"]],
            resize_keyboard=True
        )

        await update.message.reply_text(
            "Введіть суму:",
            reply_markup=markup
        )
        return True

    # ---------------- AMOUNT ----------------

    if user_state.get(user_id) == "await_amount":

        balance, _, _ = get_user_data(user_id)

        if not text.isdigit():
            await update.message.reply_text("Введіть число.")
            return True

        amount = int(text)

        if amount < 1000:
            await update.message.reply_text("Мінімум 1000.")
            return True

        if amount > balance:
            await update.message.reply_text("Недостатньо коштів.")
            return True

        user_withdraw_amount[user_id] = amount
        user_state[user_id] = "confirm_withdraw"

        markup = ReplyKeyboardMarkup(
            [["Так"], ["⬅️ Назад"]],
            resize_keyboard=True
        )

        await update.message.reply_text(
            f"Підтвердити вивід {amount} Fanki "
            f"(${amount/1000:.2f}) "
            f"на Binance ID {user_binance_id[user_id]}?",
            reply_markup=markup
        )

        return True

    # ---------------- CONFIRM ----------------

    if user_state.get(user_id) == "confirm_withdraw" and text == "Так":

        amount = user_withdraw_amount.get(user_id)

        if not amount:
            await update.message.reply_text("Помилка.")
            user_state[user_id] = None
            return True

        now = datetime.utcnow().isoformat()

    # 💰 РАХУЄМО $
        usd = round(amount / 1000, 2)

        deduct_user_balance(user_id, amount)

        res = supabase.table("Withdrawals").insert({
            "telegram_id": user_id,
            "username": update.effective_user.username or "",
            "binance_id": user_binance_id[user_id],
            "amount": amount,
            "amount_usd": usd,  # 👈 ОСЬ ЦЕ ГОЛОВНЕ
            "status": "Pending",
            "request_date": now
        }).execute()

        withdraw_id = res.data[0]["id"]

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "✅ Підтвердити",
                    callback_data=f"withdraw_approve|{withdraw_id}"
                ),
                InlineKeyboardButton(
                    "❌ Відхилити",
                    callback_data=f"withdraw_reject|{withdraw_id}"
                )
            ]
        ])

        await context.bot.send_message(
            ADMIN_ID[0],
            f"💸 Новий запит на вивід\n\n"
            f"👤 User: {user_id}\n"
            f"💰 Сума: {amount} Fanki (~{usd}$)\n"
            f"🏦 Binance ID: {user_binance_id[user_id]}",
            reply_markup=keyboard
        )

        await update.message.reply_text(
            f"✅ Заявка на вивід створена\n\n"
            f"💰 Сума: {amount} Fanki\n"
            f"💵 ≈ {usd}$\n"
            f"🏦 Binance ID: {user_binance_id[user_id]}\n\n"
            f"Очікуйте підтвердження адміністратора."
        )

        user_state[user_id] = None
        return True

    return False
# ==============================
# MESSAGE ROUTER
# ==============================

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):

    user_id = update.effective_user.id

    if not update.message:
        return

    text = update.message.text or ""

    # --- withdraw ---
    if await handle_withdraw(update, context):
        return

    try:

        # ================= ADMIN =================
        if is_admin(user_id):

            text = text.strip()

            # RESET
            if text in ["📢 Розсилка", "📊 Статистика", "💰 Змінити баланс", "🔒 Бан користувача"]:
                admin_state.pop(user_id, None)

            # BACK
            if text in ["⬅️ Назад", "Назад"]:
                admin_state.pop(user_id, None)
                await show_main_menu(update)
                return

            # STATISTICS
            if text == "📊 Статистика":
                users = get_users()
                tasks = get_tasks()
                withdrawals = get_withdrawals()

                total_balance = sum(int(r.get("balance") or 0) for r in users)
                total_users = len(users)

                pending_tasks = sum(1 for r in tasks if r.get("status") == "Pending")
                pending_withdraws = sum(1 for r in withdrawals if r.get("status") == "Pending")

                await update.message.reply_text(
                    f"📊 Статистика\n\n"
                    f"👥 Користувачів: {total_users}\n"
                    f"💰 Сума балансів: {total_balance}\n"
                    f"📋 Pending задач: {pending_tasks}\n"
                    f"💸 Pending виводів: {pending_withdraws}"
                )
                return

            # CHANGE BALANCE
            if text == "💰 Змінити баланс":
                admin_state[user_id] = {"step": "id"}
                await update.message.reply_text("Введіть ID користувача:")
                return

            # BAN
            if text == "🔒 Бан користувача":
                admin_state[user_id] = {"step": "ban_id"}
                await update.message.reply_text("Введіть ID користувача:")
                return

            # BROADCAST
            if text == "📢 Розсилка":
                admin_state[user_id] = {"step": "broadcast"}
                await update.message.reply_text("Введіть текст розсилки:")
                return

            # STATE
            state = admin_state.get(user_id)

            if state:

                if state["step"] == "id":
                    admin_state[user_id] = {
                        "step": "amount",
                        "target_id": text
                    }
                    await update.message.reply_text("Введіть суму (+500 або -300):")
                    return

                if state["step"] == "amount":
                    try:
                        amount = int(text)
                    except:
                        await update.message.reply_text("❗ Введіть число")
                        return

                    target_id = state["target_id"]

                    update_user_balance(target_id, amount)

                    await update.message.reply_text("✅ Баланс змінено")
                    admin_state.pop(user_id, None)
                    return

                if state["step"] == "ban_id":
                    supabase.table("Users").update({
                        "status": "Banned"
                    }).eq("telegram_id", int(text)).execute()

                    await update.message.reply_text("🔒 Користувача заблоковано")
                    admin_state.pop(user_id, None)
                    return

                if state["step"] == "broadcast":

                    users = get_users()

                    for r in users:
                        try:
                            await context.bot.send_message(
                                r.get("telegram_id"),
                                text
                            )
                        except:
                            pass

                    await update.message.reply_text("✅ Розсилка завершена")
                    admin_state.pop(user_id, None)
                    return

        # ================= USER =================
        await handle_user_message(update, context)

    except Exception:
        logging.error(traceback.format_exc())
        await update.message.reply_text("Сталася помилка")

# ==============================
# BUILD APP
# ==============================

def build_app():

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Regex(r"^/remove_\d+"), handle_user_message))
    app.add_handler(CallbackQueryHandler(handle_callback))

    app.add_handler(
        MessageHandler(
            (filters.TEXT | filters.PHOTO) & ~filters.COMMAND,
            handle_message
        )
    )

    return app


# ==============================
# RUN
# ==============================

if __name__ == "__main__":

    app = build_app()

    print("FankiBot Supabase Version 🚀")

    app.run_polling(drop_pending_updates=True)
