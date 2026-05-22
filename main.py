# ==============================
# FANki BOT — SUPABASE VERSION
# ==============================

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
    InlineKeyboardButton,
    InputMediaPhoto
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


def can_use_callback(user_id):
    return user_id in set(ADMIN_ID + [TASK_MODERATOR_ID, TASK_AUTO_ID])


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
user_video_screenshots = {}
current_task = {}
skipped_tasks = {}


# ==============================
# DATABASE HELPERS
# ==============================

def db_user_id(user_id):
    try:
        return int(user_id)
    except:
        return user_id


def first_row(data):
    return data[0] if data else None


def parse_limit(value):
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        limit = int(float(text.replace(",", ".")))
    except:
        return None

    return limit if limit > 0 else None


def parse_date(value):
    if not value:
        return None

    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except:
        return None


def rpc_json(response):
    data = response.data

    if isinstance(data, list):
        if not data:
            return {}
        return data[0]

    return data or {}


def call_rpc(name, params):
    return rpc_json(supabase.rpc(name, params).execute())


def select_first(table, columns="*", **filters_eq):
    query = supabase.table(table).select(columns)
    for key, value in filters_eq.items():
        query = query.eq(key, value)
    return first_row(query.limit(1).execute().data)


def get_users():
    return supabase.table("Users").select("*").execute().data or []


def get_user_row(user_id, columns="*"):
    return select_first("Users", columns, telegram_id=db_user_id(user_id))


def user_exists(user_id):
    return bool(get_user_row(user_id, "id"))


def get_tasks():
    return supabase.table("Tasks").select("*").execute().data or []


def get_templates():
    return supabase.table("TaskTemplates").select("*").execute().data or []


def get_accounts():
    return (
        supabase
        .table("Accounts")
        .select("*")
        .eq("in_cabinet", True)
        .execute()
        .data
        or []
    )


def get_user_approved_accounts(user_id, social_network=None):
    query = (
        supabase
        .table("Accounts")
        .select("id, social_network, username")
        .eq("telegram_id", db_user_id(user_id))
        .eq("status", "Approved")
        .eq("in_cabinet", True)
    )

    if social_network:
        query = query.eq("social_network", social_network)

    return query.execute().data or []


def get_task_account(user_id, social_network, account_name):
    return (
        supabase
        .table("Accounts")
        .select("id, telegram_id, username, status, social_network, gender, region")
        .eq("telegram_id", db_user_id(user_id))
        .eq("username", account_name)
        .eq("social_network", social_network)
        .eq("status", "Approved")
        .eq("in_cabinet", True)
        .limit(1)
        .execute()
        .data
        or []
    )


def load_active_reserved_task(user_id):
    rows = (
        supabase
        .table("Tasks")
        .select("id, telegram_id, social_network, account, task_id, link, comment_text, comment_row_id, screenfile_id")
        .eq("telegram_id", db_user_id(user_id))
        .eq("status", "Reserved")
        .order("assign_date", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )

    if not rows:
        return None

    row = rows[0]
    template = (
        supabase
        .table("TaskTemplates")
        .select("task_type, reward, link")
        .eq("task_id", row.get("task_id"))
        .limit(1)
        .execute()
        .data
        or []
    )

    task_type = template[0].get("task_type") if template else ""
    reward = template[0].get("reward") if template else 0
    link = row.get("link") or (template[0].get("link") if template else "")

    return {
        "task_record_id": row.get("id"),
        "task_id": row.get("task_id"),
        "social": row.get("social_network"),
        "account": row.get("account"),
        "type": task_type,
        "link": link,
        "reward": reward,
        "comment": row.get("comment_text") or "",
        "comment_row_id": row.get("comment_row_id"),
        "screenfile_id": row.get("screenfile_id")
    }


def account_username_exists(username):
    return bool(
        supabase
        .table("Accounts")
        .select("id")
        .eq("in_cabinet", True)
        .ilike("username", username)
        .limit(1)
        .execute()
        .data
    )


def account_profile_exists(profile_link):
    return bool(
        supabase
        .table("Accounts")
        .select("id")
        .eq("in_cabinet", True)
        .ilike("profile_link", profile_link)
        .limit(1)
        .execute()
        .data
    )


def get_withdrawals():
    return supabase.table("Withdrawals").select("*").execute().data or []


def has_pending_withdrawal(user_id):
    return bool(
        supabase
        .table("Withdrawals")
        .select("id")
        .eq("telegram_id", db_user_id(user_id))
        .eq("status", "Pending")
        .limit(1)
        .execute()
        .data
    )


def get_comments():
    return supabase.table("Comment_Pool").select("*").execute().data or []


def is_screenshot_used(file_id, exclude_task_id=None):
    if not file_id:
        return False

    query = (
        supabase
        .table("Tasks")
        .select("id")
        .or_(f"screenfile_id.eq.{file_id},screenfile_id_2.eq.{file_id}")
    )

    if exclude_task_id:
        query = query.neq("id", exclude_task_id)

    return bool(query.limit(1).execute().data)


def cleanup_expired_reservations():
    try:
        supabase.rpc("cleanup_expired_reservations", {
            "p_minutes": 30
        }).execute()
    except Exception as e:
        logging.error(f"Reservation cleanup error: {e}")


def release_reserved_task(task_record_id):
    if not task_record_id:
        return

    try:
        supabase.rpc("release_reserved_task", {
            "p_task_record_id": str(task_record_id)
        }).execute()
    except Exception as e:
        logging.error(f"Release reserved task error: {e}")


def release_current_task(user_id):
    task = current_task.get(user_id)
    if task:
        release_reserved_task(task.get("task_record_id"))


def reserve_task_assignment(user_id, social_network, account_name, template, user_gender, user_region):
    return call_rpc("reserve_task_assignment", {
        "p_telegram_id": db_user_id(user_id),
        "p_social_network": social_network,
        "p_account": account_name,
        "p_task_id": int(template["_task_id_int"]),
        "p_link": (template.get("link") or "").strip(),
        "p_task_type": str(template.get("task_type") or ""),
        "p_user_gender": user_gender,
        "p_user_region": user_region,
        "p_reserve_minutes": 30
    })


def submit_reserved_task(task_record_id, file_id, file_id_2=None):
    return call_rpc("submit_reserved_task_atomic", {
        "p_task_record_id": str(task_record_id),
        "p_screenfile_id": file_id,
        "p_screenfile_id_2": file_id_2
    })


def approve_task_atomic(task_record_id):
    return call_rpc("approve_task_atomic", {
        "p_task_record_id": str(task_record_id)
    })


def finalize_withdrawal_atomic(withdrawal_id, status):
    return call_rpc("finalize_withdrawal_atomic", {
        "p_withdrawal_id": str(withdrawal_id),
        "p_status": status
    })


def count_rows(table, status=None):
    query = supabase.table(table).select("id", count="exact")
    if status:
        query = query.eq("status", status)
    res = query.execute()
    return res.count if res.count else 0


# ==============================
# BALANCE FUNCTIONS
# ==============================

def get_user_data(user_id):
    row = get_user_row(user_id, "balance, total, status")

    if row:
        balance = int(row.get("balance") or 0)
        total = int(row.get("total") or 0)
        status = row.get("status") or "Active"
        return balance, total, status

    return 0, 0, "Active"


def update_user_balance(user_id, amount):
    supabase.rpc("adjust_user_balance", {
        "p_telegram_id": db_user_id(user_id),
        "p_amount": int(amount)
    }).execute()


def deduct_user_balance(user_id, amount):
    supabase.rpc("adjust_user_balance", {
        "p_telegram_id": db_user_id(user_id),
        "p_amount": -int(amount)
    }).execute()


def add_to_user_total(user_id, amount):
    supabase.rpc("adjust_user_total", {
        "p_telegram_id": db_user_id(user_id),
        "p_amount": int(amount)
    }).execute()


# ==============================
# USER STATS
# ==============================

def get_user_stats(user_id):
    row = get_user_row(user_id, "register")
    reg_date = row.get("register") if row else "_"

    res = (
        supabase
        .table("Tasks")
        .select("id", count="exact")
        .eq("telegram_id", db_user_id(user_id))
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
    user = (
        supabase
        .table("Users")
        .select("balance, register")
        .eq("telegram_id", db_user_id(user_id))
        .limit(1)
        .execute()
        .data
    )

    if not user:
        return None

    user = user[0]

    # --- БАЛАНС ---
    fanki_balance = int(user.get("balance", 0))

    reg_date_raw = user.get("register")

    if reg_date_raw:
        dt = datetime.fromisoformat(reg_date_raw)
        reg_date = dt.strftime("%d.%m.%Y")
    else:
        reg_date = "—"

    # --- ВСІ ЗАВДАННЯ ---
    tasks = (
        supabase
        .table("Tasks")
        .select("status, task_id, assign_date, approve_date")
        .eq("telegram_id", db_user_id(user_id))
        .execute()
        .data
        or []
    )

    today = datetime.utcnow().date()

    tasks_total = 0
    tasks_today = 0
    earned_total = 0
    earned_today = 0

    templates = (
        supabase
        .table("TaskTemplates")
        .select("task_id, reward")
        .execute()
        .data
        or []
    )

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

    res = (
        supabase
        .table("Users")
        .select("id", count="exact")
        .eq("status", "Active")
        .execute()
    )

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
        "💬 Спільнота користувачів:https://t.me/+5_xjkinfaaw3OGQ0 \n"
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
👋Привіт, {username}! 
 Раді вітати вас у FankiBoti.
 ⚠️ Для роботи з ботом необхідно встановити username у Telegram.

 Без username:
 • недоступний вивід коштів

📌 Встановіть username в налаштуваннях Telegram та перезапустіть бота.

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

    if not can_use_callback(query.from_user.id):
        return

    data_raw = query.data or ""

    if "|" not in data_raw:
        return

    action, record_id = data_raw.split("|", 1)

    try:
        # =========================
        # ACCOUNT APPROVE / REJECT
        # =========================

        if action in ["account_approve", "account_reject"]:
            res = supabase.table("Accounts").select("*").eq("id", record_id).limit(1).execute()
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
            res = supabase.table("Tasks").select("*").eq("id", record_id).limit(1).execute()
            if not res.data:
                return

            row = res.data[0]

            if row["status"] != "Pending":
                return

            user_id = row["telegram_id"]
            task_id = row["task_id"]

            template = (
                supabase
                .table("TaskTemplates")
                .select("task_type, link, reward")
                .eq("task_id", task_id)
                .limit(1)
                .execute()
                .data
            )

            task_type = template[0]["task_type"] if template else ""
            link = template[0]["link"] if template else ""
            action_text = TASK_TEXT.get(str(task_type).lower(), task_type)

            if action == "task_approve":
                approved = approve_task_atomic(record_id)
                if not approved.get("ok"):
                    return

                user_id = approved.get("telegram_id", user_id)
                task_id = approved.get("task_id", task_id)
                task_type = approved.get("task_type", task_type)
                link = approved.get("link", link)
                reward = int(approved.get("reward") or 0)
                action_text = TASK_TEXT.get(str(task_type).lower(), task_type)


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
                }).eq("id", record_id).eq("status", "Pending").execute()

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
            res = supabase.table("Withdrawals").select("*").eq("id", record_id).limit(1).execute()
            if not res.data:
                return

            row = res.data[0]

            if row["status"] != "Pending":
                return

            user_id = row["telegram_id"]
            amount = int(row["amount"])

            if action == "withdraw_approve":
                finalized = finalize_withdrawal_atomic(record_id, "Approved")
                if not finalized.get("ok"):
                    return

                user_id = finalized.get("telegram_id", user_id)
                amount = int(finalized.get("amount") or amount)
                binance_id = finalized.get("binance_id") or row["binance_id"]

                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=(
                        "✅ Ваш вивід підтверджено!\n\n"
                        f"💰 Сума: {amount} Fanki\n"
                        f"🏦 Binance ID: {binance_id}\n\n"
                        "🙏 Дякуємо за участь у Fanki!"
                    )
                )

                await query.edit_message_text("✅ Вивід підтверджено")

            else:
                finalized = finalize_withdrawal_atomic(record_id, "Rejected")
                if not finalized.get("ok"):
                    return

                user_id = finalized.get("telegram_id", user_id)
                amount = int(finalized.get("amount") or amount)
                binance_id = finalized.get("binance_id") or row["binance_id"]

                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=(
                        "❌ Ваш запит на вивід відхилено\n\n"
                        f"💰 Сума: {amount} Fanki\n"
                        f"🏦 Binance ID: {binance_id}\n\n"
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
    cleanup_expired_reservations()

    if user_id not in user_selected_social:
        await update.message.reply_text("Помилка: соцмережа не вибрана.")
        return

    social_network = user_selected_social[user_id]
    account_name = user_selected_account.get(user_id)

    account_rows = get_task_account(user_id, social_network, account_name)
    account_row = account_rows[0] if account_rows else None

    if not account_row:
        await update.message.reply_text("Акаунт не підтверджений.")
        return

    # ✅ ГЕНДЕР КОРИСТУВАЧА
    user_gender = str(account_row.get("gender") or "all").strip().lower()

    # ✅ РЕГІОН КОРИСТУВАЧА
    user_region = str(account_row.get("region") or "all").strip().lower()

    templates = (
        supabase
        .table("TaskTemplates")
        .select("id, task_id, task_type, link, reward, active, social_network, gender_target, region_target, max_total, max_per_day")
        .eq("active", True)
        .eq("social_network", social_network)
        .order("id")
        .execute()
        .data
        or []
    )

    prepared_templates = []
    for template in templates:
        try:
            template["_task_id_int"] = int(template.get("task_id"))
        except:
            continue
        prepared_templates.append(template)

    candidate_task_ids = [t["_task_id_int"] for t in prepared_templates]
    comment_task_ids = [
        t["_task_id_int"]
        for t in prepared_templates
        if str(t.get("task_type")).lower() == "comment"
    ]

    # --- PRELOAD TASK STATS ---
    total_used_map = {}
    day_used_map = {}

    if candidate_task_ids:
        all_tasks = (
            supabase
            .table("Tasks")
            .select("task_id, assign_date, status")
            .in_("status", ["Pending", "Approved"])
            .in_("task_id", candidate_task_ids)
            .execute()
            .data
            or []
        )

        today = datetime.utcnow().date()

        for t in all_tasks:
            try:
                tid = int(t["task_id"])
            except:
                continue

            total_used_map[tid] = total_used_map.get(tid, 0) + 1

            d = parse_date(t.get("assign_date"))
            if d == today:
                day_used_map[tid] = day_used_map.get(tid, 0) + 1

    tasks = (
        supabase
        .table("Tasks")
        .select("task_id")
        .eq("account", account_name)
        .eq("social_network", social_network)
        .in_("task_id", candidate_task_ids)
        .execute()
        .data
        or []
    )

    # already used task_ids
    done_task_ids = set()

    for r in tasks:
        try:
            done_task_ids.add(int(r.get("task_id")))
        except:
            pass

    comments_by_task = {}
    recent_comment_accounts = set()

    if comment_task_ids:
        comments = (
            supabase
            .table("Comment_Pool")
            .select("id, task_id, comment, active, gender, region")
            .eq("active", True)
            .in_("task_id", comment_task_ids)
            .execute()
            .data
            or []
        )

        for comment in comments:
            try:
                tid = int(comment.get("task_id"))
            except:
                continue
            comments_by_task.setdefault(tid, []).append(comment)

        recent_comments = (
            supabase
            .table("Tasks")
            .select("account")
            .neq("comment_text", "")
            .order("assign_date", desc=True)
            .limit(180)
            .execute()
            .data
            or []
        )

        recent_comment_accounts = {
            r.get("account")
            for r in recent_comments
            if r.get("account")
        }

    for template in prepared_templates:
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

        task_id = template["_task_id_int"]

        # =============================
        # LIMITS (швидка версія)
        # =============================

        max_total = parse_limit(template.get("max_total"))
        max_per_day = parse_limit(template.get("max_per_day"))

        if max_total is not None:
            if total_used_map.get(task_id, 0) >= max_total:
                continue

        if max_per_day is not None:
            if day_used_map.get(task_id, 0) >= max_per_day:
                continue

        if task_id in done_task_ids:
            continue

        if task_id in skipped_tasks.get((user_id, account_name), set()):
            continue

        task_type = template.get("task_type")

        # --- COMMENT TIMER (НЕ ЧІПАЄМО) ---
        if str(task_type).lower() == "comment":
            if account_name in recent_comment_accounts:
                continue

            last_comment = (
                supabase
                .table("Tasks")
                .select("assign_date")
                .eq("task_id", task_id)
                .neq("comment_text", "")
                .order("assign_date", desc=True)
                .limit(1)
                .execute()
            )

            if last_comment.data:
                from datetime import timedelta

                last_time = datetime.fromisoformat(last_comment.data[0]["assign_date"])
                now_time = datetime.now(last_time.tzinfo)

                if (now_time - last_time) < timedelta(minutes=40):
                    continue

        link = (template.get("link") or "").strip()
        reward = template.get("reward")
        action_text = TASK_TEXT.get(str(task_type).lower(), task_type)

        reservation = reserve_task_assignment(
            user_id,
            social_network,
            account_name,
            template,
            user_gender,
            user_region
        )

        if not reservation.get("ok"):
            continue

        comment_text = reservation.get("comment_text") or ""
        comment_row_id = reservation.get("comment_row_id")
        task_record_id = reservation.get("task_record_id")

        current_task[user_id] = {
            "task_record_id": task_record_id,
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
        release_current_task(user_id)

        user_state.pop(user_id, None)
        admin_state.pop(user_id, None)
        user_selected_social.pop(user_id, None)
        user_selected_account.pop(user_id, None)
        user_binance_id.pop(user_id, None)
        user_withdraw_amount.pop(user_id, None)
        user_video_screenshots.pop(user_id, None)
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
        if not user_exists(user_id):
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
            "• Facebook\n"
            "• YouTube\n"
            "• Telegram\n"
            "• Google Maps\n\n"
            "💼 Ви можете працювати з декількох власних акаунтів кожної соціальної мережі.\n\n"
            "📢 У нашому офіційному каналі публікуються:\n"
            "• новини\n"
            "• оновлення\n"
            "• інструкції\n"
            "• корисна інформація по роботі\n"
            "👇 Канал:\n"
            "https://t.me/+XnLg96cCpKpkMjA8\n\n"
            "💬 Група обговорення та допомоги:\n"
            "https://t.me/+5_xjkinfaaw3OGQ0"
        )
        return

    # ---------------- SUPPORT ----------------

    if text == "🛠Підтримка":
        await update.message.reply_text(
            "📩 Підтримка:\n"
            "Якщо у вас виникли питання або проблеми під час роботи — зверніться до адміністратора.\n\n"
            "💬 Також ви можете поставити питання у спільноті користувачів.\n"
            "Де учасники та адміністрація допомагають вирішувати проблеми та діляться досвідом."
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

        if account_username_exists(text):
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

        if account_profile_exists(link):
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
        approved = get_user_approved_accounts(user_id)

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

        approved_accounts = [
            row.get("username")
            for row in get_user_approved_accounts(user_id, text)
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
            release_reserved_task(task.get("task_record_id"))

        current_task.pop(user_id, None)

        await send_next_task(update, user_id)
        return

    if text == "✅ Виконано" and state == "working":
        if user_id not in current_task:
            await update.message.reply_text("🤷‍♂️Немає активного завдання.")
            return

        task = current_task.get(user_id) or {}

        if str(task.get("type")).lower() == "video_view":
            user_video_screenshots.pop(user_id, None)
            user_state[user_id] = "await_video_screenshot_1"
            await update.message.reply_text("📸Надішліть перший скрін перегляду відео.")
            return

        user_state[user_id] = "await_screenshot"

        await update.message.reply_text("📸Надішліть скрін.")
        return

    if state == "await_video_screenshot_1":
        return await handle_video_screenshot(update, context, first=True)

    if state == "await_video_screenshot_2":
        return await handle_video_screenshot(update, context, first=False)

    if state == "await_screenshot":
        return await handle_single_screenshot(update, context)


# ---------------- VIDEO SCREENSHOTS ----------------

async def handle_video_screenshot(update, context, first=True):
    user_id = update.effective_user.id
    task = current_task.get(user_id)
    task_record_id = task.get("task_record_id") if task else None

    if not update.message.photo:
        await update.message.reply_text("📸Будь ласка, надішліть скріншот.")
        return

    file_id = update.message.photo[-1].file_id

    if is_screenshot_used(file_id, exclude_task_id=task_record_id):
        await update.message.reply_text(
            "⚠️ Цей скріншот вже був використаний раніше.\n\n"
            "📸 Зробіть новий скріншот."
        )
        return

    if first:
        if task_record_id:
            supabase.table("Tasks").update({
                "screenfile_id": file_id
            }).eq("id", task_record_id).eq("status", "Reserved").execute()

        user_video_screenshots[user_id] = file_id
        user_state[user_id] = "await_video_screenshot_2"

        await update.message.reply_text("📸Надішліть другий скрін кінця перегляду відео.")
        return

    first_file_id = user_video_screenshots.get(user_id)

    if not first_file_id and task_record_id:
        reserved = (
            supabase
            .table("Tasks")
            .select("screenfile_id")
            .eq("id", task_record_id)
            .eq("status", "Reserved")
            .limit(1)
            .execute()
            .data
        )
        if reserved:
            first_file_id = reserved[0].get("screenfile_id")

    if not first_file_id:
        user_state[user_id] = "await_video_screenshot_1"
        await update.message.reply_text("📸Надішліть перший скрін початок перегляду відео.")
        return

    if file_id == first_file_id:
        await update.message.reply_text(
            "⚠️ Перший і другий скрін мають бути різними.\n\n"
            "📸 Надішліть інший другий скрін."
        )
        return

    await handle_single_screenshot(
        update,
        context,
        file_id=first_file_id,
        file_id_2=file_id
    )


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
    # TASK DATA
    # ======================
    task = current_task.get(user_id)
    account_name = user_selected_account.get(user_id)

    if not task:
        await update.message.reply_text("⚠️Помилка. Спробуйте ще раз.")
        user_state[user_id] = "working"
        return

    task_id = task["task_id"]
    task_record_id = task.get("task_record_id")

    # ======================
    # АНТИДУБЛЬ
    # ======================
    if is_screenshot_used(file_id, exclude_task_id=task_record_id):
        await update.message.reply_text(
            "⚠️ Цей скріншот вже був використаний раніше.\n\n"
            "📸 Зробіть новий скріншот."
        )
        return

    if file_id_2:
        if file_id_2 == file_id:
            await update.message.reply_text(
                "⚠️ Перший і другий скрін мають бути різними.\n\n"
                "📸 Зробіть новий скріншот."
            )
            return

        if is_screenshot_used(file_id_2, exclude_task_id=task_record_id):
            await update.message.reply_text(
                "⚠️ Другий скріншот вже використовувався раніше.\n\n"
                "📸 Зробіть новий скріншот."
            )
            return

    # ======================
    # SUBMIT RESERVED TASK
    # ======================
    if not task_record_id:
        await update.message.reply_text("⚠️Резерв завдання не знайдено. Отримайте завдання ще раз.")
        current_task.pop(user_id, None)
        user_state[user_id] = None
        await send_next_task(update, user_id)
        return

    submitted = submit_reserved_task(task_record_id, file_id, file_id_2)

    if not submitted.get("ok"):
        reason = submitted.get("reason")

        if reason == "RESERVATION_EXPIRED":
            await update.message.reply_text("⏳ Час на виконання завдання минув. Візьміть нове завдання.")
        elif reason in ["DUPLICATE_SCREENSHOT", "SAME_SCREENSHOT"]:
            await update.message.reply_text(
                "⚠️ Цей скріншот вже був використаний раніше або два скріни однакові.\n\n"
                "📸 Зробіть новий скріншот."
            )
            return
        else:
            await update.message.reply_text("⚠️Не вдалося відправити завдання на перевірку. Спробуйте ще раз.")
            return

        current_task.pop(user_id, None)
        user_video_screenshots.pop(user_id, None)
        user_state[user_id] = None
        await send_next_task(update, user_id)
        return

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
    user_row = (
        supabase
        .table("Users")
        .select("is_top_auto")
        .eq("telegram_id", db_user_id(user_id))
        .limit(1)
        .execute()
    )

    is_auto = False
    if user_row.data:
        is_auto = user_row.data[0]["is_top_auto"]

    caption = (
        f"👤 User ID: {user_id}\n"
        f"📱 Соцмережа: {task['social']}\n"
        f"👤 Акаунт: {account_name}\n"
        f"⚙️ Тип: {task['type']}\n"
        f"📋 Завдання: {task['task_id']}\n"
        f"🔗 Посилання: {task['link']}"
    )

    # ======================
    # AUTO
    # ======================
    if is_auto:
        await asyncio.sleep(0.5)

        approved = approve_task_atomic(task_record_id)
        if not approved.get("ok"):
            await update.message.reply_text("⚠️ Завдання відправлено, але автоапрув не спрацював.")
            return

        reward = int(approved.get("reward") or 0)

        await context.bot.send_message(
            chat_id=int(user_id),
            text=f"✅ Завдання підтверджено!\n💰 Нараховано {reward} Fanki."
        )

        if file_id_2:
            await context.bot.send_media_group(
                TASK_AUTO_ID,
                [
                    InputMediaPhoto(media=file_id, caption="🤖 AUTO APPROVE\n\n" + caption),
                    InputMediaPhoto(media=file_id_2)
                ]
            )
        else:
            await context.bot.send_photo(
                TASK_AUTO_ID,
                file_id,
                caption="🤖 AUTO APPROVE\n\n" + caption
            )

    # ======================
    # MANUAL
    # ======================
    else:
        if file_id_2:
            await context.bot.send_media_group(
                TASK_MODERATOR_ID,
                [
                    InputMediaPhoto(media=file_id, caption=caption),
                    InputMediaPhoto(media=file_id_2)
                ]
            )

            await context.bot.send_message(
                TASK_MODERATOR_ID,
                "⬆️ Два скріни до цього завдання",
                reply_markup=keyboard
            )
        else:
            await context.bot.send_photo(
                TASK_MODERATOR_ID,
                file_id,
                caption=caption,
                reply_markup=keyboard
            )

        await update.message.reply_text("Скрін відправлено на перевірку.")

    # ======================
    # CLEANUP
    # ======================
    current_task.pop(user_id, None)
    user_video_screenshots.pop(user_id, None)

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

    if text in ["⬅️ Назад", "Назад"]:
        release_current_task(user_id)

        user_state.pop(user_id, None)
        user_binance_id.pop(user_id, None)
        user_withdraw_amount.pop(user_id, None)

        await show_main_menu(update)
        return True

    # ---------------- START WITHDRAW ----------------

    if text == "💸Вивід":
        balance, _, _ = get_user_data(user_id)

        if has_pending_withdrawal(user_id):
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

        # 💰 РАХУЄМО $
        usd = round(amount / 1000, 2)

        created = call_rpc("create_withdrawal_atomic", {
            "p_telegram_id": db_user_id(user_id),
            "p_username": update.effective_user.username or "",
            "p_binance_id": user_binance_id[user_id],
            "p_amount": amount
        })

        if not created.get("ok"):
            reason = created.get("reason")
            if reason == "PENDING_WITHDRAWAL":
                await update.message.reply_text("У вас вже є заявка на розгляді.")
            elif reason == "LOW_BALANCE":
                await update.message.reply_text("Недостатньо коштів.")
            else:
                await update.message.reply_text("Помилка створення заявки на вивід.")

            user_state[user_id] = None
            return True

        withdraw_id = created.get("id")

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

    if update.message.photo and user_id not in current_task:
        active_task = load_active_reserved_task(user_id)
        if active_task:
            current_task[user_id] = active_task
            user_selected_social[user_id] = active_task["social"]
            user_selected_account[user_id] = active_task["account"]

            if str(active_task.get("type")).lower() == "video_view":
                if active_task.get("screenfile_id"):
                    user_video_screenshots[user_id] = active_task["screenfile_id"]
                    user_state[user_id] = "await_video_screenshot_2"
                    return await handle_video_screenshot(update, context, first=False)

                user_state[user_id] = "await_video_screenshot_1"
                return await handle_video_screenshot(update, context, first=True)

            user_state[user_id] = "await_screenshot"
            return await handle_single_screenshot(update, context)

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
                users_balance_rows = (
                    supabase
                    .table("Users")
                    .select("balance")
                    .execute()
                    .data
                    or []
                )

                total_balance = sum(int(r.get("balance") or 0) for r in users_balance_rows)
                total_users = count_rows("Users")
                pending_tasks = count_rows("Tasks", "Pending")
                pending_withdraws = count_rows("Withdrawals", "Pending")

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
                    }).eq("telegram_id", db_user_id(text)).execute()

                    await update.message.reply_text("🔒 Користувача заблоковано")
                    admin_state.pop(user_id, None)
                    return

                if state["step"] == "broadcast":
                    users = (
                        supabase
                        .table("Users")
                        .select("telegram_id")
                        .execute()
                        .data
                        or []
                    )

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

