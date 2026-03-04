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


# ==============================
# ERROR NOTIFY
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
    return supabase.table("Accounts").select("*").execute().data


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
    tasks = get_tasks()

    reg_date = "-"
    completed_tasks = 0

    for row in users:

        if str(row.get("telegram_id")) == str(user_id):

            reg_date = row.get("register") or "-"
            break

    for row in tasks:

        if (
            str(row.get("telegram_id")) == str(user_id)
            and row.get("status") == "Approved"
        ):
            completed_tasks += 1

    return reg_date, completed_tasks


# ==============================
# ADMIN PANEL
# ==============================

def is_admin(user_id):
    return user_id in ADMIN_ID


def log_admin_action(admin_id, action, target_user_id="", details=""):

    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    supabase.table("AdminLogs").insert({
        "date": now,
        "admin_id": str(admin_id),
        "action": action,
        "target_user_id": str(target_user_id),
        "details": details
    }).execute()


async def show_admin_panel(update, context):

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("👤 Користувачі", callback_data="admin_users")],
        [InlineKeyboardButton("📊 Аналітика", callback_data="admin_stats")]
    ])

    if update.callback_query:

        await update.callback_query.edit_message_text(
            "🛠 Адмін панель",
            reply_markup=keyboard
        )

    else:

        await update.message.reply_text(
            "🛠 Адмін панель",
            reply_markup=keyboard
        )


async def handle_admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):

    query = update.callback_query
    await query.answer()

    admin_id = update.effective_user.id

    if not is_admin(admin_id):
        return

    data = query.data

    users = get_users()

    if data == "admin_users":

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 Пошук по ID", callback_data="admin_search_user")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="admin_back")]
        ])

        await query.edit_message_text(
            "👤 Користувачі",
            reply_markup=keyboard
        )

    elif data == "admin_search_user":

        context.user_data["admin_state"] = "await_user_id"

        await query.edit_message_text("Введіть ID користувача:")

    elif data == "admin_back":

        await show_admin_panel(update, context)

    elif data == "admin_stats":

        total_users = len(users)

        active = sum(1 for r in users if r.get("status") == "Active")
        banned = sum(1 for r in users if r.get("status") == "Banned")

        total_balance = sum(
            int(r.get("balance") or 0) for r in users
        )

        await query.edit_message_text(
            f"📊 Аналітика\n\n"
            f"👥 Всього користувачів: {total_users}\n"
            f"🟢 Active: {active}\n"
            f"🔴 Banned: {banned}\n"
            f"💰 Сума балансів: {total_balance}"
        )
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
                ["📋 Завдання"],
                ["💸 Виводи"],
                ["💰 Змінити баланс"],
                ["📢 Розсилка"],
                ["📊 Статистика"],
                ["⬅️ Назад"],
                ["🔒 Бан користувача"]
            ],
            resize_keyboard=True
        )

        await update.message.reply_text(
            "🛠 Адмін панель\nВітаємо в головному меню, оберіть пункт.",
            reply_markup=markup
        )
        return

    users = get_users()
    active_users = len(users)

    markup = ReplyKeyboardMarkup(
        [
            ["Реєстрація акаунту"],
            ["Мій кабінет"],
            ["Інформація про бот"],
            ["Завдання"],
            ["Підтримка"],
            ["Вивід"]
        ],
        resize_keyboard=True
    )

    text = (
        "👋 Ласкаво просимо до головного меню!\n\n"
        "Тут ви можете:\n\n"
        "Зареєструвати акаунт для роботи\n"
        "Дізнатися інформацію про бот, валюту та методи виводу\n"
        "Звернутися до підтримки\n"
        "Подати заявку на вивід коштів\n"
        "Отримати завдання\n\n"
        "⚠️ Для отримання завдань необхідно спочатку зареєструвати акаунт.\n\n"
        f"👥 Активних користувачів: {active_users}\n\n"
        "Оберіть потрібний пункт нижче 👇"
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
Привіт, {username}! Раді вітати вас у боті з онлайн-завданнями.

📌 Правила роботи:

1️⃣ Виконуйте завдання чесно та додавайте повний скріншот виконання  
2️⃣ Використовуйте лише власні акаунти  
3️⃣ Чітко дотримуйтесь інструкцій  
4️⃣ Не надавайте чужі акаунти  

⚠️ За порушення правил або спробу обману доступ буде закрито.
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

            if action == "task_approve":

                supabase.table("Tasks").update({
                    "status": "Approved",
                    "paid": "Paid",
                    "approve_date": datetime.now().strftime("%d.%m.%Y %H:%M")
                }).eq("id", record_id).execute()

                template = supabase.table("TaskTemplates").select("*").eq("id", task_id).execute().data

                reward = int(template[0]["reward"]) if template else 0

                update_user_balance(user_id, reward)
                add_to_user_total(user_id, reward)

                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=f"✅ Завдання підтверджено. Нараховано {reward} Fanki."
                )

                await safe_edit_caption(query, "✅ Підтверджено")

            else:

                supabase.table("Tasks").update({
                    "status": "Rejected"
                }).eq("id", record_id).execute()

                await context.bot.send_message(
                    chat_id=int(user_id),
                    text="❌ Завдання відхилено."
                )

                await safe_edit_caption(query, "❌ Відхилено")

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
                    text="✅ Ваш вивід підтверджено."
                )

                await query.edit_message_text("✅ Вивід підтверджено")

            else:

                supabase.table("Withdrawals").update({
                    "status": "Rejected"
                }).eq("id", record_id).execute()

                update_user_balance(user_id, amount)

                await context.bot.send_message(
                    chat_id=int(user_id),
                    text="❌ Вивід відхилено. Баланс повернено."
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

    tasks = supabase.table("Tasks").select("*").execute().data
    comments = supabase.table("Comment_Pool").select("*").execute().data
    accounts = supabase.table("Accounts").select("*").execute().data

    if user_id not in user_selected_social:
        await update.message.reply_text("Помилка: соцмережа не вибрана.")
        return

    social_network = user_selected_social[user_id]
    account_name = user_selected_account.get(user_id)

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

    # ==========================
    # TASKS ALREADY DONE BY ACCOUNT
    # ==========================

    done_task_ids = set()

    for r in tasks:

        if (
            (r.get("account") or "").strip().lower() == account_name.strip().lower()
            and str(r.get("social_network")).lower() == str(social_network).lower()
        ):

            try:
                done_task_ids.add(int(r.get("task_id")))
            except:
                pass

    # ==========================
    # SEARCH NEXT TASK
    # ==========================
    unique_templates = {}
    for t in templates:
        if not t.get("active"):
            continue

        if str(t.get("social_network")).lower() !=str(social_network).lower()
            continue
        tid = t.get("task_id")
        if tid not in unique_templates:
            unique_templates[tid] = t

    for template in unique_templates.values():

        try:
            task_id = int(template.get("task_id"))
        except:
            continue

        sn = template.get("social_network")
        task_type = template.get("task_type")
        link = (template.get("link") or "").strip()
        reward = template.get("reward")
        max_per_day = template.get("max_per_day")
        max_total = template.get("max_total")
        active = template.get("active")

        if str(sn).lower() != str(social_network).lower():
            continue

        if not active:
            continue

        # ==========================
        # MAIN RULE
        # 1 ACCOUNT = 1 TASK_ID
        # ==========================

        if task_id in done_task_ids:
            continue

        # ==========================
        # DAILY LIMIT
        # ==========================

        today = datetime.now().strftime("%d.%m.%Y")
        user_today_count = 0

        for t in tasks:

            if (
                (t.get("account") or "").strip().lower() == account_name.strip().lower()
                and str(t.get("task_id")) == str(task_id)
                and t.get("status") == "Approved"
                and str(t.get("assign_date", "")).startswith(today)
            ):
                user_today_count += 1

        if max_per_day and user_today_count >= int(max_per_day):
            continue

        # ==========================
        # GLOBAL LIMIT
        # ==========================

        total_used = 0

        for t in tasks:

            if (
                str(t.get("task_id")) == str(task_id)
                and t.get("status") in ["Pending", "Approved"]
            ):
                total_used += 1

        if max_total and total_used >= int(max_total):
            continue

        # ==========================
        # COMMENT TASK
        # ==========================

        comment_text = ""
        comment_row_id = None

        if str(task_type).lower() == "comment":

            available_comments = [
                c for c in comments
                if str(c.get("task_id")) == str(task_id)
                and c.get("active") == True
            ]

            if not available_comments:
                continue

            comment = available_comments[0]
            comment_text = comment.get("comment")
            comment_row_id = comment.get("id")

        # ==========================
        # SAVE CURRENT TASK
        # ==========================

        current_task[user_id] = {
            "task_id": task_id,
            "social": sn,
            "type": task_type,
            "link": link,
            "reward": reward,
            "comment": comment_text,
            "comment_row_id": comment_row_id
        }

        # ==========================
        # SEND TASK
        # ==========================

        if str(task_type).lower() == "comment":

            msg = (
                f"{link}\n\n"
                f"Дія:\nЗалишити коментар\n"
                f"-------------------------\n\n"
                f"💵 Нагорода:\n{reward} Fanki"
            )

            await update.message.reply_text(msg)
            await update.message.reply_text(comment_text)

        else:

            msg = (
                f"📋 Завдання\n"
                f"Тип: {task_type}\n\n"
                f"{link}\n\n"
                f"Нагорода: {reward} Fanki"
            )

            await update.message.reply_text(msg)

        markup = ReplyKeyboardMarkup(
            [["✅ Виконано"], ["⬅️ Назад"]],
            resize_keyboard=True
        )

        await update.message.reply_text(
            "Після виконання натисніть кнопку нижче.",
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
    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    balance, total, status = get_user_data(user_id)

    if status == "Banned":
        await update.message.reply_text("🚫 Ваш акаунт заблоковано адміністрацією.")
        return

    if status == "Under Review":
        await update.message.reply_text("⏳ Ваш акаунт тимчасово на перевірці.")
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

    if text == "Вивід":
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

    if text == "Мій кабінет":

        balance, total, status = get_user_data(user_id)
        reg_date, completed_tasks = get_user_stats(user_id)

        await update.message.reply_text(
            f"Баланс: {balance} Fanki\n"
            f"Всього зароблено: {total}\n"
            f"Виконано завдань: {completed_tasks}\n"
            f"Дата реєстрації: {reg_date}\n"
            f"Статус: {status}\n"
            f"Конвертація: {balance/1000:.2f}$"
        )
        return

    # ---------------- INFO ----------------

    if text == "Інформація про бот":

        await update.message.reply_text(
            "🤖Про платформу🚀\n"
            "Ми допомагаємо блогерам підтримувати активність у соціальних мережах.\n\n"
            "Працюємо з платформами:\n"
            "• TikTok\n"
            "• Instagram\n"
            "• Facebook\n\n"
            "💼 Ви можете працювати з декількох власних акаунтів кожної соціальної мережі."
        )
        return

    # ---------------- SUPPORT ----------------

    if text == "Підтримка":

        await update.message.reply_text(
            "📩 Підтримка:\nЯкщо у вас виникли питання або проблеми — зверніться до адміністратора."
        )
        return

    # ---------------- REGISTER ACCOUNT ----------------

    if text == "Реєстрація акаунту":

        markup = ReplyKeyboardMarkup(
            [["Instagram", "TikTok", "Facebook", "Google Maps"],
             ["⬅️ Назад"]],
            resize_keyboard=True
        )

        user_state[user_id] = "await_social"

        await update.message.reply_text("Соцмережа:", reply_markup=markup)
        return

    # ---------------- SELECT SOCIAL ----------------

    if state == "await_social":

        allowed_socials = ["Instagram", "TikTok", "Facebook", "Google Maps"]

        if text not in allowed_socials:
            await update.message.reply_text("Оберіть соцмережу кнопкою")
            return

        user_selected_social[user_id] = text
        user_state[user_id] = "await_nick"

        await update.message.reply_text("Введіть нік без @:")
        return

    # ---------------- ENTER NICK ----------------

    if state == "await_nick":

        if len(text) < 2:
            await update.message.reply_text("Занадто коротке ім’я.")
            return

        accounts = get_accounts()

        if any(
            str(row.get("username")).lower() == text.lower()
            for row in accounts
        ):
            await update.message.reply_text("Це ім’я вже зареєстроване.")
            return

        user_selected_account[user_id] = text
        user_state[user_id] = "await_link"

        await update.message.reply_text("Введіть посилання на профіль:")
        return

    # ---------------- ENTER LINK ----------------

    if state == "await_link":

        link = text.strip()

        if not link.startswith("http"):
            await update.message.reply_text("Посилання має починатися з http або https.")
            return

        accounts = get_accounts()

        for row in accounts:

            if (row.get("profile_link") or "").lower() == link.lower():
                await update.message.reply_text("Цей профіль вже зареєстрований.")
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

        await update.message.reply_text("Акаунт відправлено на модерацію.")
        return
# ---------------- TASKS MENU ----------------

    if text == "Завдання":

        accounts = get_accounts()

        approved = [
            row for row in accounts
            if str(row.get("telegram_id")) == str(user_id)
            and row.get("status") == "Approved"
        ]

        if not approved:
            await update.message.reply_text("Немає підтверджених акаунтів.")
            return

        socials = {}

        for row in approved:
            socials.setdefault(row.get("social_network"), []).append(row.get("username"))

        msg = ""

        for social, accs in socials.items():
            msg += f"{social}\n"
            for i, acc in enumerate(accs, start=1):
                msg += f"{i}. {acc}\n"
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
            await update.message.reply_text("Немає акаунтів у цій мережі.")
            return

        markup = ReplyKeyboardMarkup(
            [[acc] for acc in approved_accounts] + [["⬅️ Назад"]],
            resize_keyboard=True
        )

        user_state[user_id] = "select_account"

        await update.message.reply_text("Оберіть акаунт:", reply_markup=markup)
        return

    # ---------------- SELECT ACCOUNT ----------------

    if state == "select_account":

        user_selected_account[user_id] = text
        user_state[user_id] = "working"

        await send_next_task(update, user_id)
        return

    # ---------------- TASK DONE BUTTON ----------------

    if text == "✅ Виконано" and state == "working":

        if user_id not in current_task:
            await update.message.reply_text("Немає активного завдання.")
            return

        user_state[user_id] = "await_screenshot"

        await update.message.reply_text("Надішліть скрін.")
        return

    # ---------------- SCREENSHOT ----------------

    if state == "await_screenshot":

        if not update.message.photo:
            await update.message.reply_text("Будь ласка, надішліть скріншот.")
            return

        file_id = update.message.photo[-1].file_id
        task = current_task.get(user_id)
        account_name = user_selected_account.get(user_id)
        task_id = task["task_id"]

        if not task:
            await update.message.reply_text("Помилка. Спробуйте ще раз.")
            user_state[user_id] = "working"
            return

        print("DEBAG INSERT:", user_id, account_name, task_id)

        res = supabase.table("Tasks").insert({
            "telegram_id": user_id,
            "social_network": task["social"],
            "account": account_name,
            "task_id": task_id,
            "link": task["link"],
            "status": "Pending",
            "assign_date": now,
            "screenfile_id": file_id,
            "comment_text": task.get("comment", "")
        }).execute()

        task_record_id = res.data[0]["id"]

        if task.get("comment_row_id"):

            supabase.table("Comment_Pool").update({
                "active": False
            }).eq("id", task["comment_row_id"]).execute()

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "✅ Підтвердити",
                    callback_data=f"task_approve|{task_record_id}"
                ),
                InlineKeyboardButton(
                    "❌ Відхилити",
                    callback_data=f"task_reject|{task_record_id}"
                )
            ]
        ])

        await context.bot.send_photo(
            ADMIN_ID[0],
            file_id,
            caption=f"ID: {user_id}\nTask: {task['task_id']}",
            reply_markup=keyboard
        )

        await update.message.reply_text("Скрін відправлено на перевірку.")

        current_task.pop(user_id, None)

        if task["social"] == "Google Maps":
            user_state[user_id] = None
            await show_main_menu(update)
            return

        user_state[user_id] = None
        await show_main_menu(update)
        return
# ==============================
# WITHDRAW (USER SIDE)
# ==============================

async def handle_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):

    user_id = update.effective_user.id
    text = update.message.text if update.message.text else ""
    state = user_state.get(user_id)

    if text in ["⬅️ Назад", "Назад"]:

        user_state.pop(user_id, None)
        user_binance_id.pop(user_id, None)
        user_withdraw_amount.pop(user_id, None)

        await show_main_menu(update)
        return True

    withdrawals = get_withdrawals()

    # ---------------- START WITHDRAW ----------------

    if text == "Вивід":

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

    if state == "await_binance":

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

    if state == "await_amount":

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

    if state == "confirm_withdraw" and text == "Так":

        amount = user_withdraw_amount.get(user_id)

        if not amount:
            await update.message.reply_text("Помилка.")
            user_state[user_id] = None
            return True

        now = datetime.now().strftime("%d.%m.%Y %H:%M")

        deduct_user_balance(user_id, amount)

        res = supabase.table("Withdrawals").insert({
            "telegram_id": user_id,
            "username": update.effective_user.username or "",
            "binance_id": user_binance_id[user_id],
            "amount": amount,
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
            f"Вивід\nUser: {user_id}\nСума: {amount}",
            reply_markup=keyboard
        )

        await update.message.reply_text("Заявка створена.")

        user_state[user_id] = None
        return True

    return False
# ==============================
# MESSAGE ROUTER
# ==============================

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):

    user_id = update.effective_user.id
    _, _, status = get_user_data(user_id)

    if status == "Banned":
        if update.message:
            await update.message.reply_text(
                "⛔ Ваш акаунт заблоковано адміністрацією."
            )
        return

    try:

        if not update.message:
            return

        text = update.message.text or ""

        # ================= ADMIN =================

        if is_admin(user_id):

            if text in ["⬅️ Назад", "Назад"]:
                admin_state.pop(user_id, None)
                await show_main_menu(update)
                return

            # -------- STATISTICS --------

            if text == "📊 Статистика":

                users = get_users()
                tasks = get_tasks()
                withdrawals = get_withdrawals()

                total_balance = sum(
                    int(r.get("balance") or 0) for r in users
                )

                total_users = len(users)

                pending_tasks = sum(
                    1 for r in tasks if r.get("status") == "Pending"
                )

                pending_withdraws = sum(
                    1 for r in withdrawals if r.get("status") == "Pending"
                )

                await update.message.reply_text(
                    f"👥 Користувачів: {total_users}\n"
                    f"💰 Сума балансів: {total_balance}\n"
                    f"📋 Pending задач: {pending_tasks}\n"
                    f"💸 Pending виводів: {pending_withdraws}"
                )
                return

            # -------- BAN USER --------

            if text == "🔒 Бан користувача":
                admin_state[user_id] = "await_ban_id"
                await update.message.reply_text("Введіть ID користувача:")
                return

            if admin_state.get(user_id) == "await_ban_id":

                target_id = text.strip()
                users = get_users()

                for row in users:

                    if str(row.get("telegram_id")) == target_id:

                        supabase.table("Users").update({
                            "status": "Banned"
                        }).eq("telegram_id", target_id).execute()

                        await update.message.reply_text("Користувача заблоковано.")
                        admin_state[user_id] = None
                        return

                await update.message.reply_text("Користувача не знайдено.")
                return

            # -------- CHANGE BALANCE --------

            if text == "💰 Змінити баланс":
                admin_state[user_id] = "await_balance_id"
                await update.message.reply_text("Введіть ID користувача:")
                return

            if admin_state.get(user_id) == "await_balance_id":
                admin_state[user_id] = ("await_balance_amount", text)
                await update.message.reply_text("Введіть суму (+500 або -300):")
                return

            if isinstance(admin_state.get(user_id), tuple):

                state_name, target_id = admin_state[user_id]

                if state_name == "await_balance_amount":

                    try:
                        amount = int(text)
                    except:
                        await update.message.reply_text("Введіть число.")
                        return

                    if amount >= 0:
                        update_user_balance(target_id, amount)
                    else:
                        deduct_user_balance(target_id, abs(amount))

                    await update.message.reply_text("Баланс змінено.")
                    admin_state[user_id] = None
                    return

            # -------- ADMIN TASKS --------

            if text == "📋 Завдання":
                await handle_user_message(update, context)
                return

            # -------- ADMIN WITHDRAW --------

            if text == "💸 Виводи":
                handled = await handle_withdraw(update, context)
                if handled:
                    return

            # -------- BROADCAST --------

            if text == "📢 Розсилка":

                admin_state[user_id] = "broadcast"
                await update.message.reply_text("Введіть текст:")
                return

            if admin_state.get(user_id) == "broadcast":

                users = get_users()

                for r in users:

                    try:
                        await context.bot.send_message(
                            r.get("telegram_id"),
                            text
                        )
                    except:
                        pass

                await update.message.reply_text("Розсилка завершена.")
                admin_state[user_id] = None
                return

        # ================= USER =================

        await handle_user_message(update, context)

    except Exception:
        logging.error(traceback.format_exc())
        try:
            await update.message.reply_text("Сталася помилка.")
        except:
            pass


# ==============================
# BUILD APP
# ==============================

def build_app():

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
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


















