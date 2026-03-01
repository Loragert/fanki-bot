# -- coding:utf-8 --

import re
import logging
import traceback
import asyncio
from datetime import datetime
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
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==============================
# CONFIG
# ==============================

import os
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise ValueError("BOT_TOKEN не знайдено!")
ADMIN_ID = [6699691752]
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


def safe_google_call(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        error_text = f"Google error:\n{str(e)}\n\n{traceback.format_exc()}"
        print(error_text)
        notify_admin_async(error_text)
        return None
                                   

logging.basicConfig(level=logging.INFO)

# ==============================
# GOOGLE SHEETS
# ==============================

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = safe_google_call(
    lambda: ServiceAccountCredentials.from_json_keyfile_name(
        "/workspace/creds.json", scope
    )
)

client = safe_google_call(
    lambda: gspread.authorize(creds)
) if creds else None

sheet_users = safe_google_call(
    lambda: client.open("FankiBot").worksheet("Users")
) if client else None

sheet_accounts = safe_google_call(
    lambda: client.open("FankiBot").worksheet("Accounts")
) if client else None

sheet_withdrawals = safe_google_call(
    lambda: client.open("FankiBot").worksheet("Withdrawals")
) if client else None

sheet_templates = safe_google_call(
    lambda: client.open("FankiBot").worksheet("TaskTemplates")
) if client else None

sheet_tasks = safe_google_call(
    lambda: client.open("FankiBot").worksheet("Tasks")
) if client else None

sheet_comment_pool = safe_google_call(
    lambda: client.open("FankiBot").worksheet("Comment_Pool")
) if client else None

sheet_admin_logs = safe_google_call(
    lambda:client.open("FankiBot").worksheet("AdminLogs")
) if client else None

# ==============================
# CACHE
# ==============================

cached_users = []
cached_tasks = []
cached_templates = []
cached_accounts = []
cached_withdrawals = []
cached_comments = []

def refresh_cache():
    global cached_users, cached_tasks, cached_templates
    global cached_accounts, cached_withdrawals, cached_comments

    if sheet_users:
        cached_users = sheet_users.get_all_values()
    if sheet_tasks:
        cached_tasks = sheet_tasks.get_all_values()
    if sheet_templates:
        cached_templates = sheet_templates.get_all_values()
    if sheet_accounts:
        cached_accounts = sheet_accounts.get_all_values()
    if sheet_withdrawals:
        cached_withdrawals = sheet_withdrawals.get_all_values()
    if sheet_comment_pool:
        cached_comments = sheet_comment_pool.get_all_values()

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
# BALANCE FUNCTIONS
# ==============================

def get_user_data(user_id):
    users = cached_users
    for i, row in enumerate(users, start=1):
        if row and row[0] == str(user_id):
            balance = int(row[3]) if len(row) > 3 and row[3] else 0
            total = int(row[4]) if len(row) > 4 and row[4] else 0
            status = row[5] if len(row) > 5 else "Active"
            return balance, total, status
    return 0, 0, "Active"

def update_user_balance(user_id, amount):
    users = cached_users
    for i, row in enumerate(users, start=1):
        if row and row[0] == str(user_id):
            balance = int(row[3]) if row[3] else 0
            sheet_users.update_cell(i, 4, str(balance + amount))
            return
        
def deduct_user_balance(user_id, amount):
    users = cached_users
    for i, row in enumerate(users, start=1):
        if row and row[0] == str(user_id):
            balance = int(row[3]) if row[3] else 0
            sheet_users.update_cell(i, 4, str(balance - amount))
            return
def add_to_user_total(user_id, amount):
    users = cached_users
    for i, row in enumerate(users, start=1):
        if row and row[0] == str(user_id):
            total = int(row[4]) if len(row) > 4 and row[4] else 0
            sheet_users.update_cell(i, 5, str(total + amount))
            return

def get_user_stats(user_id):
    users = cached_users
    tasks = cached_tasks

    reg_date = "-"
    comleted_tasks = 0

    # date
    for row in users:
        if row and row[0] == str(user_id):
            if len(row) > 2:
                reg_date = row[2]
            break

        #pidtvwrd
    for row in tasks:
        if row and row[0] == str(user_id) and len(row) > 4:
            if row[4] == "Approved":
                comleted_tasks += 1
                
    return reg_date, comleted_tasks

# ==============================
# ADMIN PANEL (INLINE)
# ==============================

def is_admin(user_id):
    return user_id in ADMIN_ID


def log_admin_action(admin_id, action, target_user_id="", details=""):
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    sheet_admin_logs.append_row([
        now,
        str(admin_id),
        action,
        str(target_user_id),
        details
    ])


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

    # ---------------- USERS MENU ----------------
    if data == "admin_users":
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 Пошук по ID", callback_data="admin_search_user")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="admin_back")]
        ])

        await query.edit_message_text(
            "👤 Користувачі",
            reply_markup=keyboard
        )

    # ---------------- SEARCH ----------------
    elif data == "admin_search_user":
        context.user_data["admin_state"] = "await_user_id"
        await query.edit_message_text("Введіть ID користувача:")

    # ---------------- BACK ----------------
    elif data == "admin_back":
        await show_admin_panel(update, context)

    # ---------------- USER PROFILE ACTIONS ----------------
    elif data.startswith("admin_ban_"):
        target_id = data.split("_")[-1]
        

        for i, row in enumerate(cached_users, start=1):
            if row and row[0] == target_id:
                sheet_users.update_cell(i, 6, "Banned")
                break

        log_admin_action(admin_id, "BAN", target_id, "Manual ban")
        refresh_cache()

        await query.edit_message_text(f"🔴 Користувач {target_id} заблокований.")

    elif data.startswith("admin_unban_"):
        target_id = data.split("_")[-1]
        

        for i, row in enumerate(cached_users, start=1):
            if row and row[0] == target_id:
                sheet_users.update_cell(i, 6, "Active")
                break

        log_admin_action(admin_id, "UNBAN", target_id, "Manual unban")
        refresh_cache()

        await query.edit_message_text(f"🟢 Користувач {target_id} активований.")

    elif data.startswith("admin_balance_"):
        target_id = data.split("_")[-1]
        context.user_data["admin_state"] = ("await_balance", target_id)
        await query.edit_message_text("Введіть суму (+500 або -300):")

    # ---------------- STATS ----------------
    elif data == "admin_stats":
        

        total_users = len(cached_users) - 1
        active = sum(1 for r in cached_users if len(r) > 5 and r[5] == "Active")
        banned = sum(1 for r in cached_users if len(r) > 5 and r[5] == "Banned")

        total_balance = sum(
            int(r[3]) for r in cached_users[1:]
            if len(r) > 3 and r[3].isdigit()
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
        await update.message.reply_text("🛠 Адмін панель\nВітаємо в головному меню, оберіть пункт.", reply_markup=markup)
        return

    users = cached_users
    active_users = len(users) - 1 if len(users) > 1 else 0


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
    await update.message.reply_text(text,reply_markup=markup)



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

    # --- БЕЗПЕЧНЕ читання callback_data ---
    data_raw = query.data or ""

    if "|" not in data_raw:
        return

    parts = data_raw.split("|")

    if len(parts) < 2:
        return

    action = parts[0]

    try:
        row_index = int(parts[1])
    except:
        return

    # --- БЕЗПЕЧНА перевірка row_index ---
    if row_index <= 0:
        return

    try:

        # =========================
        # ACCOUNT
        # =========================
        if action in ["account_approve", "account_reject"]:

            row = sheet_accounts.row_values(row_index)

            if not row or len(row) < 4:
                return

            if row[3] != "Pending":
                return

            user_id = row[0]
            social = row[1]
            nickname = row[2]

            if action == "account_approve":

                sheet_accounts.update_cell(row_index, 4, "Approved")
                refresh_cache()

                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=f"✅ Ваш акаунт {nickname} ({social}) підтверджено."
                )

                await query.edit_message_text("✅ Акаунт підтверджено")

            else:

                sheet_accounts.update_cell(row_index, 4, "Rejected")
                refresh_cache()

                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=f"❌ Ваш акаунт {nickname} ({social}) відхилено."
                )

                await query.edit_message_text("❌ Акаунт відхилено")

        # =========================
        # TASK
        # =========================
        elif action in ["task_approve", "task_reject"]:

            row = sheet_tasks.row_values(row_index)

            if not row or len(row) < 5:
                return

            if row[4] != "Pending":
                return

            user_id = row[0]
            task_id = row[3]

            if action == "task_approve":

                sheet_tasks.update_cell(row_index, 5, "Approved")
                sheet_tasks.update_cell(row_index, 9, "Paid")
                now = datetime.now().strftime("%d.%m.%Y %H:%M")
                sheet_tasks.update_cell(row_index, 10, now)
                

                reward = 0
                for t in cached_templates:
                    if t and t[0] == task_id:
                        reward = int(t[4])
                        break

                update_user_balance(user_id, reward)
                add_to_user_total(user_id, reward)

                refresh_cache()

                await context.bot.send_message(
                    chat_id=int(user_id),
                    text=f"✅ Завдання підтверджено. Нараховано {reward} Fanki."
                )

                await query.edit_message_caption("✅ Підтверджено")

            else:

                sheet_tasks.update_cell(row_index, 5, "Rejected")
                refresh_cache()

                await context.bot.send_message(
                    chat_id=int(user_id),
                    text="❌ Завдання відхилено."
                )

                await query.edit_message_caption("❌ Відхилено")

        # =========================
        # WITHDRAW
        # =========================
        elif action in ["withdraw_approve", "withdraw_reject"]:

            row = sheet_withdrawals.row_values(row_index)

            if not row or len(row) < 5:
                return

            if row[4] != "Pending":
                return

            user_id = row[0]
            amount = int(row[3])

            if action == "withdraw_approve":

                sheet_withdrawals.update_cell(row_index, 5, "Approved")
                refresh_cache()

                await context.bot.send_message(
                    chat_id=int(user_id),
                    text="✅ Ваш вивід підтверджено."
                )

                await query.edit_message_text("✅ Вивід підтверджено")

            else:

                sheet_withdrawals.update_cell(row_index, 5, "Rejected")
                update_user_balance(user_id, amount)
                refresh_cache()

                await context.bot.send_message(
                    chat_id=int(user_id),
                    text="❌ Вивід відхилено. Баланс повернено."
                )

                await query.edit_message_text("❌ Вивід відхилено")

    except Exception as e:
        logging.error(f"Callback error: {e}")
       

# ==============================
# GLOBAL ERROR HANDLER
# ==============================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logging.error(f"Exception: {context.error}")

# ==============================
# RUN (message handler буде в частині 2)
# ==============================

def build_app():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_error_handler(error_handler)
    return app

# ==============================
# SEND NEXT TASK
# ==============================

async def send_next_task(update: Update, user_id: str):
    

    templates = cached_templates
    tasks = cached_tasks
    comments = cached_comments
    accounts = cached_accounts

    print("HEADERS:", templates[0])

    account_name = user_selected_account.get(user_id)

    account_row = next(
        (r for r in accounts if r and r[0] == str(user_id)
         and r[2] == account_name and r[3] == "Approved"),
        None
    )

    if not account_row:
        await update.message.reply_text("Акаунт не підтверджений.")
        return

    social_network = user_selected_social.get(user_id)

    done_tasks = [
        r[3] for r in tasks
        if ( r and len(r) > 3 and r[0] == str(user_id) and r[2] == account_name )
    ]

    for template in templates[1:]:

        if not template or len(template) < 8:
            continue

        if not template[0].isdigit(): continue

        task_id = template[0]
        sn = template[1]
        task_type = template[2]
        link = template[3]
        reward = template[4]
        max_per_day = template[5]
        max_total = template[6]
        active = template[7]

        if not sn or not social_network:
            continue

        if sn.strip().lower() != social_network.strip().lower():
            continue

        if active.strip().upper() != "TRUE":
            continue

        if task_id in done_tasks:
            continue


# ==============================
# 🔹 ЛІМІТ НА КОРИСТУВАЧА В ДЕНЬ
# ==============================

        today = datetime.now().strftime("%d.%m.%Y")

        user_today_count = 0

        for t in tasks:
            if (
                t
                and len(t) > 5
                and t[0] == str(user_id)
                and t[3] == task_id
                and t[4] == "Approved"
                and t[5].startswith(today)
            ):
                user_today_count += 1

        if max_per_day and user_today_count >= int(max_per_day):
            continue


# ==============================
# 🔹 ГЛОБАЛЬНИЙ ЛІМІТ
# ==============================

        total_used = 0

        for t in tasks:
            if (
                t
                and len(t) > 4
                and t[3] == task_id
                and t[4] in ["Pending", "Approved"] 
            ):
                total_used += 1

        if max_total and total_used >= int(max_total):
            continue
            
        comment_text = ""
        comment_row_index = None

        if task_type.lower() == "comment":

            task_comments = [
                (i + 1, row)
                for i, row in enumerate(comments)
                if row and row[0] == task_id and row[2] == "TRUE"
            ]

            if not task_comments:
                continue

            row_index, comment_row = task_comments[0]
            comment_text = comment_row[1]
            comment_row_index = row_index

        current_task[user_id] = {
            "task_id": task_id,
            "social": sn,
            "type": task_type,
            "link": link,
            "reward": reward,
            "comment": comment_text,
            "comment_row_index": comment_row_index
        }

        if task_type.lower() == "comment":

            msg = (
                f"Посилання:\n{link}\n\n"
                f"Дія:\nЗалишити коментар\n"
                f"-------------------------\n\n"
                f"💵 Нагорода:\n{reward} Fanki"
            )

            await update.message.reply_text(msg)
            await update.message.reply_text(comment_text)

        else:

            msg = (
                f"📋 Завдання\n"
                f"Тип: {task_type}\n"
                f"Посилання: {link}\n"
                f"Нагорода: {reward} Fanki\n\n"
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

        return

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
        await update.message.reply_text(
        "🚫 Ваш акаунт заблоковано адміністрацією."
        )
        return

    if status == "Under Review":
        await update.message.reply_text(
        "⏳ Ваш акаунт тимчасово на перевірці."
        )
        return
    
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

    # 🔥 ОКРЕМИЙ блок для виводу
    if text == "Вивід":
        user_state.pop(user_id, None)
        return await handle_withdraw(update, context)

    print("STATE DEBAG:", user_id, user_state.get(user_id), "| TEXT:", text)
    print("HANDLE_MESSAGE TRIGGERED")
    print("USER:", user_id)
    print("TEXT:", text)
    print("STATE:", user_state.get(user_id))
    
    state = user_state.get(user_id)
    accounts = cached_accounts or []
    if state == "await_accept" and text == "Приймаю":

        users = cached_users

        if not any(r and r[0] == str(user_id) for r in users):
            sheet_users.append_row([user_id, username, now, "0", "0", "Active"])
            refresh_cache()
        user_state[user_id] = None

        await show_main_menu(update)
        return

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

    if text == "Інформація про бот":
        await update.message.reply_text(
            "🤖Про платформу🚀" \
            "Ми допомагаємо блогерам підтримувати активність у соціальних мережах." \
            "Працюємо з платформами" \
            "• TikTok" \
            "• Instagram" \
            "• Facebook" \
            "💼 Ви можете працювати з декількох власних акаунтів кожної соціальної мережі."
        )
        return

    if text == "Підтримка":
        await update.message.reply_text(
            "📩 Підтримка:\nЯкщо у вас виникли питання або проблеми — зверніться до адміністратора."
        )
        return

    if text == "Реєстрація акаунту":

        markup = ReplyKeyboardMarkup(
            [["Instagram", "TikTok", "Facebook", "Google Maps"],
             ["⬅️ Назад"]],
            resize_keyboard=True
        )

        user_state[user_id] = "await_social"

        await update.message.reply_text("Соцмережа:", reply_markup=markup)
        return

    if state == "await_social":
        allowed_socials = ["Instagram", "TikTok", "Facebook", "Google Maps"]

        if text not in allowed_socials:
            await update.message.reply_text("Оберіть соцмережу кнопкою")
            return
        user_selected_social[user_id] = text
        user_state[user_id] = "await_nick" 

        await update.message.reply_text("Введіть нік без @:")
        return

    if state == "await_nick":
                # дозволяємо будь-які символи (бо Facebook ім’я)
        if len(text) < 2:
            await update.message.reply_text("Занадто коротке ім’я.")
            return

        accounts = cached_accounts[1:] if len(cached_accounts) > 1 else []

        if any(row and len(row) > 2 and row[2] and row[2].lower() == text.lower() for row in accounts):
            await update.message.reply_text("Це ім’я вже зареєстроване.")
            return

        user_selected_account[user_id] = text
        user_state[user_id] = "await_link"

        await update.message.reply_text("Введіть посилання на профіль:")
        return

    if state == "await_link":

        link = text.strip()

        if not link.startswith("http"):
            await update.message.reply_text("Посилання має починатися з http або https.")
            return

        sheet_accounts.append_row([
            user_id,
            user_selected_social[user_id],
            user_selected_account[user_id],
            "Pending",
            now,
            link
        ])

        refresh_cache()

        accounts = cached_accounts
        row_index = len(accounts)

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "✅ Підтвердити",
                    callback_data=f"account_approve|{row_index}"
                ),
                InlineKeyboardButton(
                    "❌ Відхилити",
                    callback_data=f"account_reject|{row_index}"
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

    if text == "Завдання":

        approved = [
            row for row in accounts
            if row and row[0] == str(user_id)
            and row[3] == "Approved"
        ]

        if not approved:
            await update.message.reply_text(
                "Немає підтверджених акаунтів."
            )
            return

        socials = {}

        for row in approved:
            socials.setdefault(row[1], []).append(row[2])

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

    if state == "select_social":

        user_selected_social[user_id] = text

        approved_accounts = [
            row[2] for row in accounts
            if row and row[0] == str(user_id)
            and row[1] == text
            and row[3] == "Approved"
        ]

        if not approved_accounts:
            await update.message.reply_text(
                "Немає акаунтів у цій мережі."
            )
            return

        markup = ReplyKeyboardMarkup(
            [[acc] for acc in approved_accounts] + [["⬅️ Назад"]],
            resize_keyboard=True
        )

        user_state[user_id] = "select_account"

        await update.message.reply_text(
            "Оберіть акаунт:",
            reply_markup=markup
        )

        return

    if state == "select_account":

        user_selected_account[user_id] = text
        user_state[user_id] = "working"

        await send_next_task(update, user_id)
        return

    if text == "✅ Виконано" and state == "working":

        if user_id not in current_task:
            await update.message.reply_text(
                "Немає активного завдання."
            )
            return

        user_state[user_id] = "await_screenshot"

        await update.message.reply_text("Надішліть скрін.")
        return

    if state == "await_screenshot":

        if not update.message.photo:
            await update.message.reply_text(
                "Будь ласка, надішліть скріншот."
            )
            return

        file_id = update.message.photo[-1].file_id
        task = current_task.get(user_id)

        if not task:
            await update.message.reply_text(
                "Помилка. Спробуйте ще раз."
            )
            user_state[user_id] = "working"
            return

        sheet_tasks.append_row([
            user_id,
            task["social"],
            user_selected_account[user_id],
            task["task_id"],
            "Pending",
            now,
            file_id,
            task.get("comment", "") or "",
            ""
        ])
        

        if task["comment_row_index"]:
            sheet_comment_pool.update_cell(
                task["comment_row_index"],
                3,
                "FALSE"
            )
        refresh_cache()
        tasks = cached_tasks
        row_index = len(tasks)

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "✅ Підтвердити",
                    callback_data=f"task_approve|{row_index}"
                ),
                InlineKeyboardButton(
                    "❌ Відхилити",
                    callback_data=f"task_reject|{row_index}"
                )
            ]
        ])

        await context.bot.send_photo(
            ADMIN_ID[0],
            file_id,
            caption=f"ID: {user_id}\nTask: {task['task_id']}",
            reply_markup=keyboard
        )

        await update.message.reply_text(
            "Скрін відправлено на перевірку."
        )

        if task["social"] == "Google Maps":
            user_state[user_id] = None
            await show_main_menu(update)
            return

        user_state[user_id] = "working"
        await send_next_task(update, user_id)
        return

# ==============================
# WITHDRAW (USER SIDE)
# ==============================

async def handle_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):

    user_id = update.effective_user.id
    text = update.message.text if update.message.text else ""
    state = user_state.get(user_id)

    # 🔥 Глобальний "Назад" всередині withdraw
    if text in ["⬅️ Назад", "Назад"]:
        user_state.pop(user_id, None)
        user_binance_id.pop(user_id, None)
        user_withdraw_amount.pop(user_id, None)
        await show_main_menu(update)
        return True

    withdrawals = cached_withdrawals

    # --- START WITHDRAW ---
    if text == "Вивід":

        balance, _, _ = get_user_data(user_id)

        pending = any(
            r and r[0] == str(user_id) and r[4] == "Pending"
            for r in withdrawals
        )

        if pending:
            await update.message.reply_text(
                "У вас вже є заявка на розгляді."
            )
            return True

        if balance < 1000:
            await update.message.reply_text(
                "Мінімум для виводу 1000 Fanki."
            )
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

    # --- BINANCE ID ---
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

    # --- AMOUNT ---
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
            await update.message.reply_text(
                "Недостатньо коштів."
            )
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

    # --- CONFIRM ---
    if state == "confirm_withdraw" and text == "Так":

        amount = user_withdraw_amount.get(user_id)

        if not amount:
            await update.message.reply_text("Помилка.")
            user_state[user_id] = None
            return True

        now = datetime.now().strftime("%d.%m.%Y %H:%M")

        deduct_user_balance(user_id, amount)

        sheet_withdrawals.append_row([
            user_id,
            update.effective_user.username or "",
            user_binance_id[user_id],
            str(amount),
            "Pending",
            now
        ])
        refresh_cache()

        withdrawals = cached_withdrawals
        row_index = len(withdrawals)

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "✅ Підтвердити",
                    callback_data=f"withdraw_approve|{row_index}"
                ),
                InlineKeyboardButton(
                    "❌ Відхилити",
                    callback_data=f"withdraw_reject|{row_index}"
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

    # 🔒 ЖОРСТКИЙ БАН
    if status == "Banned":
        if update.message:
            await update.message.reply_text("⛔ Ваш акаунт заблоковано адміністрацією.")
        return

    try:
        if not update.message:
            return

        text = update.message.text or ""

        # ================= ADMIN =================
        if is_admin(user_id):

            # Назад
            if text in ["⬅️ Назад", "Назад"]:
                admin_state.pop(user_id, None)
                await show_main_menu(update)
                return

            # 📊 Статистика
            if text == "📊 Статистика":
                

                users = cached_users
                tasks = cached_tasks
                withdrawals = cached_withdrawals

                total_balance = sum(
                    int(r[3]) for r in users[1:]
                    if len(r) > 3 and r[3].isdigit()
                )

                total_users = max(len(users) - 1, 0)

                pending_tasks = sum(
                    1 for r in tasks if len(r) > 4 and r[4] == "Pending"
                )

                pending_withdraws = sum(
                    1 for r in withdrawals if len(r) > 4 and r[4] == "Pending"
                )

                await update.message.reply_text(
                    f"👥 Користувачів: {total_users}\n"
                    f"💰 Сума балансів: {total_balance}\n"
                    f"📋 Pending задач: {pending_tasks}\n"
                    f"💸 Pending виводів: {pending_withdraws}"
                )
                return

            # 🔒 Бан користувача
            if text == "🔒 Бан користувача":
                admin_state[user_id] = "await_ban_id"
                await update.message.reply_text("Введіть ID користувача:")
                return

            if admin_state.get(user_id) == "await_ban_id":
                
                target_id = text.strip()
                
                for i, row in enumerate(cached_users, start=1):
                    if row and row[0] == target_id:
                        sheet_users.update_cell(i, 6, "Banned")
                        await update.message.reply_text("Користувача заблоковано.")
                        admin_state[user_id] = None
                        refresh_cache()
                        return

                await update.message.reply_text("Користувача не знайдено.")
                return

            # 💰 Змінити баланс
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

                    refresh_cache()
                    await update.message.reply_text("Баланс змінено.")
                    admin_state[user_id] = None
                    return

            # 📋 Завдання
            if text == "📋 Завдання":
                await handle_user_message(update, context)
                return

            # 💸 Виводи
            if text == "💸 Виводи":
                handled = await handle_withdraw(update, context)
                if handled:
                    return

            # 📢 Розсилка
            if text == "📢 Розсилка":
                admin_state[user_id] = "broadcast"
                await update.message.reply_text("Введіть текст:")
                return

            if admin_state.get(user_id) == "broadcast":
                for r in cached_users[1:]:
                    try:
                        await context.bot.send_message(r[0], text)
                    except:
                        pass

                await update.message.reply_text("Розсилка завершена.")
                admin_state[user_id] = None
                return

        # ================= USER =================
        
        await handle_user_message(update, context)

    except Exception as e:
        logging.error(traceback.format_exc())
        try:
            await

            update.message.reply_text(str(e))
        except:
            pass
       


if __name__ == "__main__":
    refresh_cache()

    app = build_app()

    app.add_handler(
    MessageHandler(
        (filters.TEXT | filters.PHOTO) & ~filters.COMMAND,
        handle_message
    )
    ) 

    print("FankiBot Production Ready 🚀")

    app.run_polling(drop_pending_updates=True)























