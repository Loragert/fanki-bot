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
ADMIN_ID = 6699691752
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
            ADMIN_ID,
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
                                   

logging.basicConfig(level=logging.ERROR)

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
    users = sheet_users.get_all_values()
    for i, row in enumerate(users, start=1):
        if row and row[0] == str(user_id):
            balance = int(row[3]) if len(row) > 3 and row[3] else 0
            total = int(row[4]) if len(row) > 4 and row[4] else 0
            status = row[5] if len(row) > 5 else "Active"
            return balance, total, status
    return 0, 0, "Active"

def update_user_balance(user_id, amount):
    users = sheet_users.get_all_values()
    for i, row in enumerate(users, start=1):
        if row and row[0] == str(user_id):
            balance = int(row[3]) if row[3] else 0
            sheet_users.update_cell(i, 4, str(balance + amount))
            return

def deduct_user_balance(user_id, amount):
    users = sheet_users.get_all_values()
    for i, row in enumerate(users, start=1):
        if row and row[0] == str(user_id):
            balance = int(row[3]) if row[3] else 0
            sheet_users.update_cell(i, 4, str(balance - amount))
            return
def add_to_user_total(user_id, amount):
    users = sheet_users.get_all_values()
    for i, row in enumerate(users, start=1):
        if row and row[0] == str(user_id):
            total = int(row[4]) if len(row) > 4 and row[4] else 0
            sheet_users.update_cell(i, 5, str(total + amount))
            return

def get_user_stats(user_id):
    users = sheet_users.get_all_values()
    tasks = sheet_tasks.get_all_values()

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
# MENU
# ==============================

async def show_main_menu(update: Update):
    user_id = update.effective_user.id

    if user_id == ADMIN_ID:
        markup = ReplyKeyboardMarkup(
            [
                ["📋 Завдання"],
                ["💸 Виводи"],
                ["💰 Змінити баланс"],
                ["📢 Розсилка"],
                ["📊 Статистика"]
            ],
            resize_keyboard=True
        )
        await update.message.reply_text("🛠 Адмін панель\nВітаємо в головному меню, оберіть пункт.", reply_markup=markup)
        return

    users = sheet_users.get_all_values()
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
    await query.answer()

    if update.effective_user.id != ADMIN_ID:
        return

    data = query.data.split("|")
    action = data[0]
    row_index = int(data[1])

    if action.startswith("task"):

        row = sheet_tasks.row_values(row_index)

        if len(row) < 5 or row[4] != "Pending":
            return

        user_id = row[0]
        task_id = row[3]

        if action == "task_approve":

            sheet_tasks.update_cell(row_index, 5, "Approved")
            sheet_tasks.update_cell(row_index, 9, "Paid")

            templates = sheet_templates.get_all_values()
            reward = 0
            for t in templates:
                if t and t[0] == task_id:
                    reward = int(t[4])
                    break

            update_user_balance(user_id, reward)
            add_to_user_total(user_id, reward)

            await context.bot.send_message(
                chat_id=int(user_id),
                text=f"✅ Завдання підтверджено. Нараховано {reward} Fanki."
            )

            await safe_edit_caption(query, "✅ Підтверджено")

        if action == "task_reject":

            sheet_tasks.update_cell(row_index, 5, "Rejected")

            if len(row) > 7 and row[7]:
                comments = sheet_comment_pool.get_all_values()
                for i, c in enumerate(comments, start=1):
                    if c and c[1] == row[7]:
                        sheet_comment_pool.update_cell(i, 3, "TRUE")

            await context.bot.send_message(
                user_id,
                "❌ Завдання відхилено."
            )

            await safe_edit_caption(query, "❌ Відхилено")

    if action.startswith("withdraw"):

        row = sheet_withdrawals.row_values(row_index)

        if len(row) < 5 or row[4] != "Pending":
            return

        user_id = row[0]
        amount = int(row[3])

        if action == "withdraw_approve":

            sheet_withdrawals.update_cell(row_index, 5, "Approved")

            await context.bot.send_message(
                user_id,
                "✅ Ваш вивід підтверджено."
            )

            await safe_edit_caption(query, "✅ Вивід підтверджено")

        if action == "withdraw_reject":

            sheet_withdrawals.update_cell(row_index, 5, "Rejected")

            update_user_balance(user_id, amount)

            await context.bot.send_message(
                user_id,
                "❌ Вивід відхилено. Баланс повернено."
            )

            await safe_edit_caption(query, "❌ Вивід відхилено")

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
    templates = sheet_templates.get_all_values()
    tasks = sheet_tasks.get_all_values()
    comments = sheet_comment_pool.get_all_values()
    accounts = sheet_accounts.get_all_values()

    account_name = user_selected_account.get(user_id)

    account_row = next(
        (r for r in accounts if r and r[0] == str(user_id)
         and r[2] == account_name and r[3] == "Approved"),
        None
    )

    if not account_row:
        await update.message.reply_text("Акаунт не підтверджений.")
        return

    social_network = account_row[1]

    done_tasks = [
        r[3] for r in tasks
        if r and len(r) > 3 and r[2] == account_name
    ]

    for template in templates:

        if not template or len(template) < 7:
            continue

        task_id, sn, task_type, link, reward, max_per_day, active = template

        if sn != social_network or active != "TRUE" or task_id in done_tasks:
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

    state = user_state.get(user_id)

    accounts = sheet_accounts.get_all_values()

    if state == "await_accept" and text == "Приймаю":

        users = sheet_users.get_all_values()

        if not any(r and r[0] == str(user_id) for r in users):
            sheet_users.append_row([user_id, username, now, "0", "0", "Active"])

        user_state[user_id] = None

        await show_main_menu(update)
        return

    if text == "⬅️ Назад":
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

        user_selected_social[user_id] = text
        user_state[user_id] = "await_nick"

        await update.message.reply_text("Введіть нік без @:")
        return

    if state == "await_nick":

        if not re.match(r'^[A-Za-z0-9_.]+$', text):
            await update.message.reply_text("Невірний формат ніку.")
            return

        if any(row and row[2].lower() == text.lower() for row in accounts):
            await update.message.reply_text("Цей нік вже зареєстрований.")
            return

        if user_selected_social[user_id] == "Google Maps":
            if any(row and row[0] == str(user_id)
                   and row[1] == "Google Maps" for row in accounts):
                await update.message.reply_text(
                    "Google Maps можна додати тільки один акаунт."
                )
                return

        sheet_accounts.append_row(
            [user_id,
             user_selected_social[user_id],
             text,
             "Pending",
             now]
        )

        user_state[user_id] = None

        await update.message.reply_text(
            "Акаунт відправлено на модерацію."
        )

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
            task["comment"],
            ""
        ])

        if task["comment_row_index"]:
            sheet_comment_pool.update_cell(
                task["comment_row_index"],
                3,
                "FALSE"
            )

        tasks = sheet_tasks.get_all_values()
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
            ADMIN_ID,
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

    await show_main_menu(update)

# ==============================
# WITHDRAW (USER SIDE)
# ==============================

async def handle_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):

    user_id = update.effective_user.id
    text = update.message.text if update.message.text else ""
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    state = user_state.get(user_id)

    withdrawals = sheet_withdrawals.get_all_values()

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
        await update.message.reply_text("Введіть Binance ID:")
        return True

    if state == "await_binance":

        if not text.isdigit():
            await update.message.reply_text(
                "Binance ID повинен містити тільки цифри."
            )
            return True

        user_binance_id[user_id] = text
        user_state[user_id] = "await_amount"

        await update.message.reply_text("Введіть суму:")
        return True

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
            f"Підтвердити вивід {amount} Fanki $({amount/1000:.2f}) на Binance ID {user_binance_id[user_id]}?",
            reply_markup=markup
        )

        return True

    if state == "confirm_withdraw" and text == "Так":

        amount = user_withdraw_amount.get(user_id)

        if not amount:
            await update.message.reply_text("Помилка.")
            user_state[user_id] = None
            return True

        deduct_user_balance(user_id, amount)

        sheet_withdrawals.append_row([
            user_id,
            update.effective_user.username or "",
            user_binance_id[user_id],
            str(amount),
            "Pending",
            now
        ])

        withdrawals = sheet_withdrawals.get_all_values()
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
            ADMIN_ID,
            f"Вивід\nUser: {user_id}\nСума: {amount}",
            reply_markup=keyboard
        )

        await update.message.reply_text(
            "Заявка створена."
        )

        user_state[user_id] = None
        return True

    return False

# ==============================
# MESSAGE ROUTER
# ==============================

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):

    try:

        if not update.message:
            return

        user_id = update.effective_user.id

        if user_id == ADMIN_ID:

            text = update.message.text if update.message.text else ""

            if text == "📊 Статистика":

                users = sheet_users.get_all_values()
                tasks = sheet_tasks.get_all_values()
                withdrawals = sheet_withdrawals.get_all_values()

                total_balance = sum(
                    int(r[3]) for r in users
                    if len(r) > 3 and r[3]
                )

                total_earned = sum(
                    int(r[4]) for r in users
                    if len(r) > 4 and r[4]
                )

                pending_tasks = sum(
                    1 for r in tasks
                    if r and r[4] == "Pending"
                )

                pending_withdraws = sum(
                    1 for r in withdrawals
                    if r and r[4] == "Pending"
                )

                await update.message.reply_text(
                    f"👥 Користувачів: {len(users)}\n"
                    f"💰 Сума балансів: {total_balance}\n"
                    f"📈 Всього зароблено: {total_earned}\n"
                    f"📋 Pending задач: {pending_tasks}\n"
                    f"💸 Pending виводів: {pending_withdraws}"
                )
                return

            if text == "💰 Змінити баланс":

                admin_state[user_id] = "await_user_id"
                await update.message.reply_text("Введіть ID користувача:")
                return

            if admin_state.get(user_id) == "await_user_id":

                admin_state[user_id] = ("await_amount", text)
                await update.message.reply_text("Введіть суму (+500 або -300):")
                return

            if isinstance(admin_state.get(user_id), tuple):

                state_name, target_id = admin_state[user_id]

                if state_name == "await_amount":

                    try:
                        amount = int(update.message.text)
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

            if text == "📢 Розсилка":

                admin_state[user_id] = "broadcast"
                await update.message.reply_text("Введіть текст:")
                return

            if admin_state.get(user_id) == "broadcast":

                users = sheet_users.get_all_values()

                for r in users:
                    try:
                        await context.bot.send_message(r[0], update.message.text)
                    except:
                        pass

                await update.message.reply_text("Розсилка завершена.")
                admin_state[user_id] = None
                return

        handled = await handle_withdraw(update, context)
        if handled:
            return

        await handle_user_message(update, context)

    except Exception as e:
        logging.error(f"Runtime error: {e}")
        try:
            await update.message.reply_text(
                "Сталася помилка. Спробуйте ще раз."
            )
        except:
            pass

# ==============================
# FINAL RUN
# ==============================

if __name__ == "__main__":

    app = build_app()

    app.add_handler(MessageHandler(filters.ALL, handle_message))

    print("FankiBot Production Ready 🚀")


    app.run_polling()





