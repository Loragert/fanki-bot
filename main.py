# ==============================
# FANki BOT — CLEAN VERSION
# ==============================

import os
from supabase import create_client
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# ==============================
# CONFIG
# ==============================
BOT_TOKEN = os.getenv("BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN not found")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ SUPABASE credentials not found")

# Підключення до супабази
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==============================
# REGISTER USER IN DATABASE
# ==============================

async def register_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    user_name = user.username or "Unknown"

    # Перевірка, чи є користувач вже в базі
    existing_user = supabase.table("Users").select("*").eq("user_id", user_id).execute().data

    if existing_user:
        # Якщо користувач вже зареєстрований
        await update.message.reply_text("Ви вже зареєстровані!")
    else:
        # Додавання нового користувача в базу
        user_data = {
            "user_id": user_id,
            "username": user_name,
            "status": "Active", # Статус активний
            "balance": 0, # Початковий баланс
            "created_at": datetime.utcnow().isoformat(), # Дата реєстрації
        }

        # Запис нового користувача в таблицю "Users"
        supabase.table("Users").insert(user_data).execute()

        # Відправка повідомлення про успішну реєстрацію
        await update.message.reply_text(f"Привіт, {user_name}! Ви успішно зареєстровані!")

# ==============================
# BUILD APP
# ==============================

def build_app():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("register", register_user)) # Команда реєстрації користувача
    app.add_error_handler(error_handler)

    return app

# ==============================
# RUN
# ==============================

if __name__ == "__main__":
    print("🚀 Starting Clean FankiBot...")
    app = build_app()
    app.run_polling(drop_pending_updates=True)
