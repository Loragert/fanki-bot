# ==============================
# FANki BOT — CLEAN VERSION
# ==============================

import os
import logging
from datetime import datetime

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
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
    raise ValueError("❌ BOT_TOKEN not found")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ SUPABASE credentials not found")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


ADMIN_ID = 6699691752 # твій Telegram ID


# ==============================
# BASIC COMMANDS
# ==============================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await update.message.reply_text(
        f"Привіт, {user.first_name}! 🤖\n"
        f"Clean-версія бота запущена успішно 🚀"
    )


# ==============================
# ERROR HANDLER
# ==============================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logging.error(f"Exception: {context.error}")


# ==============================
# BUILD APP
# ==============================

def build_app():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_error_handler(error_handler)

    return app


# ==============================
# RUN
# ==============================

if __name__ == "__main__":
    print("🚀 Starting Clean FankiBot...")

    app = build_app()
    app.run_polling(drop_pending_updates=True)
