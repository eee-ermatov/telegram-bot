import os

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID          = int(os.getenv("ADMIN_ID", 0))
API_ID            = int(os.getenv("API_ID", 0))
API_HASH          = os.getenv("API_HASH")
LOG_CHANNEL_ID    = int(os.getenv("LOG_CHANNEL_ID", 0))
STATS_CHANNEL_ID  = int(os.getenv("STATS_CHANNEL_ID", 0))

# Оставляем их дефолты, если не задали:
MIN_JOIN_DELAY       = int(os.getenv("MIN_JOIN_DELAY", 15))
MAX_JOIN_ATTEMPTS    = int(os.getenv("MAX_JOIN_ATTEMPTS", 3))
JOIN_DELAY           = int(os.getenv("JOIN_DELAY", 15))
GROUP_UPDATE_INTERVAL= int(os.getenv("GROUP_UPDATE_INTERVAL", 86400))
