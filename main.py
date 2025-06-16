import asyncio
import logging
import os
import json
import random
from datetime import datetime, timedelta
import re
from typing import Dict, List, Optional, Any

import phonenumbers
from phonenumbers import NumberParseException, PhoneNumberFormat
import aiosqlite
from aiogram.types import MessageEntity
from telethon import TelegramClient, types as telethon_types, events
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.errors import (
    ChatWriteForbiddenError, ChatAdminRequiredError, ChannelPrivateError,
    FloodWaitError, SessionPasswordNeededError, AuthKeyUnregisteredError, BadRequestError
)
from telethon.network.connection.tcpfull import ConnectionTcpFull
from telethon.tl.types import (
    UpdateChannel, PeerChannel, InputDocument, Document, MessageEntityCustomEmoji,
    InputStickerSetID, DocumentAttributeSticker
)
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart
from aiogram.filters.text import Text
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest
from filelock import FileLock

from config import (
    TELEGRAM_BOT_TOKEN,
    ADMIN_ID,
    API_ID,
    API_HASH,
    LOG_CHANNEL_ID,
    STATS_CHANNEL_ID,
    MIN_JOIN_DELAY,
    MAX_JOIN_ATTEMPTS,
    JOIN_DELAY,
    GROUP_UPDATE_INTERVAL
)

def escape_md(text: str) -> str:
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

def extract_text_and_entities(message: types.Message) -> tuple:
    text = message.text or message.caption or ""
    entities = message.entities or message.caption_entities or []
    
    custom_entities = []
    
    for entity in entities:
        if entity.type == "custom_emoji":
            custom_entities.append({
                'type': entity.type,
                'offset': entity.offset,
                'length': entity.length,
                'custom_emoji_id': str(entity.custom_emoji_id)
            })

    return text, custom_entities

SQLITE_PRAGMAS = {
    'journal_mode': 'wal',
    'cache_size': -1 * 10000,
    'synchronous': 'normal',
    'busy_timeout': 30000
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='bot.log'
)
logger = logging.getLogger(__name__)

storage = MemoryStorage()
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher(storage=storage)

SESSION_DIR = "sessions/"
DB_FILE = "data/bot_data.db"
MESSAGE_FILE = "data/messages.json"
os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
os.makedirs(os.path.dirname(MESSAGE_FILE), exist_ok=True)

class Form(StatesGroup):
    enter_phone = State()
    enter_code = State()
    enter_password = State()
    add_group = State()
    set_group_interval = State()
    create_message = State()
    select_category = State()
    edit_message = State()
    set_interval_type = State()
    select_group_for_interval = State()
    select_account_for_interval = State()
    select_account_for_message_interval = State()
    select_message_for_interval = State()
    enter_interval_value = State()
    manage_messages = State()
    manage_categories = State()
    add_category = State()
    delete_category = State()
    link_account_category = State()

class SchedulerState:
    def __init__(self):
        self.running = False
        self.task = None
        self.message_indices = {}  # Сохраняем индексы сообщений

scheduler_state = SchedulerState()

class Database:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.conn = None
        self.lock = asyncio.Lock()

    async def connect(self):
        self.conn = await aiosqlite.connect(self.db_file, timeout=30)
        await self._initialize_db()

    async def _initialize_db(self):
        for pragma, value in SQLITE_PRAGMAS.items():
            await self.conn.execute(f"PRAGMA {pragma}={value}")
        
        await self.conn.execute('''
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY
            )
        ''')

        version = await self._get_schema_version()

        migrations = [
            self._migration_v1,
            self._migration_v2,
            self._migration_v3,
        ]

        for i in range(version + 1, len(migrations) + 1):
            await migrations[i-1]()
            await self._set_schema_version(i)

        await self.conn.commit()

    async def _get_schema_version(self) -> int:
        try:
            result = await self.conn.execute_fetchall('SELECT version FROM schema_version')
            return result[0][0] if result else 0
        except aiosqlite.OperationalError:
            return 0

    async def _set_schema_version(self, version: int):
        await self.conn.execute('DELETE FROM schema_version')
        await self.conn.execute('INSERT INTO schema_version VALUES (?)', (version,))
        await self.conn.commit()

    async def _migration_v1(self):
        await self.conn.executescript('''
            CREATE TABLE IF NOT EXISTS accounts (
                phone TEXT PRIMARY KEY,
                session_file TEXT NOT NULL,
                last_used TEXT,
                password TEXT,
                username TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                message_interval INTEGER DEFAULT 180
            );

            CREATE TABLE IF NOT EXISTS groups (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                username TEXT,
                invite_link TEXT,
                added_at TEXT DEFAULT CURRENT_TIMESTAMP,
                message_interval INTEGER DEFAULT 180,
                category TEXT DEFAULT 'default'
            );

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_phone TEXT NOT NULL,
                message_text TEXT NOT NULL,
                media_type TEXT,
                media_data TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                message_interval INTEGER DEFAULT 10,
                file_reference BLOB
            );

            CREATE TABLE IF NOT EXISTS categories (
                name TEXT PRIMARY KEY
            );
        ''')
        await self.conn.commit()

    async def _migration_v2(self):
        await self.conn.executescript('''
            CREATE TABLE IF NOT EXISTS account_categories (
                account_phone TEXT,
                category_name TEXT,
                PRIMARY KEY (account_phone, category_name)
            );
        ''')
        await self.conn.commit()

    async def _migration_v3(self):
        try:
            await self.conn.execute("SELECT file_reference FROM messages LIMIT 1")
        except aiosqlite.OperationalError:
            await self.conn.execute('''
                ALTER TABLE messages ADD COLUMN file_reference BLOB
            ''')
            cursor = await self.conn.execute('SELECT id, media_data FROM messages')
            async for row in cursor:
                media_data = json.loads(row[1]) if row[1] else {}
                if 'file_reference' in media_data:
                    await self.conn.execute(
                        'UPDATE messages SET file_reference = ? WHERE id = ?',
                        (media_data['file_reference'], row[0]))
            await self.conn.commit()

    async def execute(self, query: str, params: tuple = (), commit: bool = False):
        async with self.lock:
            cursor = await self.conn.execute(query, params)
            if commit:
                await self.conn.commit()
            return cursor

    async def fetchone(self, query: str, params: tuple = ()):   
        async with self.lock:
            cursor = await self.conn.execute(query, params)
            return await cursor.fetchone()

    async def close(self):
        if self.conn:
            await self.conn.close()

class AccountManager:
    def __init__(self, db: Database):
        self.db = db
        self.lock = asyncio.Lock()

    async def add_account(self, phone: str, session_file: str, username: str, password: str = None) -> bool:
        async with self.lock:
            try:
                await self.db.execute(
                    '''INSERT INTO accounts 
                    (phone, session_file, last_used, password, username)
                    VALUES (?, ?, ?, ?, ?)''',
                    (phone, session_file, datetime.now().isoformat(), password, username),
                    commit=True
                )
                return True
            except aiosqlite.IntegrityError:
                logger.error(f"Account {phone} already exists")
                return False
            except Exception as e:
                logger.error(f"Error adding account: {e}")
                return False

    async def get_account(self, phone: str) -> Optional[Dict]:
        async with self.lock:
            cursor = await self.db.execute(
                'SELECT phone, session_file, last_used, password, username, message_interval FROM accounts WHERE phone = ?',
                (phone,))
            row = await cursor.fetchone()
            if row:
                return {
                    'phone': row[0], 
                    'session_file': row[1], 
                    'last_used': row[2],
                    'password': row[3], 
                    'username': row[4],
                    'message_interval': row[5]
                }
            return None

    async def get_all_accounts(self) -> List[Dict]:
        cursor = await self.db.execute('SELECT phone, session_file, last_used, password, username, message_interval FROM accounts')
        rows = await cursor.fetchall()
        return [dict(zip(['phone', 'session_file', 'last_used', 'password', 'username', 'message_interval'], row)) for row in rows]

    async def remove_account(self, phone: str) -> bool:
        try:
            await self.db.execute('DELETE FROM accounts WHERE phone = ?', (phone,), commit=True)
            await self.db.execute('DELETE FROM account_categories WHERE account_phone = ?', (phone,), commit=True)
            await self.db.execute('DELETE FROM messages WHERE account_phone = ?', (phone,), commit=True)  # Добавлено удаление сообщений
            session_file = os.path.join(SESSION_DIR, f"{phone}.session")
            if os.path.exists(session_file):
                os.remove(session_file)
            return True
        except Exception as e:
            logger.error(f"Error removing account: {e}")
            return False

    async def update_account_interval(self, phone: str, interval: int) -> bool:
        try:
            await self.db.execute(
                'UPDATE accounts SET message_interval = ? WHERE phone = ?',
                (interval, phone),
                commit=True
            )
            return True
        except Exception as e:
            logger.error(f"Error updating account interval: {e}")
            return False

    async def get_account_categories(self, phone: str) -> List[str]:
        cursor = await self.db.execute(
            'SELECT category_name FROM account_categories WHERE account_phone = ?',
            (phone,)
        )
        rows = await cursor.fetchall()
        return [row[0] for row in rows]

    async def add_account_category(self, phone: str, category: str) -> bool:
        try:
            await self.db.execute(
                'INSERT INTO account_categories VALUES (?, ?)',
                (phone, category),
                commit=True
            )
            return True
        except Exception as e:
            logger.error(f"Error adding account category: {e}")
            return False

    async def remove_account_category(self, phone: str, category: str) -> bool:
        try:
            await self.db.execute(
                'DELETE FROM account_categories WHERE account_phone = ? AND category_name = ?',
                (phone, category),
                commit=True
            )
            return True
        except Exception as e:
            logger.error(f"Error removing account category: {e}")
            return False

class GroupManager:
    def __init__(self, db: Database):
        self.db = db

    async def add_group(self, group_data: dict) -> bool:
        try:
            await self.db.execute(
                '''INSERT OR REPLACE INTO groups 
                (id, title, username, invite_link, added_at, message_interval, category)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)''',
                (group_data['id'], group_data['title'], group_data.get('username'),
                group_data.get('invite_link'), group_data.get('message_interval', 180),
                group_data.get('category', 'default')),
                commit=True
            )
            return True
        except Exception as e:
            logger.error(f"Error adding group: {e}")
            return False

    async def get_all_groups(self, category: str = None) -> List[dict]:
        query = 'SELECT id, title, username, invite_link, message_interval, category FROM groups'
        params = ()
        if category:
            query += ' WHERE category = ?'
            params = (category,)
        
        cursor = await self.db.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(zip(['id', 'title', 'username', 'invite_link', 'message_interval', 'category'], row)) for row in rows]

    async def remove_group(self, group_id: int) -> bool:
        try:
            await self.db.execute('DELETE FROM groups WHERE id = ?', (group_id,), commit=True)
            return True
        except Exception as e:
            logger.error(f"Error removing group: {e}")
            return False

    async def update_group_interval(self, group_id: int, interval: int) -> bool:
        try:
            await self.db.execute(
                'UPDATE groups SET message_interval = ? WHERE id = ?',
                (interval, group_id),
                commit=True
            )
            return True
        except Exception as e:
            logger.error(f"Error updating group interval: {e}")
            return False

    async def update_group(self, group_id: int, new_data: dict) -> bool:
        try:
            await self.db.execute(
                '''UPDATE groups SET 
                title = ?, 
                username = ?, 
                invite_link = ? 
                WHERE id = ?''',
                (new_data['title'], new_data['username'], 
                 new_data['invite_link'], group_id),
                commit=True
            )
            return True
        except Exception as e:
            logger.error(f"Error updating group: {e}")
            return False

class MessageManager:
    def __init__(self, db):
        self.db = db

    async def add_message(
        self,
        account_phone: str,
        text: str,
        media_type: str = None,
        media_data: dict = None,
        file_reference: bytes = None,
        entities: List[dict] = None
    ) -> bool:
        try:
            full_media_data = media_data or {}
            if entities:
                full_media_data["entities"] = entities

            await self.db.execute(
                '''
                INSERT INTO messages (
                    account_phone, message_text, media_type, media_data, file_reference
                ) VALUES (?, ?, ?, ?, ?)
                ''',
                (
                    account_phone,
                    text,
                    media_type,
                    json.dumps(full_media_data) if full_media_data else None,
                    file_reference
                ),
                commit=True
            )
            return True
        except Exception as e:
            logger.error(f"Error saving message: {e}")
            return False

    async def get_messages(self, account_phone: str = None) -> List[dict]:
        query = 'SELECT id, account_phone, message_text, media_type, media_data, file_reference FROM messages'
        params = ()
        if account_phone:
            query += ' WHERE account_phone = ?'
            params = (account_phone,)

        cursor = await self.db.execute(query, params)
        rows = await cursor.fetchall()

        messages = []
        for row in rows:
            media_data = json.loads(row[4]) if row[4] else {}
            entities = media_data.get("entities", [])
            messages.append({
                'id': row[0],
                'account_phone': row[1],
                'message_text': row[2],
                'media_type': row[3],
                'media_data': media_data,
                'file_reference': row[5],
                'entities': entities
            })

        return messages

    async def get_message(self, message_id: int) -> Optional[dict]:
        cursor = await self.db.execute(
            'SELECT id, account_phone, message_text, media_type, media_data, file_reference FROM messages WHERE id = ?',
            (message_id,)
        )
        row = await cursor.fetchone()
        if row:
            media_data = json.loads(row[4]) if row[4] else {}
            return {
                'id': row[0],
                'account_phone': row[1],
                'message_text': row[2],
                'media_type': row[3],
                'media_data': media_data,
                'file_reference': row[5],
                'entities': media_data.get("entities", [])
            }
        return None

    async def remove_message(self, message_id: int) -> bool:
        try:
            await self.db.execute(
                'DELETE FROM messages WHERE id = ?',
                (message_id,),
                commit=True
            )
            return True
        except Exception as e:
            logger.error(f"Error removing message: {e}")
            return False

    async def update_message(self, message_id: int, new_text: str) -> bool:
        try:
            await self.db.execute(
                'UPDATE messages SET message_text = ? WHERE id = ?',
                (new_text, message_id),
                commit=True
            )
            return True
        except Exception as e:
            logger.error(f"Error updating message: {e}")
            return False

    async def update_message_interval(self, message_id: int, interval: int) -> bool:
        try:
            await self.db.execute(
                'UPDATE messages SET message_interval = ? WHERE id = ?',
                (interval, message_id),
                commit=True
            )
            return True
        except Exception as e:
            logger.error(f"Error updating message interval: {e}")
            return False

class CategoryManager:
    def __init__(self, db: Database):
        self.db = db

    async def add_category(self, name: str) -> bool:
        try:
            await self.db.execute(
                'INSERT INTO categories VALUES (?)',
                (name,),
                commit=True
            )
            return True
        except Exception as e:
            logger.error(f"Error adding category: {e}")
            return False

    async def remove_category(self, name: str) -> bool:
        try:
            await self.db.execute(
                'DELETE FROM categories WHERE name = ?',
                (name,),
                commit=True
            )
            return True
        except Exception as e:
            logger.error(f"Error removing category: {e}")
            return False

    async def get_all_categories(self) -> List[str]:
        cursor = await self.db.execute('SELECT name FROM categories')
        rows = await cursor.fetchall()
        return [row[0] for row in rows]

class PersistentClient:
    def __init__(self, phone: str, session_file: str):
        self.phone = phone
        self.session_file = session_file
        self.client: TelegramClient = None
        self.lock = asyncio.Lock()
        self._initialize_client()

    def _initialize_client(self):
        self.client = TelegramClient(
            session=self.session_file,
            api_id=API_ID,
            api_hash=API_HASH,
            connection=ConnectionTcpFull,
            auto_reconnect=True,
            system_version='4.16.30-vxCUSTOM'
        )
        self.client.add_event_handler(self.handle_sticker, events.NewMessage(func=lambda e: e.sticker))

    async def handle_sticker(self, event):
        try:
            sticker = event.message.sticker
            if not sticker.premium_animation:
                return

            doc = await self.client.get_document(sticker.document.id)
            media_data = {
                'id': doc.id,
                'access_hash': doc.access_hash,
                'file_reference': doc.file_reference,
                'dc_id': doc.dc_id,
                'mime_type': doc.mime_type,
                'attributes': [str(a) for a in doc.attributes]
            }

            await message_manager.add_message(
                account_phone=self.phone,
                text="",
                media_type="sticker",
                media_data=media_data,
                file_reference=doc.file_reference,
                entities=[]
            )
            logger.info(f"Saved premium sticker: {doc.id}")
        except Exception as e:
            logger.error(f"Error handling sticker: {e}")

    async def connect(self):
        async with self.lock:
            if not self.client.is_connected():
                await self.client.connect()
            if not await self.client.is_user_authorized():
                await self.full_reauthorization()

    async def full_reauthorization(self):
        await self.client.disconnect()
        lock = FileLock(self.session_file + ".lock")
        with lock:
            if os.path.exists(self.session_file): 
                os.remove(self.session_file)
        self._initialize_client()
        await self.client.connect()
        await self.client.send_code_request(self.phone)
        code = await ask_for_code(self.phone)
        await self.client.sign_in(self.phone, code)

    async def ensure_connection(self) -> bool:
        try:
            if not self.client.is_connected(): 
                await self.connect()
            if not await self.client.is_user_authorized(): 
                await self.full_reauthorization()
            return True
        except Exception as e:
            logger.error(f"Connection error: {e}")
            return False

    async def join_group(self, group_info: dict):
        try:
            if group_info.get('username'):
                await self.client(JoinChannelRequest(group_info['username']))
            elif group_info.get('invite_link'):
                link = group_info['invite_link']
                if 'joinchat' in link:
                    inv = link.rstrip('/').rsplit('/',1)[-1]
                    await self.client(ImportChatInviteRequest(inv))
                else:
                    uname = link.rstrip('/').split('/')[-1]
                    await self.client(JoinChannelRequest(uname))
            await asyncio.sleep(JOIN_DELAY)
            return await self.client.get_entity(group_info.get('username') or group_info.get('invite_link'))
        except Exception as e:
            logger.error(f"Failed to join group {group_info.get('title')}: {e}")
            return None

    async def update_group_info(self, group_id: int, old_link: Optional[str]) -> dict:
        if not await self.ensure_connection(): 
            return {}
        try:
            full = await self.client(GetFullChannelRequest(group_id))
            chat = full.chats[0]
            exported = getattr(full.full_chat, 'exported_invite', None)
            invite = exported.link if exported else old_link
            return {'title': chat.title, 'username': getattr(chat,'username',None), 'invite_link': invite}
        except Exception as e:
            logger.error(f"Error updating info for {group_id}: {e}")
            return {}

class SessionManager:
    _instance = None
    clients: Dict[str, PersistentClient] = {}

    def __new__(cls):
        if cls._instance is None: 
            cls._instance = super().__new__(cls)
        return cls._instance

    async def get_client(self, phone: str) -> PersistentClient:
        if phone not in self.clients:
            session_file = os.path.join(SESSION_DIR, f"{phone}.session")
            self.clients[phone] = PersistentClient(phone, session_file)
        return self.clients[phone]

    def remove_client(self, phone: str):
        if phone in self.clients:
            del self.clients[phone]

async def ask_for_code(phone: str) -> str:
    return "11111"

@retry(wait=wait_exponential(multiplier=1, max=60),
       stop=stop_after_attempt(MAX_JOIN_ATTEMPTS),
       retry=(retry_if_exception_type(FloodWaitError)|retry_if_exception_type(ConnectionError)),
       reraise=True)
async def send_message_to_group(account: dict, group: dict, message: dict) -> bool:
    client = await SessionManager().get_client(account['phone'])
    await client.ensure_connection()

    try:
        entity = await client.client.get_entity(group['id'])
    except (ValueError, ChannelPrivateError):
        ent = await client.join_group(group)
        if not ent: 
            return False
        entity = ent

    try:
        entities = []
        for ent in message.get('entities', []):
          if ent['type'] == 'custom_emoji':
             try:
                entities.append(MessageEntityCustomEmoji(
                offset=ent['offset'],
                length=ent['length'],
                document_id=int(ent['custom_emoji_id'])  # это и есть ID emoji
            ))
             except Exception as e:
              logger.error(f"Error creating custom emoji entity: {e}")
              continue


        if message.get('media_type') == "sticker":
            media_data = message.get('media_data')
            try:
                sticker = await client.client.get_document(int(ent['custom_emoji_id']))
                entities.append(MessageEntityCustomEmoji(offset=ent['offset'],length=ent['length'],document_id=sticker.id))
                file_reference = sticker.file_reference
            except Exception as e:
                logger.error(f"Error updating sticker reference: {e}")
                file_reference = message.get('file_reference') or b''

            input_doc = InputDocument(
                id=int(media_data['id']),
                access_hash=int(media_data['access_hash']),
                file_reference=file_reference,
                dc_id=int(media_data.get('dc_id', 0)))
            
            await client.client.send_file(
                entity,
                input_doc,
                attributes=[DocumentAttributeSticker()]
            )
            
            if message['message_text']:
                await client.client.send_message(
                    entity, 
                    message['message_text'],
                    formatting_entities=entities
                )
        else:
            await client.client.send_message(
                entity, 
                message['message_text'],
                formatting_entities=entities
            )
        
        return True
    except Exception as e:
        logger.error(f"Error sending message: {e}")
        return False

async def join_all_groups():
    accounts = await account_manager.get_all_accounts()
    groups = await group_manager.get_all_groups()
    for account in accounts:
        client = await SessionManager().get_client(account['phone'])
        await client.ensure_connection()
        for group in groups:
            await client.join_group(group)

async def group_updater_task():
    while True:
        try:
            accounts = await account_manager.get_all_accounts()
            if accounts:
                client = await SessionManager().get_client(accounts[0]['phone'])
                await client.ensure_connection()
                groups = await group_manager.get_all_groups()
                for g in groups:
                    info = await client.update_group_info(g['id'], g.get('invite_link'))
                    if info:
                        await group_manager.update_group(g['id'], info)
            await asyncio.sleep(GROUP_UPDATE_INTERVAL)
        except Exception as e:
            logger.error(f"Group updater error: {e}")
            await asyncio.sleep(60)

async def message_scheduler():
    scheduler_state.running = True
    await join_all_groups()

    while scheduler_state.running:
        try:
            accounts = await account_manager.get_all_accounts()
            if not accounts:
                await asyncio.sleep(10)
                continue

            all_messages = {}

            for acc in accounts:
                messages = await message_manager.get_messages(acc['phone'])
                if messages:
                    all_messages[acc['phone']] = messages
                else:
                    logger.warning(f"No messages for account {acc['phone']}")

            random.shuffle(accounts)
            
            for account in accounts:
                if not scheduler_state.running:
                    return

                phone = account['phone']
                messages = all_messages.get(phone)
                if not messages:
                    continue

                if phone not in scheduler_state.message_indices:
                    scheduler_state.message_indices[phone] = 0  # Инициализация индекса

                categories = await account_manager.get_account_categories(phone)
                groups = []
                for category in categories:
                    category_groups = await group_manager.get_all_groups(category)  # Исправлено получение групп по категории
                    groups.extend(category_groups)

                if not groups:
                    logger.warning(f"No groups for account {phone}")
                    continue

                random.shuffle(groups)

                # Получаем текущее сообщение для аккаунта
                msg_index = scheduler_state.message_indices[phone] % len(messages)
                message = messages[msg_index]

                # Отправляем текущее сообщение во все группы
                for group in groups:
                    if not scheduler_state.running:
                        return

                    success = await send_message_to_group(account, group, message)

                    interval = message.get('message_interval', 
                                      group.get('message_interval', 
                                      account.get('message_interval', 10)))

                    jitter = random.uniform(0.8, 1.2)
                    delay = interval * 60 * jitter

                    if success:
                        logger.info(f"Sent message {msg_index} from {phone} to {group['title']}. Next in {delay/60:.1f}min")
                        await asyncio.sleep(delay)
                    else:
                        await asyncio.sleep(delay * 2)

                # Увеличиваем индекс после отправки во все группы
                scheduler_state.message_indices[phone] += 1

        except Exception as e:
            logger.error(f"Scheduler error: {e}")
            await asyncio.sleep(60)

def main_menu() -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="📱 Аккаунты", callback_data="accounts"),
         types.InlineKeyboardButton(text="💬 Чаты", callback_data="groups")],
        [types.InlineKeyboardButton(text="📨 Рассылка", callback_data="mailing"),
         types.InlineKeyboardButton(text="🗂️ Категории", callback_data="manage_categories")]
    ])

@dp.message(Command("start"))
async def start_command(message: types.Message):
    if message.from_user.id == ADMIN_ID:
        await message.answer(
            "🤖 *Добро пожаловать в Telegram Account Manager\!*"
            "\n\nВыберите раздел управления:",
            reply_markup=main_menu(),
            parse_mode="MarkdownV2"
        )
    else:
        await message.answer("⛔ Доступ запрещен!")

@dp.callback_query(F.data == "accounts")
async def accounts_menu(callback: types.CallbackQuery):
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="➕ Добавить аккаунт", callback_data="add_account")],
        [types.InlineKeyboardButton(text="📋 Список аккаунтов", callback_data="list_accounts")],
        [types.InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]
    ])
    await callback.message.edit_text(
        "📱 *Управление аккаунтами:*",
        reply_markup=keyboard,
        parse_mode="MarkdownV2"
    )

@dp.callback_query(F.data == "add_account")
async def add_account_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(Form.enter_phone)
    await callback.message.answer(
        "📱 Введите номер телефона \(с кодом страны\):",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]]
        ),
        parse_mode="MarkdownV2"
    )

@dp.callback_query(F.data == "cancel")
async def cancel_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        "❌ Действие отменено",
        reply_markup=main_menu()
    )

@dp.message(Form.enter_phone)
async def process_phone(message: types.Message, state: FSMContext):
    raw_phone = message.text.strip()
    
    try:
        # Определяем регион для номеров без кода страны
        region = 'RU' if raw_phone.startswith(('8', '7')) and not raw_phone.startswith('+') else None
        parsed = phonenumbers.parse(raw_phone, region)
        
        if not phonenumbers.is_valid_number(parsed):
            raise ValueError
        
        # Форматируем в международный формат
        phone = phonenumbers.format_number(parsed, PhoneNumberFormat.E164)
        
    except (NumberParseException, ValueError) as e:
        await message.answer(
            "❌ Неверный формат номера!\n"
            "Введите номер в одном из форматов:\n"
            "+79123456789\n79123456789\n89123456789",
            reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]]
            )
        )
        return

    session_file = os.path.join(SESSION_DIR, f"{phone}.session")
    
    try:
        client = TelegramClient(
            session=session_file,
            api_id=API_ID,
            api_hash=API_HASH,
            system_version='4.16.30-vxCUSTOM',
            device_model='Redmi Note 9S',
            app_version='8.9.3'
        )
        
        await client.connect()
        
        # Проверяем существование сессии
        if await client.is_user_authorized():
            await message.answer("⚠️ Этот аккаунт уже авторизован!")
            await client.disconnect()
            return
            
        await client.send_code_request(phone)
        
        await state.update_data(phone=phone, client=client)
        await message.answer(
            "🔢 Код отправлен. Введите полученный код:",
            reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]]
            )
        )
        await state.set_state(Form.enter_code)
        
    except FloodWaitError as e:
        await message.answer(f"⌛ Telegram требует подождать {e.seconds} секунд перед повторной попыткой")
    except Exception as e:
        logger.error(f"Auth error: {str(e)}")
        await message.answer(
            f"❌ Ошибка авторизации: {str(e)}\n"
            "Проверьте правильность номера и повторите попытку"
        )
        await state.clear()

# Добавить этот обработчик в раздел callback handlers
@dp.callback_query(F.data == "mailing")
async def mailing_menu(callback: types.CallbackQuery):
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(
            text="🟢 Запустить рассылку" if not scheduler_state.running else "🔴 Остановить рассылку",
            callback_data="toggle_mailing"
        )],
        [types.InlineKeyboardButton(text="⚙️ Настройки интервалов", callback_data="interval_settings")],
        [types.InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]
    ])
    
    status = "активна" if scheduler_state.running else "остановлена"
    text = f"📨 *Управление рассылкой*\n\nТекущий статус: {escape_md(status)}"
    
    await callback.message.edit_text(
        text,
        reply_markup=keyboard,
        parse_mode="MarkdownV2"
    )

@dp.callback_query(F.data == "toggle_mailing")
async def toggle_mailing(callback: types.CallbackQuery):
    if scheduler_state.running:
        scheduler_state.running = False
        if scheduler_state.task:
            scheduler_state.task.cancel()
        await callback.answer("🔴 Рассылка остановлена")
    else:
        scheduler_state.task = asyncio.create_task(message_scheduler())
        await callback.answer("🟢 Рассылка запущена")
    
    await mailing_menu(callback)

@dp.callback_query(F.data == "interval_settings")
async def interval_settings(callback: types.CallbackQuery):
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⏱️ Для аккаунтов", callback_data="set_account_interval")],
        [types.InlineKeyboardButton(text="⏱️ Для групп", callback_data="set_group_interval_menu")],
        [types.InlineKeyboardButton(text="⏱️ Для сообщений", callback_data="set_message_interval")],
        [types.InlineKeyboardButton(text="🔙 Назад", callback_data="mailing")]
    ])
    
    await callback.message.edit_text(
        "⚙️ *Настройка интервалов*\n\nВыберите тип интервала для настройки:",
        reply_markup=keyboard,
        parse_mode="MarkdownV2"
    )

@dp.callback_query(F.data == "set_group_interval_menu")
async def set_group_interval_menu(callback: types.CallbackQuery):
    categories = await category_manager.get_all_categories()
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=cat, callback_data=f"set_group_interval_cat_{cat}")]
        for cat in categories
    ] + [[types.InlineKeyboardButton(text="🔙 Назад", callback_data="interval_settings")]])
    
    await callback.message.edit_text(
        "📋 Выберите категорию групп для настройки интервала:",
        reply_markup=keyboard
    )

@dp.callback_query(F.data.startswith("set_group_interval_cat_"))
async def set_group_interval_category(callback: types.CallbackQuery, state: FSMContext):
    category = callback.data.split("_")[4]
    groups = await group_manager.get_all_groups(category)
    
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=group['title'], callback_data=f"set_group_interval_{group['id']}")]
        for group in groups
    ] + [[types.InlineKeyboardButton(text="🔙 Назад", callback_data="interval_settings")]])
    
    await callback.message.edit_text(
        f"Выберите группу из категории '{category}':",
        reply_markup=keyboard
    )

@dp.callback_query(F.data.startswith("set_group_interval_"))
async def set_group_interval_handler(callback: types.CallbackQuery, state: FSMContext):
    group_id = int(callback.data.split("_")[3])
    await state.update_data(interval_type="group", group_id=group_id)
    await callback.message.answer("Введите новый интервал в минутах:")
    await state.set_state(Form.enter_interval_value)

@dp.callback_query(F.data == "set_account_interval")
async def set_account_interval_menu(callback: types.CallbackQuery):
    accounts = await account_manager.get_all_accounts()
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=acc.get('username', acc['phone']), callback_data=f"set_acc_interval_{acc['phone']}")]
        for acc in accounts
    ] + [[types.InlineKeyboardButton(text="🔙 Назад", callback_data="interval_settings")]])
    
    await callback.message.edit_text(
        "Выберите аккаунт для настройки интервала:",
        reply_markup=keyboard
    )

@dp.callback_query(F.data.startswith("set_acc_interval_"))
async def select_account_interval_handler(callback: types.CallbackQuery, state: FSMContext):
    phone = callback.data.split("_")[3]
    await state.update_data(interval_type="account", account_phone=phone)
    await callback.message.answer("Введите новый интервал в минутах:")
    await state.set_state(Form.enter_interval_value)

@dp.callback_query(F.data == "set_message_interval")
async def set_message_interval_menu(callback: types.CallbackQuery):
    accounts = await account_manager.get_all_accounts()
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(
            text=f"@{acc['username']}" if acc['username'] else f"Аккаунт {acc['phone']}", 
            callback_data=f"set_msg_acc_interval_{acc['phone']}")]
        for acc in accounts
    ] + [[types.InlineKeyboardButton(text="🔙 Назад", callback_data="interval_settings")]])
    
    await callback.message.edit_text(
        "Выберите аккаунт для настройки интервала сообщений:",
        reply_markup=keyboard
    )

@dp.callback_query(F.data.startswith("set_msg_acc_interval_"))
async def select_account_for_message_interval(callback: types.CallbackQuery, state: FSMContext):
    phone = callback.data.split("_")[4]
    await state.update_data(account_phone=phone)
    messages = await message_manager.get_messages(phone)
    
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(
            text=msg['message_text'][:30] or "Стикер", 
            callback_data=f"select_message_interval_{msg['id']}")]
        for msg in messages
    ] + [[types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]])
    
    await callback.message.answer("Выберите сообщение:", reply_markup=keyboard)
    await state.set_state(Form.select_message_for_interval)

@dp.message(Form.enter_code)
async def process_code(message: types.Message, state: FSMContext):
    code = message.text.strip()
    data = await state.get_data()
    phone = data['phone']
    client = data['client']
    
    try:
        await client.sign_in(phone, code)
        user = await client.get_me()
        username = user.username or ""
        
        session_file = os.path.join(SESSION_DIR, f"{phone}.session")
        success = await account_manager.add_account(
            phone=phone,
            session_file=session_file,
            username=username,
            password=data.get('password')
        )
        if success:
            await message.answer("✅ Аккаунт успешно добавлен!", reply_markup=main_menu())
        else:
            await message.answer("❌ Ошибка добавления аккаунта")
        await state.clear()
    except SessionPasswordNeededError:
        await message.answer("🔐 Введите пароль двухфакторной аутентификации:")
        await state.set_state(Form.enter_password)
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}")
        await state.clear()

@dp.message(Form.enter_password)
async def process_password(message: types.Message, state: FSMContext):
    password = message.text.strip()
    data = await state.get_data()
    client = data['client']
    
    try:
        await client.sign_in(password=password)
        user = await client.get_me()
        username = user.username or ""
        
        session_file = os.path.join(SESSION_DIR, f"{data['phone']}.session")
        success = await account_manager.add_account(
            phone=data['phone'],
            session_file=session_file,
            username=username,
            password=password
        )
        if success:
            await message.answer("✅ Аккаунт успешно добавлен!", reply_markup=main_menu())
        else:
            await message.answer("❌ Ошибка добавления аккаунта")
        await state.clear()
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}")
        await state.clear()

@dp.callback_query(F.data == "list_accounts")
async def list_accounts_handler(callback: types.CallbackQuery):
    try:
        accounts = await account_manager.get_all_accounts()
        if not accounts:
            await callback.message.edit_text(
                "📭 Список аккаунтов пуст\!",
                reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text="🔙 Назад", callback_data="accounts")]]),
                parse_mode="MarkdownV2"
            )
            return
        
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text=f"@{acc['username']}" if acc['username'] else f"Аккаунт {acc['phone']}", callback_data=f"account_{acc['phone']}")]
            for acc in accounts
        ] + [[types.InlineKeyboardButton(text="🔙 Назад", callback_data="accounts")]])
        
        await callback.message.edit_text(
            "📋 Список аккаунтов:",
            reply_markup=keyboard
        )
    except Exception as e:
        logger.error(f"Error in list_accounts_handler: {str(e)}")
        await callback.answer("❌ Произошла ошибка при загрузке аккаунтов")

@dp.callback_query(F.data.startswith("account_"))
async def account_detail(callback: types.CallbackQuery):
    phone = callback.data.split("_")[1]
    account = await account_manager.get_account(phone)
    
    if not account:
        await callback.answer("⚠️ Аккаунт не найден!")
        try:
            await callback.message.delete()
        except Exception as e:
            logger.warning(f"Error deleting message: {e}")
        try:
            await list_accounts_handler(callback)
        except TelegramBadRequest as e:
            if "message to edit not found" in str(e):
                await callback.message.answer("📭 Список аккаунтов пуст!", reply_markup=main_menu())
        return
    
    try:
        msgs = await message_manager.get_messages(phone)
        
        last_used = account.get('last_used') or "Никогда"
        interval = account.get('message_interval', 180)
        username = account.get('username') or "Нет username"
        
        text = (
            f"📱 *Детали аккаунта:*\n\n"
            f"• *Телефон:* `{escape_md(phone)}`\n"
            f"• *Username:* @{escape_md(username)}\n"
            f"• *Последняя активность:* {escape_md(last_used)}\n"
            f"• *Интервал сообщений:* {escape_md(str(interval))} мин\n"
            f"• *Всего сообщений для рассылки:* {len(msgs)}"
        )
        
        keyboard_buttons = [
            [types.InlineKeyboardButton(text="📝 Сообщения", callback_data=f"posts_{phone}"),
             types.InlineKeyboardButton(text="✏️ Управление сообщениями", callback_data=f"manage_messages_{phone}")],
            [types.InlineKeyboardButton(text="🗑️ Удалить аккаунт", callback_data=f"confirm_delete_account_{phone}")]
        ]
        
        keyboard_buttons.append([types.InlineKeyboardButton(text="🔙 Назад", callback_data="list_accounts")])
        
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
        
        await callback.message.edit_text(
            text,
            reply_markup=keyboard,
            parse_mode="MarkdownV2"
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            await callback.answer("⚠️ Сообщение не было изменено")
    except Exception as e:
        logger.error(f"Error in account_detail: {e}")
        await callback.answer("❌ Произошла ошибка при обработке запроса")

@dp.callback_query(F.data.startswith("posts_"))
async def account_posts(callback: types.CallbackQuery):
    phone = callback.data.split("_")[1]
    messages = await message_manager.get_messages(phone)
    
    if not messages:
        await callback.message.edit_text(
            "📭 Нет сообщений для этого аккаунта\!",
            reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="🔙 Назад", callback_data=f"account_{phone}")]]
            ),
            parse_mode="MarkdownV2"
        )
        return
    
    text = "📝 *Сообщения аккаунта:*\n\n"
    for i, msg in enumerate(messages):
        media_info = ""
        if msg.get('media_type') == "sticker":
            media_info = "\n🎁 Прикреплен премиум-стикер"
        
        text += (
            f"📌 *Сообщение {i+1}:*\n"
            f"Символов: {len(msg['message_text'])}\n"
            f"Интервал: {msg.get('message_interval', 10)} мин{media_info}\n"
            f"Текст:\n{escape_md(msg['message_text'])}\n\n"
        )
    
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=f"✏️ Редактировать {i+1}", callback_data=f"edit_msg_{msg['id']}"),
         types.InlineKeyboardButton(text=f"🗑️ Удалить {i+1}", callback_data=f"del_msg_{msg['id']}")]
        for i, msg in enumerate(messages)
    ] + [
        [types.InlineKeyboardButton(text="🔙 Назад", callback_data=f"account_{phone}")]
    ])
    
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="MarkdownV2")

@dp.callback_query(F.data.startswith("edit_msg_"))
async def edit_message_handler(callback: types.CallbackQuery, state: FSMContext):
    msg_id = int(callback.data.split("_")[2])
    await state.update_data(msg_id=msg_id)
    await state.set_state(Form.edit_message)
    await callback.message.answer(
        "Введите новый текст сообщения:",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]]
        )
    )

@dp.message(Form.edit_message)
async def process_edit_message(message: types.Message, state: FSMContext):
    data = await state.get_data()
    msg_id = data['msg_id']
    if await message_manager.update_message(msg_id, message.text):
        await message.answer("✅ Сообщение обновлено!", reply_markup=main_menu())
    else:
        await message.answer("❌ Ошибка обновления сообщения!")
    await state.clear()

@dp.callback_query(F.data.startswith("del_msg_"))
async def delete_message_handler(callback: types.CallbackQuery):
    msg_id = int(callback.data.split("_")[2])
    if await message_manager.remove_message(msg_id):
        await callback.answer("✅ Сообщение удалено!")
        # Обновляем список сообщений
        phone = callback.data.split("_")[1] if "_" in callback.data else None
        if phone:
            await account_posts(callback)
        else:
            await callback.message.delete()
    else:
        await callback.answer("❌ Ошибка удаления сообщения!")

@dp.callback_query(F.data.startswith("confirm_delete_account_"))
async def confirm_delete_account(callback: types.CallbackQuery):
    phone = callback.data.split("_")[3]
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="✅ Да", callback_data=f"delete_account_{phone}"),
         types.InlineKeyboardButton(text="❌ Нет", callback_data="cancel")]
    ])
    await callback.message.edit_text(
        f"❓ Вы уверены, что хотите удалить аккаунт `{escape_md(phone)}`\?",
        reply_markup=keyboard,
        parse_mode="MarkdownV2"
    )

@dp.callback_query(F.data.startswith("delete_account_"))
async def delete_account(callback: types.CallbackQuery):
    phone = callback.data.split("_")[2]
    await account_manager.remove_account(phone)
    SessionManager().remove_client(phone)
    await callback.answer("✅ Аккаунт удален!")
    await list_accounts_handler(callback)

@dp.callback_query(F.data.startswith("manage_messages_"))
async def manage_messages_handler(callback: types.CallbackQuery, state: FSMContext):
    try:
        phone = callback.data.split("_")[2]
        await state.update_data(account_phone=phone)
        
        messages = await message_manager.get_messages(phone)
        
        # Формируем текст с правильным экранированием
        text = "📝 *Сообщения для аккаунта:*\n\n"
        for i, msg in enumerate(messages):
            escaped_text = escape_md(msg['message_text'][:50])
            text += f"{i+1}\\. {escaped_text}"
            if len(msg['message_text']) > 50:
                text += "\\.\\.\\."
            text += "\n"

        # Создаем клавиатуру
        keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
            [
                types.InlineKeyboardButton(
                    text="➕ Добавить сообщение",
                    callback_data="add_message"
                ),
                types.InlineKeyboardButton(
                    text="🗑️ Удалить сообщение",
                    callback_data="delete_message"
                )
            ],
            [
                types.InlineKeyboardButton(
                    text="🔙 Назад",
                    callback_data=f"account_{phone}"
                )
            ]
        ])

        # Пытаемся обновить сообщение
        await callback.message.edit_text(
            text,
            reply_markup=keyboard,
            parse_mode="MarkdownV2"
        )
        
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            await callback.answer("⚠️ Сообщение не было изменено")
    except Exception as e:
        logger.error(f"Ошибка в manage_messages_handler: {str(e)}")
        await callback.answer("❌ Произошла ошибка при загрузке сообщений")

@dp.callback_query(F.data == "add_message")
async def add_message_handler(callback: types.CallbackQuery, state: FSMContext):
    try:
        data = await state.get_data()
        phone = data.get('account_phone')
        
        # Отправляем новое сообщение вместо редактирования
        await callback.message.answer(
            "📝 Отправьте текст сообщения и/или премиум-стикер:",
            reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="❌ Отмена", callback_data=f"manage_messages_{phone}")]]
            )
        )
        await state.set_state(Form.create_message)
        
    except Exception as e:
        logger.error(f"Ошибка в add_message_handler: {str(e)}")
        await callback.answer("❌ Ошибка при создании сообщения")

@dp.message(Form.create_message)
async def process_create_message(message: types.Message, state: FSMContext):
    try:
        data = await state.get_data()
        phone = data.get('account_phone')
        
        media_type = None
        media_data = None
        file_reference = None
        text, entities = extract_text_and_entities(message)
        
        if message.sticker:
            sticker = message.sticker
            if not sticker.is_premium:
                await message.answer("❌ Это не премиум-стикер!")
                return
            
            document: Document = sticker.document
            media_type = "sticker"
            media_data = {
                "id": document.id,
                "access_hash": document.access_hash,
                "dc_id": document.dc_id,
                "mime_type": document.mime_type,
                "attributes": [str(attr) for attr in document.attributes]
            }
            file_reference = document.file_reference

        success = await message_manager.add_message(
            phone,
            text,
            media_type,
            media_data,
            file_reference,
            entities
        )
        
        if success:
            await message.answer("✅ Сообщение добавлено!", reply_markup=main_menu())
        else:
            await message.answer("❌ Ошибка сохранения сообщения!")
        await state.clear()
    except Exception as e:
        logger.error(f"Error creating message: {e}")
        await message.answer("❌ Произошла ошибка при создании сообщения!")
        await state.clear()

@dp.callback_query(F.data == "delete_message")
async def delete_message_handler(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    phone = data.get('account_phone')
    messages = await message_manager.get_messages(phone)
    
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=f"{i+1}. {msg['message_text'][:30]}...", callback_data=f"delete_msg_{msg['id']}")]
        for i, msg in enumerate(messages)
    ] + [
        [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])
    
    await callback.message.answer(
        "Выберите сообщение для удаления:",
        reply_markup=keyboard
    )

@dp.callback_query(F.data == "groups")
async def groups_menu(callback: types.CallbackQuery):
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="➕ Добавить группу", callback_data="add_group")],
        [types.InlineKeyboardButton(text="📋 Список групп", callback_data="list_groups")],
        [types.InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]
    ])
    await callback.message.edit_text(
        "💬 *Управление группами:*",
        reply_markup=keyboard,
        parse_mode="MarkdownV2"
    )

@dp.callback_query(F.data == "add_group")
async def add_group_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(Form.add_group)
    await state.update_data(groups=[])
    await callback.message.answer(
        "📩 Присылайте ссылки на группы или пересылайте сообщения из чатов (по одному или несколько через запятую):\n"
        "Когда закончите - нажмите кнопку 'Готово'",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="✅ Готово", callback_data="finish_adding_groups")],
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]]
        )
    )

@dp.message(Form.add_group)
async def process_add_group(message: types.Message, state: FSMContext):
    data = await state.get_data()
    groups = data.get('groups', [])
    
    if message.forward_from_chat:
        chat = message.forward_from_chat
        groups.append({
            'id': chat.id,
            'title': chat.title,
            'username': getattr(chat, 'username', None),
            'invite_link': getattr(chat, 'invite_link', None)
        })
    elif message.text:
        links = [link.strip() for link in message.text.split(',')]
        for link in links:
            groups.append({'invite_link': link})
    
    await state.update_data(groups=groups)
    await message.answer(f"✅ Получено {len(groups)} групп. Продолжайте присылать ссылки или нажмите 'Готово'")

@dp.callback_query(F.data == "finish_adding_groups")
async def finish_adding_groups(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    groups = data.get('groups', [])
    
    if not groups:
        await callback.answer("❌ Нет групп для добавления!")
        return
    
    accounts = await account_manager.get_all_accounts()
    if not accounts:
        await callback.answer("❌ Нет доступных аккаунтов!")
        return
    
    categories = await category_manager.get_all_categories()
    if not categories:
        await category_manager.add_category('default')
        categories = ['default']
    
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=cat, callback_data=f"setcategory_{cat}")] 
        for cat in categories
    ] + [[types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]])
    
    await state.set_state(Form.select_category)
    await callback.message.answer(
        "Выберите категорию для добавленных групп:",
        reply_markup=keyboard
    )

@dp.callback_query(F.data.startswith("setcategory_"))
async def process_setcategory(callback: types.CallbackQuery, state: FSMContext):
    category = callback.data.split("_")[1]
    data = await state.get_data()
    groups = data.get('groups', [])
    
    account = (await account_manager.get_all_accounts())[0]
    client = await SessionManager().get_client(account['phone'])
    await client.connect()
    
    success_count = 0
    for group in groups:
        try:
            if 'id' in group:
                group_data = {
                    'id': group['id'],
                    'title': group['title'],
                    'username': group['username'],
                    'invite_link': group['invite_link'],
                    'category': category
                }
            else:
                link = group['invite_link']
                if link.startswith("https://t.me/+"):
                    invite_hash = link.split('/')[-1]
                    await client.client(ImportChatInviteRequest(invite_hash))
                    await asyncio.sleep(2)
                    updates = await client.client.get_updates(limit=10)
                    for update in updates:
                        if isinstance(update, UpdateChannel):
                            channel = update.channel_id
                            entity = await client.client.get_entity(PeerChannel(channel))
                            full_info = await client.client(GetFullChannelRequest(entity))
                            group_data = {
                                'id': entity.id,
                                'title': entity.title,
                                'invite_link': full_info.full_chat.exported_invite.link if full_info.full_chat.exported_invite else link,
                                'category': category
                            }
                            break
                else:
                    username = link.split('/')[-1]
                    entity = await client.client.get_entity(username)
                    full_info = await client.client(GetFullChannelRequest(entity))
                    group_data = {
                        'id': entity.id,
                        'title': entity.title,
                        'username': entity.username,
                        'invite_link': full_info.full_chat.exported_invite.link if full_info.full_chat.exported_invite else f"https://t.me/{entity.username}",
                        'category': category
                    }
            
            if await group_manager.add_group(group_data):
                success_count += 1
        except Exception as e:
            logger.error(f"Ошибка добавления группы {group}: {str(e)}")
    
    await callback.message.answer(
        f"✅ Успешно добавлено {success_count}/{len(groups)} групп в категорию '{category}'!",
        reply_markup=main_menu()
    )
    await state.clear()

@dp.callback_query(F.data == "list_groups")
async def list_groups_handler(callback: types.CallbackQuery):
    categories = await category_manager.get_all_categories()
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=cat, callback_data=f"category_{cat}")] for cat in categories
    ] + [[types.InlineKeyboardButton(text="🔙 Назад", callback_data="groups")]]
    )
    await callback.message.edit_text(
        "📋 Выберите категорию групп:",
        reply_markup=keyboard
    )

@dp.callback_query(F.data.startswith("category_"))
async def show_category_groups(callback: types.CallbackQuery):
    try:
        category = callback.data.split("_")[1]
        groups = await group_manager.get_all_groups(category)
        
        if not groups:
            await callback.message.edit_text(
                f"📭 Список групп в категории '{category}' пуст\!",
                reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
                    [types.InlineKeyboardButton(text="🔙 Назад", callback_data="list_groups")]]
                ),
                parse_mode="MarkdownV2"
            )
            return
        
        keyboard = types.InlineKeyboardMarkup(
            inline_keyboard=[
                [types.InlineKeyboardButton(text=f"💬 {group['title']}", callback_data=f"group_{group['id']}")]
                for group in groups
            ] + [
                [types.InlineKeyboardButton(text="🔙 Назад", callback_data="list_groups")]
            ]
        )
        
        await callback.message.edit_text(
            f"📋 Список групп \(*{category}*\):",
            reply_markup=keyboard,
            parse_mode="MarkdownV2"
        )
    except Exception as e:
        logger.error(f"Error showing category groups: {e}")
        await callback.answer("❌ Ошибка при загрузке групп!")

@dp.callback_query(F.data.startswith("group_"))
async def group_detail(callback: types.CallbackQuery):
    group_id = int(callback.data.split("_")[1])
    group = next((g for g in await group_manager.get_all_groups() if g['id'] == group_id), None)
    if not group:
        await callback.answer("❌ Группа не найдена!")
        return
    
    text = (
        f"💬 *Информация о группе:*\n\n"
        f"• *Название:* {escape_md(group['title'])}\n"
        f"• *Категория:* {escape_md(group['category'])}\n"
        f"• *ID:* `{group_id}`\n"
        f"• *Интервал:* {escape_md(str(group['message_interval']))} мин\n"
        f"• *Ссылка:* {escape_md(group['invite_link'] or 'Нет ссылки')}"
    )
    
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🔄 Изменить интервал", callback_data=f"set_interval_{group_id}")],
        [types.InlineKeyboardButton(text="🗑️ Удалить группу", callback_data=f"confirm_delete_group_{group_id}")],
        [types.InlineKeyboardButton(text="🔙 Назад", callback_data="list_groups")]
    ])
    
    await callback.message.edit_text(
        text,
        reply_markup=keyboard,
        parse_mode="MarkdownV2"
    )

@dp.callback_query(F.data.startswith("set_interval_"))
async def set_group_interval(callback: types.CallbackQuery, state: FSMContext):
    group_id = int(callback.data.split("_")[2])
    await state.update_data(group_id=group_id)
    await callback.message.answer("Введите новый интервал в минутах:")
    await state.set_state(Form.enter_interval_value)

@dp.callback_query(F.data.startswith("select_account_interval_"))
async def select_account_interval(callback: types.CallbackQuery, state: FSMContext):
    phone = callback.data.split("_")[3]
    await state.update_data(account_phone=phone)
    await callback.message.answer("Введите новый интервал в минутах:")
    await state.set_state(Form.enter_interval_value)

@dp.callback_query(F.data.startswith("select_account_for_message_interval_"))
async def select_account_for_message_interval(callback: types.CallbackQuery, state: FSMContext):
    phone = callback.data.split("_")[5]
    await state.update_data(account_phone=phone)
    messages = await message_manager.get_messages(phone)
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=msg['message_text'][:30], callback_data=f"select_message_interval_{msg['id']}")]
        for msg in messages
    ] + [[types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]])
    await callback.message.answer("Выберите сообщение:", reply_markup=keyboard)
    await state.set_state(Form.select_message_for_interval)

@dp.callback_query(F.data.startswith("select_message_interval_"))
async def select_message_interval(callback: types.CallbackQuery, state: FSMContext):
    message_id = int(callback.data.split("_")[3])
    await state.update_data(interval_type="message", message_id=message_id)
    await callback.message.answer("Введите новый интервал в минутах:")
    await state.set_state(Form.enter_interval_value)

@dp.message(Form.enter_interval_value)
async def process_interval_value(message: types.Message, state: FSMContext):
    data = await state.get_data()
    interval_type = data['interval_type']
    try:
        interval = int(message.text)
        if interval <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Неверный формат числа. Введите целое число больше 0.")
        return
    
    success = False
    if interval_type == "group":
        group_id = data['group_id']
        success = await group_manager.update_group_interval(group_id, interval)
    elif interval_type == "account":
        phone = data['account_phone']
        success = await account_manager.update_account_interval(phone, interval)
    elif interval_type == "message":
        message_id = data['message_id']
        success = await message_manager.update_message_interval(message_id, interval)
    
    if success:
        await message.answer("✅ Интервал успешно обновлен!", reply_markup=main_menu())
    else:
        await message.answer("❌ Ошибка обновления интервала.")
    
    await state.clear()

@dp.callback_query(F.data == "manage_categories")
async def manage_categories_menu(callback: types.CallbackQuery):
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="➕ Добавить категорию", callback_data="add_category")],
        [types.InlineKeyboardButton(text="🗑️ Удалить категорию", callback_data="delete_category")],
        [types.InlineKeyboardButton(text="🔗 Привязать к аккаунту", callback_data="link_account_category")],
        [types.InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")]
    ])
    await callback.message.edit_text(
        "🗂️ *Управление категориями:*",
        reply_markup=keyboard,
        parse_mode="MarkdownV2"
    )

@dp.callback_query(F.data == "add_category")
async def add_category_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(Form.add_category)
    await callback.message.answer(
        "Введите название новой категории:",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]]
        )
    )

@dp.message(Form.add_category)
async def process_add_category(message: types.Message, state: FSMContext):
    category = message.text.strip()
    if await category_manager.add_category(category):
        await message.answer(f"✅ Категория '{category}' добавлена!", reply_markup=main_menu())
    else:
        await message.answer("❌ Ошибка добавления категории!")
    await state.clear()

@dp.callback_query(F.data == "delete_category")
async def delete_category_handler(callback: types.CallbackQuery, state: FSMContext):
    categories = await category_manager.get_all_categories()
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text=cat, callback_data=f"delete_cat_{cat}")]
        for cat in categories
    ] + [[types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]])
    await callback.message.answer(
        "Выберите категорию для удаления:",
        reply_markup=keyboard
    )

@dp.callback_query(F.data.startswith("delete_cat_"))
async def process_delete_category(callback: types.CallbackQuery):
    category = callback.data.split("_")[2]
    if await category_manager.remove_category(category):
        await callback.answer(f"✅ Категория '{category}' удалена!")
    else:
        await callback.answer("❌ Ошибка удаления категории!")
    await callback.message.delete()

@dp.callback_query(F.data == "link_account_category")
async def link_account_category_handler(callback: types.CallbackQuery, state: FSMContext):
    accounts = await account_manager.get_all_accounts()
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text=acc['username'] or acc['phone'], callback_data=f"select_acc_cat_{acc['phone']}")] for acc in accounts] + [[types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]])
    await callback.message.answer(
        "Выберите аккаунт для привязки категорий:",
        reply_markup=keyboard
    )
    await state.set_state(Form.link_account_category)

@dp.callback_query(F.data.startswith("select_acc_cat_"))
async def select_account_for_category(callback: types.CallbackQuery, state: FSMContext):
    phone = callback.data.split("_")[3]
    await state.update_data(account_phone=phone)
    
    categories = await category_manager.get_all_categories()
    current_categories = await account_manager.get_account_categories(phone)
    
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(
            text=f"{'✅' if cat in current_categories else '☑️'} {cat}", 
            callback_data=f"toggle_cat_{cat}")]
        for cat in categories
    ] + [
        [types.InlineKeyboardButton(text="💾 Сохранить", callback_data="save_categories")],
        [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])
    
    await callback.message.edit_text(
        f"📌 Текущие категории для аккаунта {phone}:\n\n" +
        "\n".join([f"• {cat}" for cat in current_categories]),
        reply_markup=keyboard
    )

@dp.callback_query(F.data.startswith("toggle_cat_"))
async def toggle_category(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    phone = data['account_phone']
    category = callback.data.split("_")[2]
    
    current_categories = await account_manager.get_account_categories(phone)
    if category in current_categories:
        await account_manager.remove_account_category(phone, category)
    else:
        await account_manager.add_account_category(phone, category)
    
    current_categories = await account_manager.get_account_categories(phone)
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(
            text=f"{'✅' if cat in current_categories else '☑️'} {cat}", 
            callback_data=f"toggle_cat_{cat}")]
        for cat in await category_manager.get_all_categories()
    ] + [
        [types.InlineKeyboardButton(text="💾 Сохранить", callback_data="save_categories")],
        [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])
    
    await callback.message.edit_text(
        f"📌 Текущие категории для аккаунта {phone}:\n\n" +
        "\n".join([f"• {cat}" for cat in current_categories]),
        reply_markup=keyboard
    )

@dp.callback_query(F.data == "save_categories")
async def save_categories(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer("✅ Категории сохранены!")
    await state.clear()
    await callback.message.delete()

@dp.callback_query(F.data == "main_menu")
async def return_to_main_menu(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🤖 *Главное меню:*",
        reply_markup=main_menu(),
        parse_mode="MarkdownV2"
    )

@dp.errors()
async def errors_handler(update, exception):
    # чтобы любые ошибки не крашили бот
    logger.error(f"Ошибка при обработке update {update}: {exception}")
    return True

async def on_startup():
    # инициализация БД и менеджеров
    global db, account_manager, group_manager, message_manager, category_manager
    db = Database(DB_FILE)
    await db.connect()
    account_manager = AccountManager(db)
    group_manager = GroupManager(db)
    message_manager = MessageManager(db)
    category_manager = CategoryManager(db)
    asyncio.create_task(group_updater_task())

async def on_shutdown():
    await db.close()
    await bot.session.close()

async def main():
    await on_startup()
    # теперь стартуем polling с обработчиками
    await dp.start_polling(bot, on_startup=on_startup, on_shutdown=on_shutdown)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
