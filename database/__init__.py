import sqlite3
from pathlib import Path
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Менеджер для работы с SQLite базой данных"""
    def __init__(self, db_path: str = "game_platform.db"):
        current_dir = Path(__file__).parent
        self.db_path = current_dir / db_path
        
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.connection: Optional[sqlite3.Connection] = None
        self._init_database()
    
    def _init_database(self):
        """Инициализация таблиц базы данных"""
        self.connect()
        
        cursor = self.connection.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            country_code TEXT NOT NULL,
            region TEXT,
            registration_date DATETIME NOT NULL,
            total_spent REAL DEFAULT 0.00,
            last_active DATETIME,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS developers (
            developer_id INTEGER PRIMARY KEY,
            studio_name TEXT UNIQUE NOT NULL,
            country_code TEXT NOT NULL,
            foundation_year INTEGER,
            total_revenue REAL DEFAULT 0.00,
            contact_email TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS games (
            game_id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            developer_id INTEGER NOT NULL,
            release_date DATE NOT NULL,
            base_price REAL DEFAULT 0.00,
            current_price REAL DEFAULT 0.00,
            monetization_type TEXT CHECK(monetization_type IN ('free', 'paid')) NOT NULL,
            genre_main TEXT NOT NULL,
            genre_tags TEXT,
            age_rating TEXT NOT NULL,
            total_purchases INTEGER DEFAULT 0,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (developer_id) REFERENCES developers (developer_id) ON DELETE CASCADE
        )
        ''')
        
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_country ON users(country_code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_games_developer ON games(developer_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_games_genre ON games(genre_main)')
        
        self.connection.commit()
        logger.info(f"База данных инициализирована: {self.db_path}")
    
    def connect(self):
        """Создание подключения к базе данных"""
        self.connection = sqlite3.connect(self.db_path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
    
    def get_connection(self) -> sqlite3.Connection:
        """Получение соединения с БД"""
        if self.connection is None:
            self.connect()
        return self.connection
    
    def execute_query(self, query: str, params: tuple = ()):
        """Выполнение SQL запроса"""
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        self.connection.commit()
        return cursor
    
    def fetch_one(self, query: str, params: tuple = ()):
        """Получение одной записи"""
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        return cursor.fetchone()
    
    def fetch_all(self, query: str, params: tuple = ()):
        """Получение всех записей"""
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()
    
    def close(self):
        """Закрытие соединения с базой данных"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


db_manager = DatabaseManager()
