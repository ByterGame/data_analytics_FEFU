import os
import logging
from typing import Optional, Dict, List, Any
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection as Connection
from psycopg2.pool import SimpleConnectionPool

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Менеджер для работы с PostgreSQL базой данных"""
    
    def __init__(self):
        self.db_params = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'game_platform'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
        }
        
        self.connection_pool = None
        self._init_pool()
        self._init_database()
    
    def _init_pool(self, min_conn: int = 1, max_conn: int = 10):
        """Инициализация пула соединений"""
        try:
            self.connection_pool = SimpleConnectionPool(
                min_conn, max_conn, **self.db_params
            )
            logger.info(f"Пул соединений PostgreSQL инициализирован: {self.db_params['database']}")
        except Exception as e:
            logger.error(f"Ошибка инициализации пула соединений: {e}")
            raise
    
    def _init_database(self):
        """Инициализация таблиц базы данных"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username VARCHAR(100) UNIQUE NOT NULL,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    country_code CHAR(2) NOT NULL,
                    region VARCHAR(100),
                    registration_date TIMESTAMP NOT NULL,
                    total_spent DECIMAL(12,2) DEFAULT 0.00,
                    last_active TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS developers (
                    developer_id INTEGER PRIMARY KEY,
                    studio_name VARCHAR(255) UNIQUE NOT NULL,
                    country_code CHAR(2) NOT NULL,
                    foundation_year INTEGER,
                    total_revenue DECIMAL(12,2) DEFAULT 0.00,
                    contact_email VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS games (
                    game_id INTEGER PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    developer_id INTEGER NOT NULL REFERENCES developers(developer_id) ON DELETE CASCADE,
                    release_date DATE NOT NULL,
                    base_price DECIMAL(10,2) DEFAULT 0.00,
                    current_price DECIMAL(10,2) DEFAULT 0.00,
                    monetization_type VARCHAR(10) CHECK (monetization_type IN ('free', 'paid')) NOT NULL,
                    genre_main VARCHAR(50) NOT NULL,
                    genre_tags TEXT,
                    age_rating VARCHAR(10) NOT NULL,
                    total_purchases INTEGER DEFAULT 0,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_library (
                    user_game_id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(user_id),
                    game_id INTEGER REFERENCES games(game_id),
                    purchase_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, game_id)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(user_id),
                    game_id INTEGER REFERENCES games(game_id),
                    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    amount DECIMAL(10,2),
                    developer_revenue DECIMAL(10,2),
                    platform_commission DECIMAL(10,2)
                )
            ''')
            
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_country ON users(country_code)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_games_developer ON games(developer_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_games_genre ON games(genre_main)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_library_user ON user_library(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_library_game ON user_library(game_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date)')
            
            conn.commit()
            logger.info(f"Таблицы базы данных инициализированы")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка инициализации таблиц: {e}")
            raise
        finally:
            self.return_connection(conn)
    
    def get_connection(self) -> Connection:
        """Получение соединения из пула"""
        if self.connection_pool is None:
            self._init_pool()
        return self.connection_pool.getconn()
    
    def return_connection(self, conn: Connection):
        """Возврат соединения в пул"""
        if self.connection_pool:
            self.connection_pool.putconn(conn)
    
    def execute_query(self, query: str, params: tuple = ()) -> psycopg2.extensions.cursor:
        """Выполнение SQL запроса с получением курсора"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            conn.commit()
            return cursor
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка выполнения запроса: {e}")
            raise
        finally:
            self.return_connection(conn)
    
    def fetch_one(self, query: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
        """Получение одной записи"""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            cursor.execute(query, params)
            result = cursor.fetchone()
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Ошибка fetch_one: {e}")
            return None
        finally:
            self.return_connection(conn)
    
    def fetch_all(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Получение всех записей"""
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            cursor.execute(query, params)
            results = cursor.fetchall()
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Ошибка fetch_all: {e}")
            return []
        finally:
            self.return_connection(conn)
    
    def execute_with_connection(self, query: str, params: tuple = ()) -> psycopg2.extensions.cursor:
        """Выполнение запроса с явным управлением соединением (для транзакций)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        return cursor, conn
    
    def commit_connection(self, conn: Connection):
        """Коммит соединения"""
        conn.commit()
        self.return_connection(conn)
    
    def rollback_connection(self, conn: Connection):
        """Откат соединения"""
        conn.rollback()
        self.return_connection(conn)
    
    def close_all(self):
        """Закрытие всех соединений пула"""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("Все соединения пула закрыты")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_all()


db_manager = DatabaseManager()