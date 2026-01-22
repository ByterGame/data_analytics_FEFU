import json
from typing import List, Dict, Optional
from datetime import datetime
import logging
import threading

from . import db_manager

logger = logging.getLogger(__name__)


class BaseRepository:
    """Базовый класс репозитория"""
    def __init__(self):
        self.db = db_manager
        self.db_lock = threading.Lock()

    def _format_datetime(self, dt) -> str:
        """Форматирование datetime в строку"""
        if isinstance(dt, datetime):
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        return dt


class UserRepository(BaseRepository):
    """Репозиторий для работы с пользователями"""
    
    def insert_user(self, user_data: Dict) -> bool:
        """Добавление пользователя в БД"""
        try:
            query = '''
                INSERT INTO users 
                (user_id, username, email, country_code, region, last_active, registration_date, total_spent)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            '''
            self.db.execute_query(query, (
                user_data['user_id'],
                user_data['username'],
                user_data['email'],
                user_data['country_code'],
                user_data['region'],
                self._format_datetime(user_data['registration_date']),
                self._format_datetime(user_data['registration_date']),
                user_data['total_spent']
            ))
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления пользователя {user_data.get('username')}: {e}")
            return False
    
    def insert_users_batch(self, users: List[Dict]) -> int:
        """Добавление нескольких пользователей - пакетная вставка для производительности"""
        if not users:
            return 0
            
        query = '''
            INSERT INTO users 
            (user_id, username, email, country_code, region, last_active, registration_date, total_spent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        '''
        
        success_count = 0
        try:
            cursor, conn = self.db.execute_with_connection("BEGIN", ())
            
            for user in users:
                try:
                    cursor.execute(query, (
                        user['user_id'],
                        user['username'],
                        user['email'],
                        user['country_code'],
                        user['region'],
                        self._format_datetime(user['registration_date']),
                        self._format_datetime(user['registration_date']),
                        user['total_spent']
                    ))
                    success_count += 1
                except Exception as e:
                    logger.warning(f"Пропущен пользователь {user.get('username')}: {e}")
                    continue
            
            self.db.commit_connection(conn)
            logger.info(f"Добавлено {success_count} из {len(users)} пользователей")
            
        except Exception as e:
            logger.error(f"Ошибка пакетного добавления пользователей: {e}")
            try:
                self.db.rollback_connection(conn)
            except:
                pass
        
        return success_count
    
    def get_user_count(self) -> int:
        """Получение количества пользователей"""
        result = self.db.fetch_one("SELECT COUNT(*) as count FROM users")
        return result['count'] if result else 0
    
    def get_all_user_ids(self) -> List[int]:
        """Получение всех ID пользователей"""
        results = self.db.fetch_all("SELECT user_id FROM users ORDER BY user_id")
        return [row['user_id'] for row in results]
    
    def get_user_by_id(self, user_id: int) -> Optional[Dict]:
        """Получение пользователя по ID"""
        result = self.db.fetch_one(
            "SELECT * FROM users WHERE user_id = %s",
            (user_id,)
        )
        return result if result else None
    
    def update_user_spent(self, user_id: int, amount: float, last_active: datetime) -> bool:
        """Обновление суммы потраченных средств пользователем"""
        with self.db_lock:
            try:
                self.db.execute_query('''
                    UPDATE users 
                    SET total_spent = total_spent + %s, last_active = %s
                    WHERE user_id = %s
                ''', (amount, last_active, user_id))
                return True
            except Exception as e:
                logger.error(f"Ошибка обновления пользователя {user_id}: {e}")
                return False
    
    def update_user_active(self, user_id: int, last_active: datetime) -> bool:
        """Обновление последнего времени активности у юзеров"""
        with self.db_lock:
            try:
                self.db.execute_query('''
                    UPDATE users
                    SET last_active = %s
                    WHERE user_id = %s
                ''', (last_active, user_id))
                return True
            except Exception as e:
                logger.error(f"Ошибка обновления активности пользователя: {e}")
                return False
    
    def delete_old_users(self, border_date: datetime) -> int:
        """Удаление пользователей, которые в последний раз были активны до border_date"""
        with self.db_lock:
            try:
                cursor = self.db.execute_query('''
                    DELETE FROM users
                    WHERE last_active < %s
                ''', (border_date,))
                return cursor.rowcount if cursor else 0
            except Exception as e:
                logger.error(f"Ошибка удаления неактивных пользователей: {e}")
                return 0


class DeveloperRepository(BaseRepository):
    """Репозиторий для работы с разработчиками"""
    
    def insert_developer(self, developer_data: Dict) -> bool:
        """Добавление разработчика в БД"""
        try:
            self.db.execute_query('''
                INSERT INTO developers 
                (developer_id, studio_name, country_code, foundation_year, total_revenue, contact_email)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (
                developer_data['developer_id'],
                developer_data['studio_name'],
                developer_data['country_code'],
                developer_data['foundation_year'],
                developer_data['total_revenue'],
                developer_data['contact_email']
            ))
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления разработчика {developer_data.get('studio_name')}: {e}")
            return False
    
    def insert_developers_batch(self, developers: List[Dict]) -> int:
        """Добавление нескольких разработчиков - пакетная вставка"""
        if not developers:
            return 0
            
        query = '''
            INSERT INTO developers 
            (developer_id, studio_name, country_code, foundation_year, total_revenue, contact_email)
            VALUES (%s, %s, %s, %s, %s, %s)
        '''
        
        success_count = 0
        try:
            cursor, conn = self.db.execute_with_connection("BEGIN", ())
            
            for dev in developers:
                try:
                    cursor.execute(query, (
                        dev['developer_id'],
                        dev['studio_name'],
                        dev['country_code'],
                        dev['foundation_year'],
                        dev['total_revenue'],
                        dev['contact_email']
                    ))
                    success_count += 1
                except Exception as e:
                    logger.warning(f"Пропущен разработчик {dev.get('studio_name')}: {e}")
                    continue
            
            self.db.commit_connection(conn)
            logger.info(f"Добавлено {success_count} из {len(developers)} разработчиков")
            
        except Exception as e:
            logger.error(f"Ошибка пакетного добавления разработчиков: {e}")
            try:
                self.db.rollback_connection(conn)
            except:
                pass
        
        return success_count
    
    def get_developer_count(self) -> int:
        """Получение количества разработчиков"""
        result = self.db.fetch_one("SELECT COUNT(*) as count FROM developers")
        return result['count'] if result else 0
    
    def get_all_developer_ids(self) -> List[int]:
        """Получение всех ID разработчиков"""
        results = self.db.fetch_all("SELECT developer_id FROM developers ORDER BY developer_id")
        return [row['developer_id'] for row in results]
    
    def get_random_developer_id(self) -> Optional[int]:
        """Получение случайного ID разработчика"""
        result = self.db.fetch_one(
            "SELECT developer_id FROM developers ORDER BY RANDOM() LIMIT 1"
        )
        return result['developer_id'] if result else None
    
    def get_developer_by_id(self, id: int) -> Dict:
        """Получение конкретного разработчика по id"""
        try:
            result = self.db.fetch_one("SELECT * FROM developers WHERE developer_id = %s", (id,))
            return result if result else {}
        except Exception as e:
            logger.error(f"Ошибка при получении разработчика с id {id}: {e}")
            return {}
    
    def update_developer_revenue(self, developer_id: int, revenue: float) -> bool:
        """Обновление выручки разработчика"""
        try:
            self.db.execute_query('''
                UPDATE developers 
                SET total_revenue = total_revenue + %s
                WHERE developer_id = %s
            ''', (revenue, developer_id))
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления разработчика {developer_id}: {e}")
            return False


class GameRepository(BaseRepository):
    """Репозиторий для работы с играми"""
    
    def insert_game(self, game_data: Dict) -> bool:
        """Добавление игры в БД"""
        try:
            genre_tags = game_data.get('genre_tags')
            if isinstance(genre_tags, list):
                genre_tags = json.dumps(genre_tags)
            
            self.db.execute_query('''
                INSERT INTO games 
                (game_id, title, developer_id, release_date, base_price, current_price,
                 monetization_type, genre_main, genre_tags, age_rating, total_purchases, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                game_data['game_id'],
                game_data['title'],
                game_data['developer_id'],
                game_data['release_date'],
                game_data['base_price'],
                game_data['current_price'],
                game_data['monetization_type'],
                game_data['genre_main'],
                genre_tags,
                game_data['age_rating'],
                game_data['total_purchases'],
                game_data.get('is_active', True)
            ))
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления игры {game_data.get('title')}: {e}")
            return False
    
    def insert_games_batch(self, games: List[Dict]) -> int:
        """Добавление нескольких игр - пакетная вставка"""
        if not games:
            return 0
            
        success_count = 0
        try:
            cursor, conn = self.db.execute_with_connection("BEGIN", ())
            
            for game in games:
                try:
                    genre_tags = game.get('genre_tags')
                    if isinstance(genre_tags, list):
                        genre_tags = json.dumps(genre_tags)
                    
                    cursor.execute('''
                        INSERT INTO games 
                        (game_id, title, developer_id, release_date, base_price, current_price,
                         monetization_type, genre_main, genre_tags, age_rating, total_purchases, is_active)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        game['game_id'],
                        game['title'],
                        game['developer_id'],
                        game['release_date'],
                        game['base_price'],
                        game['current_price'],
                        game['monetization_type'],
                        game['genre_main'],
                        genre_tags,
                        game['age_rating'],
                        game['total_purchases'],
                        game.get('is_active', True)
                    ))
                    success_count += 1
                except Exception as e:
                    logger.warning(f"Пропущена игра {game.get('title')}: {e}")
                    continue
            
            self.db.commit_connection(conn)
            logger.info(f"Добавлено {success_count} из {len(games)} игр")
            
        except Exception as e:
            logger.error(f"Ошибка пакетного добавления игр: {e}")
            try:
                self.db.rollback_connection(conn)
            except:
                pass
        
        return success_count
    
    def get_game_count(self) -> int:
        """Получение количества игр"""
        result = self.db.fetch_one("SELECT COUNT(*) as count FROM games")
        return result['count'] if result else 0
    
    def get_games_by_developer(self, developer_id: int) -> List[Dict]:
        """Получение игр разработчика"""
        results = self.db.fetch_all(
            "SELECT * FROM games WHERE developer_id = %s ORDER BY release_date",
            (developer_id,)
        )
        return results
    
    def get_random_game(self) -> Optional[Dict]:
        """Получение случайной игры"""
        result = self.db.fetch_one(
            "SELECT * FROM games WHERE is_active = TRUE ORDER BY RANDOM() LIMIT 1"
        )
        return result if result else {}
    
    def get_can_purchases_games(self, border_purchases: int) -> List[Dict]:
        """Получение игр, которые еще не раскупили"""
        try:
            result = self.db.fetch_all('''
                SELECT * FROM games WHERE total_purchases < %s
            ''', (border_purchases,))
            return result
        except Exception as e:
            logger.error(f"Ошибка при получении игр get_can_purchases_games: {e}")
            return []
    
    def update_game_purchases(self, game_id: int, purchases: int = 1) -> bool:
        """Обновление количества покупок игры"""
        try:
            self.db.execute_query('''
                UPDATE games 
                SET total_purchases = total_purchases + %s
                WHERE game_id = %s
            ''', (purchases, game_id))
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления игры {game_id}: {e}")
            return False


class TransactionRepository(BaseRepository):
    """Репозиторий для работы с транзакциями"""
    
    def create_transaction(self, transaction_data: Dict) -> bool:
        """Создание новой транзакции"""
        try:
            self.db.execute_query('''
                INSERT INTO transactions 
                (user_id, game_id, transaction_date, amount, developer_revenue, platform_commission)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (
                transaction_data['user_id'],
                transaction_data['game_id'],
                transaction_data['transaction_date'],
                transaction_data['amount'],
                transaction_data['developer_revenue'],
                transaction_data['platform_commission']
            ))
            return True
        except Exception as e:
            logger.error(f"Ошибка создания транзакции: {e}")
            return False
    
    def create_transactions_batch(self, transactions: List[Dict]) -> int:
        """Создание нескольких транзакций - пакетная вставка"""
        if not transactions:
            return 0
            
        query = '''
            INSERT INTO transactions 
            (user_id, game_id, transaction_date, amount, developer_revenue, platform_commission)
            VALUES (%s, %s, %s, %s, %s, %s)
        '''
        
        success_count = 0
        try:
            cursor, conn = self.db.execute_with_connection("BEGIN", ())
            
            for trans in transactions:
                try:
                    cursor.execute(query, (
                        trans['user_id'],
                        trans['game_id'],
                        trans['transaction_date'],
                        trans['amount'],
                        trans['developer_revenue'],
                        trans['platform_commission']
                    ))
                    success_count += 1
                except Exception as e:
                    logger.warning(f"Пропущена транзакция: {e}")
                    continue
            
            self.db.commit_connection(conn)
            
        except Exception as e:
            logger.error(f"Ошибка пакетного создания транзакций: {e}")
            try:
                self.db.rollback_connection(conn)
            except:
                pass
        
        return success_count
    
    def get_total_platform_revenue(self) -> float:
        """Получение общей выручки платформы"""
        result = self.db.fetch_one('''
            SELECT COALESCE(SUM(platform_commission), 0) as total_revenue 
            FROM transactions
        ''')
        return float(result['total_revenue']) if result else 0.0
    
    def get_daily_platform_revenue(self, date: datetime) -> float:
        """Получение выручки платформы за день"""
        result = self.db.fetch_one('''
            SELECT COALESCE(SUM(platform_commission), 0) as daily_revenue 
            FROM transactions
            WHERE DATE(transaction_date) = DATE(%s)
        ''', (date,))
        return float(result['daily_revenue']) if result else 0.0


class UserLibraryRepository(BaseRepository):
    """Репозиторий для работы с библиотекой игр пользователей"""
    
    def add_game_to_library(self, user_id: int, game_id: int, purchase_date: datetime) -> bool:
        """Добавление игры в библиотеку пользователя"""
        try:
            self.db.execute_query('''
                INSERT INTO user_library (user_id, game_id, purchase_date)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id, game_id) DO NOTHING
            ''', (user_id, game_id, purchase_date))
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления игры {game_id} в библиотеку пользователя {user_id}: {e}")
            return False
    
    def add_games_to_library_batch(self, library_entries: List[Dict]) -> int:
        """Добавление нескольких игр в библиотеку - пакетная вставка"""
        if not library_entries:
            return 0
            
        query = '''
            INSERT INTO user_library (user_id, game_id, purchase_date)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, game_id) DO NOTHING
        '''
        
        success_count = 0
        try:
            cursor, conn = self.db.execute_with_connection("BEGIN", ())
            
            for entry in library_entries:
                try:
                    cursor.execute(query, (
                        entry['user_id'],
                        entry['game_id'],
                        entry['purchase_date']
                    ))
                    success_count += 1
                except Exception as e:
                    logger.warning(f"Пропущено добавление в библиотеку: {e}")
                    continue
            
            self.db.commit_connection(conn)
            
        except Exception as e:
            logger.error(f"Ошибка пакетного добавления в библиотеку: {e}")
            try:
                self.db.rollback_connection(conn)
            except:
                pass
        
        return success_count
    
    def get_users_without_game(self, game_id: int) -> List[int]:
        """Получение пользователей, у которых нет указанной игры"""
        try:
            query = '''
                SELECT u.user_id FROM users u
                WHERE u.user_id NOT IN (
                    SELECT ul.user_id FROM user_library ul
                    WHERE ul.game_id = %s
                )
                ORDER BY u.user_id
            '''
            results = self.db.fetch_all(query, (game_id,))
            return [row['user_id'] for row in results]
        except Exception as e:
            logger.error(f"Ошибка получения пользователей без игры {game_id}: {e}")
            return []


user_repo = UserRepository()
developer_repo = DeveloperRepository()
game_repo = GameRepository()
transaction_repo = TransactionRepository()
user_lib_repo = UserLibraryRepository()