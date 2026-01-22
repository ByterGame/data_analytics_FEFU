import random
import time
import math
from datetime import datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from generator.generator import DataGenerator, DictionaryLoader
from database.repository import (
    user_repo, 
    developer_repo, 
    game_repo
)


class MarketSimulator:
    """Модель рынка игровой индустрии на основе реальной статистики"""
    
    STEAM_STATS = {
        'daily_new_users': 600000,
        'total_users': 132000000,
        'daily_active': 33000000,
        'total_games': 50000,
        'total_developers': 28000,
        'avg_games_per_dev': 1.78,
        'avg_daily_new_games': 38,
        'avg_ccu_per_game': 380,
        'market_growth_rate': 0.086,
    }
    
    @staticmethod
    def get_seasonal_multiplier(month: int) -> float:
        seasonal_factors = {
            1: 1.15, 2: 0.95, 3: 1.05, 4: 1.00, 5: 0.98,
            6: 0.90, 7: 0.85, 8: 0.92, 9: 1.10, 10: 1.20,
            11: 1.25, 12: 1.30
        }
        return seasonal_factors.get(month, 1.0)
    
    @staticmethod
    def get_weekday_multiplier(weekday: int) -> float:
        if weekday in [5, 6]:
            return 1.25
        elif weekday == 0:
            return 0.85
        else:
            return 1.0


class TimeSimulator:
    """Симулятор времени: 10 реальных секунд = 1 симулированный день"""
    
    def __init__(self, start_date: str = None):
        if start_date is None:
            start_date = datetime.now().strftime('%Y-%m-%d')
        self.real_start_time = datetime.now()
        self.sim_start_date = datetime.strptime(start_date, "%Y-%m-%d")
    
    def get_current_sim_day(self) -> int:
        """Возвращает текущий день симуляции"""
        real_elapsed = datetime.now() - self.real_start_time
        real_minutes = real_elapsed.total_seconds() / 60
        return int(real_minutes)
    
    def get_simulated_date(self) -> datetime:
        """Возвращает текущую дату в симуляции"""
        days = self.get_current_sim_day()
        return self.sim_start_date + timedelta(days=days)
    
    def get_simulated_datetime(self) -> datetime:
        """Возвращает дату и время в симуляции"""
        sim_date = self.get_simulated_date()
        hour = random.randint(9, 18)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        return sim_date.replace(hour=hour, minute=minute, second=second)


class EconomicSimulator:
    """Модель экономики игрового магазина"""        
    
    def calculate_daily_user_growth(self, current_users: int, current_games: int, month: int) -> int:
        """
        Комбинированная модель:
        1. Bass Diffusion - инновации + имитация
        2. Логистическое ограничение - насыщение рынка
        3. Сетевой эффект - меткалфов закон
        4. Сезонность и случайность
        """
        
        MARKET_POTENTIAL = 300_000_000 # Максимальное количество пользователей
        INNOVATION_COEFF = 0.0000005 # Коэффициент инноваций
        IMITATION_COEFF = 0.002 # Коэффициент имитации
        
        # Модель Басса для притока новых пользователей
        innovation_effect = INNOVATION_COEFF * (MARKET_POTENTIAL - current_users)
        imitation_effect = IMITATION_COEFF * (current_users / MARKET_POTENTIAL) * (MARKET_POTENTIAL - current_users)
        
        bass_growth = innovation_effect + imitation_effect
        
        GAMES_SATURATION_POINT = 50_000
        if current_games < GAMES_SATURATION_POINT:
            games_factor = 0.1 * (1 - current_games / GAMES_SATURATION_POINT)
            games_attraction = max(current_games * games_factor, 550)
        else:
            games_attraction = current_games * 0.01
        
        # Сетевой эффект закон Меткалфа
        if current_users > 1000:
            network_value = max(1, (current_users ** 1.1) / (10**5))
            network_effect = min(2.5, 1 + math.log10(network_value) * 0.3)
        else:
            network_effect = 1.0
        
        # Сезонность
        seasonal = MarketSimulator.get_seasonal_multiplier(month)
        
        # Общий приток
        total_inflow = (bass_growth + games_attraction) * network_effect * seasonal
        
        # Случайные колебания 
        random_factor = random.uniform(0.6, 1.4)
        
        max_daily_growth = current_users * 0.05  # Не более 5% в день
        daily_change = min(total_inflow * random_factor, max_daily_growth)
        return daily_change
    
    def calculate_daily_dev_growth(self, current_devs: int, current_users: int) -> int:
        """
        Рост разработчиков зависит от:
        1. Размер аудитории
        2. Конкуренция
        """
        
        # Потенциальная аудитория
        if current_users > 10_000:
            audience_factor = math.log10(max(1, current_users / 10_000)) * 1.5 + 1
        else:
            audience_factor = 1
        
        # Конкуренция (чем больше разработчиков, тем сложнее выделиться)
        competition_factor = 1.0
        if current_devs > 5000:
            competition_factor = 5000 / current_devs
        
        # Базовая вероятность прихода нового разработчика
        base_probability = 0.2 * audience_factor * competition_factor
        if base_probability > 0:
            return max(0, random.gauss(base_probability, math.sqrt(base_probability)))
        
        return 0

    def calculate_daily_game_growth(self, active_devs: int, current_games: int, current_users: int) -> int:
        """
        Рост игр зависит от:
        1. Спроса (пользователи)
        2. Сложности разработки (увеличивается со временем)
        3. Трендов (случайность)
        """
        
        # Спрос от пользователей
        demand_factor = min(current_users**0.1, 10)
        
        # чем больше игр, тем сложнее сделать уникальную
        if current_games < 1000:
            uniqueness_factor = 1.0
        elif current_games < 10_000:
            uniqueness_factor = 0.8
        elif current_games < 50_000:
            uniqueness_factor = 0.55
        else:
            uniqueness_factor = 0.3
        
        # Тренды и случайность
        trend_factor = random.uniform(0.5, 1.5)
        
        expected_games = (active_devs * demand_factor * uniqueness_factor * trend_factor)
        if expected_games > 0:
            return max(0, random.gauss(expected_games, math.sqrt(expected_games)))
        return 0


class ContinuousGenerator:
    """Генератор данных с непрерывной симуляцией"""
    
    def __init__(self, sim_start_date=None):
        if sim_start_date is None:
            sim_start_date = datetime.now().strftime('%Y-%m-%d')
            
        self.time_simulator = TimeSimulator(sim_start_date)
        self.market_model = MarketSimulator()
        self.economy = EconomicSimulator()
        self.generator = DataGenerator(DictionaryLoader())

        self.active_users_cnt = 0
        self.new_devs = 0
        self.new_users = 0
        self.new_games = 0
        
        # Проверяем и генерируем начальные данные если нужно
        if self._should_generate_initial_data():
            self._generate_initial_data()
        
        print("Генератор данных инициализирован")
        print(f"Симуляция начата с даты: {sim_start_date}")
        print(f"Текущий день симуляции: {self.time_simulator.get_current_sim_day()}")
    
    def _should_generate_initial_data(self) -> bool:
        """Проверяем, нужно ли генерировать начальные данные"""
        user_count = user_repo.get_user_count()
        dev_count = developer_repo.get_developer_count()
        game_count = game_repo.get_game_count()
        
        return user_count == 0 and dev_count == 0 and game_count == 0
    
    def _generate_initial_data(self):
        """Генерация начальных данных в базу данных"""
        
        # Стартовые параметры
        INITIAL_USERS = 10000
        INITIAL_DEVELOPERS = 10
        
        sim_date = self.time_simulator.get_simulated_date()
        
        # Генерация начальных пользователей
        initial_users = self.generator.create_users_batch(INITIAL_USERS, sim_date)
        user_success = user_repo.insert_users_batch(initial_users)
        print(f"Создано {user_success} начальных пользователей")
        
        # Генерация начальных разработчиков
        initial_devs = self.generator.create_developers_batch(INITIAL_DEVELOPERS, sim_date)
        dev_success = developer_repo.insert_developers_batch(initial_devs)
        print(f"Создано {dev_success} начальных разработчиков")
        
        # Генерация начальных игр
        total_games_created = 0
        for dev in initial_devs:
            games_count = random.randint(1, 3)
            games = self.generator.create_games_batch(
                games_count,
                sim_date,
                dev['developer_id']
            )
            game_success = game_repo.insert_games_batch(games)
            total_games_created += game_success
        
        print(f"Создано {total_games_created} начальных игр")
    
    def calculate_daily_growth(self):
        """Расчет ежедневного роста на основе текущего состояния"""
        current_users = user_repo.get_user_count()
        current_games = game_repo.get_game_count()
        current_devs = developer_repo.get_developer_count()
        
        # Оценка активных пользователей
        base_activity = random.uniform(0.1, 0.25)
        seasonal_mult = MarketSimulator.get_seasonal_multiplier(self.time_simulator.get_simulated_datetime().month)
        weekday_mult = MarketSimulator.get_weekday_multiplier(self.time_simulator.get_simulated_datetime().weekday())
        random_variation = random.uniform(0.9, 1.1)
        
        activity_rate = base_activity * seasonal_mult * weekday_mult * random_variation
        activity_rate = min(max(activity_rate, 0.25), 0.8)
        
        self.active_users_cnt = int(current_users * activity_rate)
        
        sim_date = self.time_simulator.get_simulated_date()
        month = sim_date.month
        
        self.new_users += self.economy.calculate_daily_user_growth(
            current_users,
            current_games,
            month
        )
        
        self.new_devs += self.economy.calculate_daily_dev_growth(
            current_devs,
            current_users,
        )
        
        game_growth = self.economy.calculate_daily_game_growth(
            current_devs,
            current_games,
            self.active_users_cnt
        )
        self.new_games += min(game_growth, developer_repo.get_developer_count() / 175)
        
    def generate_users_batch(self):
        """Генерация пользователей"""
        if self.new_users >= 5:
            sim_date = self.time_simulator.get_simulated_date()
            
            batch_size = int(self.new_users // 5)
            self.new_users %= 1
            
            users = self.generator.create_users_batch(batch_size, sim_date)
            
            if users:
                success_count = user_repo.insert_users_batch(users)
                print(f"[День {self.time_simulator.get_current_sim_day()}] Добавлено {success_count} новых пользователей")
                return users
        
        return []

    def update_active_users(self):
        try:
            all_user_ids = user_repo.get_all_user_ids()
            if not all_user_ids:
                print("Нет пользователей для обновления активности")
                return
            if self.active_users_cnt > len(all_user_ids):
                active_ids = all_user_ids
            else:
                active_ids = random.sample(all_user_ids, self.active_users_cnt)
            
            current_time = self.time_simulator.get_simulated_datetime()
            success_count = 0
            
            for user_id in active_ids:
                try:
                    if user_repo.update_user_active(user_id, current_time):
                        success_count += 1
                except Exception as e:
                    print(f"Ошибка обновления активности для пользователя {user_id}: {e}")
                    continue
            
            print(f"Обновлена активность для {success_count}/{len(active_ids)} пользователей")
            
        except Exception as e:
            print(f"Ошибка в update_active_users: {e}")

    def delete_old_users(self):
        try:
            border_date = self.time_simulator.get_simulated_datetime() - timedelta(days=730) # Удаляю тех, кто неактивен 2 и более года
            delete_cnt = user_repo.delete_old_users(border_date)
            print(f"Удалено неактивных пользователей {delete_cnt}")
        except Exception as e:
            print(f"Ошибка при удалении старых пользователей {e}")

    
    def generate_developers_batch(self):
        """Генерация разработчиков"""
        if self.new_devs >= 1:
            sim_date = self.time_simulator.get_simulated_date()

            batch_size = int(self.new_devs // 1)
            self.new_devs %= 1

            developers = self.generator.create_developers_batch(batch_size, sim_date)
            
            if developers:
                success_count = developer_repo.insert_developers_batch(developers)
                print(f"[День {self.time_simulator.get_current_sim_day()}] Добавлено {success_count} новых разработчиков")
                return developers
        
        return []
    
    def generate_games_batch(self):
        """Генерация игр"""
        if self.new_games >= 1:
            sim_date = self.time_simulator.get_simulated_date()
            
            games = []
            batch_size = int(self.new_games // 1)
            self.new_games %= 1

            for _ in range(batch_size):
                game = self.generator.create_game(sim_date)
                games.append(game)
            
            if games:
                success_count = game_repo.insert_games_batch(games)
                print(f"[День {self.time_simulator.get_current_sim_day()}] Добавлено {success_count} новых игр")
                return games
        
        return []
    
    def add_transaction(self):
        wanna_sell = self.active_users_cnt * 0.03 # Мне уже лень что-то выдумывать, поэтому пусть просто каждый день 3% активных юзеров чето покупают
        sold_cnt = self.generator.add_transaction(wanna_sell, self.time_simulator.get_simulated_datetime())
        print(f"[День {self.time_simulator.get_current_sim_day()}] продано игр {sold_cnt}")
    
    def print_statistics(self):
        """Вывод текущей статистики"""
        total_users = user_repo.get_user_count()
        total_devs = developer_repo.get_developer_count()
        total_games = game_repo.get_game_count()
        
        peak_concurrent = int(self.active_users_cnt * 1.2)
        
        print("\n" + "="*50)
        print(f"День симуляции: {self.time_simulator.get_current_sim_day()}")
        print("-"*50)
        print(f"Пользователей: {total_users:,}")
        print(f"Активных пользователей: {self.active_users_cnt:,} ({self.active_users_cnt*100:.1f}%)")
        print(f"Разработчиков: {total_devs:,}")
        print(f"Игр: {total_games:,}")
        print(f"Пиковый онлайн: {peak_concurrent:,}")
        print("="*50)
    
    def run_scheduler(self):
        """Запуск планировщика задач"""
        scheduler = BackgroundScheduler()
        self.calculate_daily_growth()
        scheduler.add_job(
            self.calculate_daily_growth,
            IntervalTrigger(minutes=1),
            id="calculate_daily_growth"
        )

        scheduler.add_job(
            self.generate_users_batch,
            IntervalTrigger(minutes=1),
            id='generate_users'
        )
        
        scheduler.add_job(
            self.generate_developers_batch,
            IntervalTrigger(minutes=1),
            id='generate_developers'
        )
        
        scheduler.add_job(
            self.generate_games_batch,
            IntervalTrigger(minutes=1),
            id='generate_games'
        )

        scheduler.add_job(
            self.update_active_users,
            IntervalTrigger(minutes=1),
            id="update_active_users"
        )

        scheduler.add_job(
            self.delete_old_users,
            IntervalTrigger(minutes=30),
            id="delete_old_users"
        )

        scheduler.add_job(
            self.add_transaction,
            IntervalTrigger(minutes=1),
            id="add_transaction"
        )
        
        scheduler.add_job(
            self.print_statistics,
            IntervalTrigger(minutes=5),
            id='print_stats'
        )
        
        scheduler.start()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nОстановка симуляции...")
            scheduler.shutdown()
            self.print_statistics()


def main():
    """Основная функция запуска симуляции"""
    generator = ContinuousGenerator()
    generator.run_scheduler()


if __name__ == "__main__":
    main()