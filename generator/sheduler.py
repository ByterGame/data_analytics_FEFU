"""
Вместо простой генерации я начал реализовывать симуляцию, это показалось мне интереснее. Сейчас 1 минута реального времени должна равняться 1 дню в симуляции
Если я, конечно, ничего не перепутал
В настоящее время реализован только рост платформы (регистрация новых пользователей и разработчиков, а также создание игр), в дальнейшем будет проще работать уже
с таблицами, так как для изменения данных или контроля данных, которые зависят от других таблиц проще пользоваться запросами к бд. Скорее всего я просто напишу 
репозиторий и буду использовать его здесь.
"""

import json
import random
import time
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from generator import DataGenerator, DictionaryLoader 


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
    def calculate_scaling_factor(sim_days: int) -> float:
        base_factor = 0.001
        growth = 1 + (MarketSimulator.STEAM_STATS['market_growth_rate'] * sim_days / 365)
        return base_factor * growth
    
    @staticmethod
    def calculate_network_effect(active_users: int, total_users: int) -> float:
        if total_users < 100:
            return 1.0
        normalized_users = active_users / total_users if total_users > 0 else 0
        network_value = 1 + (normalized_users ** 2) * 2
        return min(network_value, 2.0)
    
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
    """Симулятор времени: 1 реальная минута = 1 симулированный день"""
    def __init__(self, start_date: str = None):
        if start_date is None:
            start_date = datetime.now().strftime('%Y-%m-%d')
        self.real_start_time = datetime.now()
        self.sim_start_date = datetime.strptime(start_date, "%Y-%m-%d")
        
    def get_simulated_date(self) -> datetime:
        real_elapsed = datetime.now() - self.real_start_time
        real_minutes = real_elapsed.total_seconds() / 60
        simulated_date = self.sim_start_date + timedelta(days=real_minutes)
        return simulated_date
    
    def get_simulated_datetime(self) -> datetime:
        sim_date = self.get_simulated_date()
        hour = random.randint(9, 18)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        return sim_date.replace(hour=hour, minute=minute, second=second)
    
    def get_simulated_date_str(self) -> str:
        return self.get_simulated_date().strftime("%Y-%m-%d")
    
    def get_simulated_time_elapsed(self) -> timedelta:
        return self.get_simulated_date() - self.sim_start_date
    
    def get_days_since_start(self) -> int:
        """Возвращает количество прошедших дней в симуляции"""
        elapsed = self.get_simulated_time_elapsed()
        return elapsed.days


class EconomicSimulator:
    """Модель экономики игрового магазина"""
    def __init__(self):
        self.user_churn_rate = 0.001
        self.dev_churn_rate = 0.0003
        
        self.user_growth_factors = {
            'existing_users': 0.00015,
            'new_games': 0.03,
            'seasonal': 1.0,
            'market_maturity': 1.0
        }
        
        self.dev_growth_factors = {
            'existing_devs': 0.0001,
            'active_users': 0.00002,
            'successful_games': 0.1
        }
        
        self.game_growth_factors = {
            'active_devs': 0.005,
            'market_demand': 1.0,
            'competition': 1.0
        }
    
    def calculate_daily_user_growth(self, current_users: int, current_games: int, 
                                   month: int, sim_days: int) -> int:
        base_growth = current_users * self.user_growth_factors['existing_users']
        game_attraction = current_games * self.user_growth_factors['new_games']
        seasonal = MarketSimulator.get_seasonal_multiplier(month)
        market_maturity = 1 / (1 + math.log(1 + sim_days / 365))
        
        if current_users > 100:
            network_effect = math.log(current_users / 100) * 0.1 + 1
        else:
            network_effect = 1.0
        
        total_growth = (base_growth + game_attraction) * seasonal * market_maturity * network_effect
        churn_loss = current_users * self.user_churn_rate
        net_growth = total_growth - churn_loss
        
        return max(int(net_growth), 0)
    
    def calculate_daily_dev_growth(self, current_devs: int, current_users: int, 
                                  successful_games: int) -> int:
        from_existing = current_devs * self.dev_growth_factors['existing_devs']
        from_users = current_users * self.dev_growth_factors['active_users']
        from_games = successful_games * self.dev_growth_factors['successful_games']
        churn_loss = current_devs * self.dev_churn_rate
        total_growth = from_existing + from_users + from_games - churn_loss
        
        if random.random() < min(total_growth, 1.0):
            return 1
        return 0
    
    def calculate_daily_game_growth(self, active_devs: int, current_games: int, 
                                   current_users: int) -> int:
        base_probability = active_devs * self.game_growth_factors['active_devs']
        demand_factor = min(current_users / 1000, 10)
        
        if current_games > 100:
            competition_factor = 100 / current_games
        else:
            competition_factor = 1.0
        
        total_probability = base_probability * demand_factor * competition_factor
        
        if total_probability > 0:
            games_count = max(0, int(random.normalvariate(total_probability, math.sqrt(total_probability))))
        else:
            games_count = 0
        
        return games_count


class ContinuousGenerator:
    def __init__(self, output_dir="simulated_data", sim_start_date=None):
        if sim_start_date is None:
            sim_start_date = datetime.now().strftime('%Y-%m-%d')
            
        self.time_simulator = TimeSimulator(sim_start_date)
        self.market_model = MarketSimulator()
        self.economy = EconomicSimulator()
        self.generator = DataGenerator(DictionaryLoader())
        
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Инициализация состояния рынка
        self.market_state = {
            'total_users': 10000,
            'total_developers': 70,
            'total_games': 280,
            'active_users': 6872,
            'successful_games': 144,
            'peak_concurrent': 60,
            'last_update_day': -1
        }
        
        self.stats = {
            'users': 0,
            'developers': 0,
            'games': 0,
            'real_start_time': datetime.now(),
            'sim_start_date': sim_start_date,
            'daily_stats': []
        }
        
        self.files = {
            'users': self.output_dir / 'users.jsonl',
            'developers': self.output_dir / 'developers.jsonl',
            'games': self.output_dir / 'games.jsonl'
        }
        
        for file in self.files.values():
            if not file.exists():
                file.write_text('')
        
        self._generate_initial_data()
    
    def _generate_initial_data(self):
        initial_users = self.generator.create_users_batch(
            self.market_state['total_users'], 
            self.time_simulator.get_simulated_date()
        )
        self.save_to_jsonl(initial_users, 'users')
        
        initial_devs = self.generator.create_developers_batch(
            self.market_state['total_developers'],
            self.time_simulator.get_simulated_date()
        )
        self.save_to_jsonl(initial_devs, 'developers')
        
        for dev in initial_devs:
            games_count = random.randint(1, 3)
            games = self.generator.create_games_batch(
                games_count,
                self.time_simulator.get_simulated_date(),
                dev['developer_id']
            )
            self.save_to_jsonl(games, 'games')
            self.market_state['total_games'] += games_count
        
    def save_to_jsonl(self, data: List[Dict], entity: str):
        if not data:
            return
            
        sim_date = self.time_simulator.get_simulated_date()
        sim_date_str = sim_date.strftime("%Y-%m-%d")
        
        with open(self.files[entity], 'a', encoding='utf-8') as f:
            for item in data:
                item['_simulated_date'] = sim_date_str
                item['_real_generated_at'] = datetime.now().isoformat()
                item['_market_state'] = self.market_state.copy()
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
    
    def check_and_update_day(self):
        """Проверяет, сменился ли день в симуляции и обновляет состояние"""
        current_sim_day = self.time_simulator.get_days_since_start()
        
        if current_sim_day < self.market_state['last_update_day']:
            return False, 0, 0, 0
        
        self.market_state['last_update_day'] = current_sim_day
        sim_date = self.time_simulator.get_simulated_date()
        month = sim_date.month
        weekday = sim_date.weekday()
        
        user_growth = self.economy.calculate_daily_user_growth(
            self.market_state['total_users'],
            self.market_state['total_games'],
            month,
            current_sim_day
        )
        
        dev_growth = self.economy.calculate_daily_dev_growth(
            self.market_state['total_developers'],
            self.market_state['total_users'],
            self.market_state['successful_games']
        )
        
        game_growth = self.economy.calculate_daily_game_growth(
            self.market_state['total_developers'],
            self.market_state['total_games'],
            self.market_state['active_users']
        )
        
        self.market_state['total_users'] += user_growth
        self.market_state['total_developers'] += dev_growth
        self.market_state['total_games'] += game_growth
        
        base_activity = 0.45
        weekday_mult = MarketSimulator.get_weekday_multiplier(weekday)
        seasonal_mult = MarketSimulator.get_seasonal_multiplier(month)
        
        if self.market_state['total_users'] > 100:
            network_factor = 1 + math.log(self.market_state['total_users'] / 100) * 0.05
        else:
            network_factor = 1.0
        
        random_variation = random.uniform(0.9, 1.1)
        
        activity_rate = base_activity * weekday_mult * seasonal_mult * network_factor * random_variation
        activity_rate = min(activity_rate, 0.8)
        activity_rate = max(activity_rate, 0.25)
        
        self.market_state['active_users'] = int(
            self.market_state['total_users'] * activity_rate
        )
        
        self.market_state['active_users'] = min(
            self.market_state['active_users'],
            self.market_state['total_users']
        )
        
        self.market_state['successful_games'] = max(1, int(self.market_state['total_games'] * 0.03))
        
        if self.market_state['active_users'] > self.market_state['peak_concurrent']:
            self.market_state['peak_concurrent'] = self.market_state['active_users']
        
        daily_stat = {
            'date': sim_date.strftime('%Y-%m-%d'),
            'users': self.market_state['total_users'],
            'developers': self.market_state['total_developers'],
            'games': self.market_state['total_games'],
            'active_users': self.market_state['active_users'],
            'user_growth': user_growth,
            'dev_growth': dev_growth,
            'game_growth': game_growth,
            'activity_rate': activity_rate,
            'sim_day': current_sim_day
        }
        self.stats['daily_stats'].append(daily_stat)
        
        self.stats['users'] += user_growth
        self.stats['developers'] += dev_growth
        self.stats['games'] += game_growth
        
        return True, user_growth, dev_growth, game_growth
    
    def generate_users_batch(self) -> List[Dict]:
        """Генерация пользователей"""
        sim_date = self.time_simulator.get_simulated_date()
        
        day_changed, user_growth, _, _ = self.check_and_update_day()
        
        if day_changed and user_growth > 0:
            users = self.generator.create_users_batch(user_growth, sim_date)
            self.save_to_jsonl(users, 'users')
            print(f"Сгенерированно {len(users)} новых пользователей")
            return users
        return []
    
    def generate_developers_batch(self) -> List[Dict]:
        """Генерация разработчиков"""
        sim_date = self.time_simulator.get_simulated_date()
        
        day_changed, _, dev_growth, _ = self.check_and_update_day()
        
        if day_changed and dev_growth > 0:
            developers = self.generator.create_developers_batch(dev_growth, sim_date)
            self.save_to_jsonl(developers, 'developers')
            print(f"Сгенерированно {len(developers)} новых разработчиков")
            return developers
        return []
    
    def generate_games_batch(self) -> List[Dict]:
        """Генерация игр"""
        sim_date = self.time_simulator.get_simulated_date()
        
        day_changed, _, _, game_growth = self.check_and_update_day()
        
        if day_changed and game_growth > 0 and self.market_state['total_developers'] > 0:
            games = []
            for _ in range(game_growth):
                dev_id = random.randint(0, self.market_state['total_developers'] - 1)
                game = self.generator.create_game(sim_date, dev_id)
                games.append(game)
            
            self.save_to_jsonl(games, 'games')
            print(f"Создано {len(games)} новых игр")
            return games
        return []
    

    def run_scheduler(self):
        scheduler = BackgroundScheduler()
        
        scheduler.add_job(
            self.generate_users_batch,
            IntervalTrigger(seconds=15),
            id='generate_users'
        )
        
        scheduler.add_job(
            self.generate_developers_batch,
            IntervalTrigger(minutes=1),
            id='generate_developers'
        )
        
        scheduler.add_job(
            self.generate_games_batch,
            IntervalTrigger(seconds=20),
            id='generate_games'
        )
        
        scheduler.add_job(
            self.daily_reset,
            'cron',
            hour=0,
            minute=0,
            id='daily_reset'
        )
        print("Начало работы")
        scheduler.start()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            scheduler.shutdown()
    
    def daily_reset(self):
        """Ежедневная архивация"""
        
        archive_dir = self.output_dir / 'archive' / datetime.now().strftime('%Y%m%d')
        archive_dir.mkdir(parents=True, exist_ok=True)
        
        for entity, filepath in self.files.items():
            if filepath.exists() and filepath.stat().st_size > 0:
                archive_file = archive_dir / f"{entity}_{datetime.now().strftime('%H%M%S')}.jsonl"
                filepath.rename(archive_file)
                filepath.write_text('')
        

def main():
    generator = ContinuousGenerator(output_dir="simulated_data")
    generator.run_scheduler()


if __name__ == "__main__":
    main()