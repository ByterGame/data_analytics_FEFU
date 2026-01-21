import json
import random
from typing import List, Dict, Set, Optional
from datetime import datetime, timedelta
from pathlib import Path
import coolname 
from faker import Faker

from database.repository import (
    user_repo, 
    developer_repo, 
    game_repo
)


class DictionaryLoader:
    """Загрузчик словарей из текущей директории"""
    
    def __init__(self):
        self.words = self._load_words_file("words_dictionary.json")
        self.templates = self._load_words_file("templates.json")
        self.countries_region = self._load_words_file("countries_region.json")
        
    def _load_words_file(self, filename: str) -> Dict:
        """Загружаем JSON файл из текущей директории"""
        file_path = Path(__file__).parent / filename
        
        if not file_path.exists():
            print(f"Файл '{filename}' не найден в {file_path.parent}")
            if filename == "words_dictionary.json":
                return {
                    'adjectives': ['Shadow', 'Dark', 'Epic', 'Golden', 'Mystic'],
                    'nouns': ['Realm', 'Kingdom', 'Dragon', 'Phoenix', 'Warrior'],
                    'mythical_creatures': ['Dragon', 'Phoenix', 'Griffin', 'Unicorn'],
                    'colors': ['Red', 'Blue', 'Black', 'White', 'Golden'],
                    'prefixes': ['Shadow', 'Dark', 'Epic', 'Golden'],
                    'locations': ['Forest', 'Mountain', 'Castle', 'Temple'],
                    'subtitles': ['Awakening', 'Rebirth', 'Origins', 'Legacy'],
                    'roman_numerals': ['II', 'III', 'IV', 'V', 'VI'],
                    'studio_suffixes': ['Games', 'Studios', 'Interactive'],
                    'edition_suffixes': ['HD', 'Remastered', 'Definitive Edition'],
                    'verbs': ['Rising', 'Falling', 'Awakening', 'Hunting']
                }
            elif filename == "templates.json":
                return {
                    'game_titles': [
                        '{adjective} {noun}',
                        'The {adjective} {noun}',
                        '{noun} {roman_numeral}',
                        '{mythical} {noun}'
                    ],
                    'studio_names': [
                        '{word1} {word2} {suffix}',
                        '{word1} {suffix}'
                    ]
                }
            else:
                return {}
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data
        except json.JSONDecodeError as e:
            print(f"ОШИБКА в JSON файле {filename}: {e}")
            return {}
        except Exception as e:
            print(f"ОШИБКА при загрузке {filename}: {e}")
            return {}


class DataGenerator:
    """Генератор игровых данных"""
    def __init__(self, dict_loader: DictionaryLoader):
        self.words = dict_loader.words
        self.templates = dict_loader.templates
        self.countries_region = dict_loader.countries_region
        
        self.game_templates = self.templates.get('game_titles', ['{adjective} {noun}'])
        self.studio_templates = self.templates.get('studio_names', ['{word1} {suffix}'])
        
        if not self.words.get('adjectives'):
            print("Словарь прилагательных пуст!")
        if not self.words.get('nouns'):
            print("Словарь существительных пуст!")

        self.fake = Faker()
        
        # Распределение по Steam статистике
        self.country_distribution = [
            ('US', 0.142), ('CN', 0.118), ('RU', 0.096), ('DE', 0.054),
            ('BR', 0.047), ('GB', 0.037), ('FR', 0.036), ('TR', 0.035),
            ('PL', 0.034), ('CA', 0.027), ('JP', 0.024), ('UA', 0.021),
            ('AU', 0.020), ('TW', 0.019), ('NL', 0.019), ('KR', 0.018),
            ('SE', 0.017), ('IT', 0.016), ('CZ', 0.015), ('RO', 0.014)
        ]
        self.genre_distribution = [
            ('Action', 0.22),
            ('Role-Playing (RPG)', 0.18),
            ('Adventure', 0.15),
            ('Strategy', 0.12),
            ('Simulation', 0.10),
            ('Sports', 0.08),
            ('Shooter', 0.07),
            ('Racing', 0.04),
            ('Puzzle', 0.04)
        ]
        
        self.age_rating_distribution = [
            ('3+', 0.05),
            ('7+', 0.15),
            ('12+', 0.40),
            ('16+', 0.30),
            ('18+', 0.10)
        ]
        self.monetization_distribution = [
            ('free', 0.25),
            ('paid', 0.75)
        ]
        self.genre_tags = {
            'Action': ['action', 'fast-paced', 'combat', 'adventure'],
            'Role-Playing (RPG)': ['rpg', 'story-rich', 'character-development', 'quests'],
            'Adventure': ['adventure', 'exploration', 'puzzle', 'narrative'],
            'Strategy': ['strategy', 'tactical', 'resource-management'],
            'Simulation': ['simulation', 'realistic', 'management', 'sandbox'],
            'Sports': ['sports', 'competitive', 'realistic', 'team-based'],
            'Shooter': ['shooter', 'fps', 'multiplayer', 'competitive'],
            'Racing': ['racing', 'driving', 'simulation', 'arcade'],
            'Puzzle': ['puzzle', 'casual', 'brain-teaser', 'logic']
        }
        
        self.generated_usernames: Set[str] = set()
        self.generated_emails: Set[str] = set()
        self.generated_game_titles: Set[str] = set()
        self.generated_studio_names: Set[str] = set()
        
        self.user_id_counter = self._get_next_user_id()
        self.developer_id_counter = self._get_next_developer_id()
        self.game_id_counter = self._get_next_game_id()
        
        self.last_game_dates = {}
    
    def _get_next_user_id(self) -> int:
        """Получение следующего ID для пользователя из БД"""
        try:
            user_ids = user_repo.get_all_user_ids()
            return max(user_ids) + 1 if user_ids else 0
        except Exception as e:
            print(f"Ошибка при получении ID пользователя из БД: {e}")
            return 0
    
    def _get_next_developer_id(self) -> int:
        """Получение следующего ID для разработчика из БД"""
        try:
            dev_ids = developer_repo.get_all_developer_ids()
            return max(dev_ids) + 1 if dev_ids else 0
        except Exception as e:
            print(f"Ошибка при получении ID разработчика из БД: {e}")
            return 0
    
    def _get_next_game_id(self) -> int:
        """Получение следующего ID для игры из БД"""
        try:
            return game_repo.get_game_count()
        except Exception as e:
            print(f"Ошибка при получении ID игры из БД: {e}")
            return 0
    
    def _get_random_word(self, category: str) -> str:
        """Берем случайное слово из категории"""
        words = self.words.get(category, [])
        if not words:
            print(f"Категория '{category}' пуста или не найдена")
            return "Unknown"
        return random.choice(words)
    
    def _get_random_country_region(self) -> tuple:
        """Возвращает случайную пару (country_code, region) по Steam статистике"""
        if not self.countries_region:
            countries, probs = zip(*self.country_distribution)
            country_code = random.choices(countries, weights=probs)[0]
            default_regions = {
                'US': ['California', 'New York', 'Texas', 'Florida'],
                'RU': ['Moscow', 'Saint Petersburg', 'Novosibirsk'],
                'DE': ['Berlin', 'Bavaria', 'Hamburg'],
                'CN': ['Guangdong', 'Beijing', 'Shanghai'],
                'BR': ['São Paulo', 'Rio de Janeiro', 'Minas Gerais']
            }
            region = random.choice(default_regions.get(country_code, ['Central']))
            return (country_code, region)
        
        if isinstance(self.countries_region, dict) and 'countries' in self.countries_region:
            countries_dict = {}
            for country_info in self.countries_region['countries']:
                countries_dict[country_info['code']] = country_info['regions']
            
            countries, probs = zip(*self.country_distribution)
            available_countries = [c for c in countries if c in countries_dict]
            
            if available_countries:
                available_probs = [p for c, p in self.country_distribution if c in available_countries]
                total = sum(available_probs)
                available_probs = [p/total for p in available_probs]
                
                country_code = random.choices(available_countries, weights=available_probs)[0]
                regions = countries_dict[country_code]
                region = random.choice(regions) if regions else "Central"
                return (country_code, region)
        print("Проблема с countries_region")
        print(self.countries_region)
        return ('US', 'California')
    
    def _generate_username(self) -> str:
        """Генерация имени пользователя"""
        username = coolname.generate_slug(2) 
    
        if random.random() < 0.7:
            username = username.split('-')[0]
        username = username.replace('-', '_')

        while username in self.generated_usernames:
            username += str(random.randint(1, 9))
        self.generated_usernames.add(username)
        return username
    
    def _generate_developer_email(self, studio_name: str) -> str:
        """Генерация email для разработчика на основе названия студии"""
        clean_name = ''.join(c for c in studio_name if c.isalnum() or c.isspace())
        clean_name = clean_name.lower().replace(' ', '')
        email = f"{clean_name}@gmail.com"
        return email
    
    def _get_random_country(self) -> str:
        """Возвращает случайный код страны для разработчика"""
        developer_countries = [
            ('US', 0.35),
            ('JP', 0.15),
            ('DE', 0.10),
            ('GB', 0.08),
            ('CA', 0.07),
            ('FR', 0.06),
            ('PL', 0.05),
            ('RU', 0.04),
            ('UA', 0.03),
            ('KR', 0.03),
            ('CN', 0.02),
            ('AU', 0.02),
        ]
        
        countries, probs = zip(*developer_countries)
        return random.choices(countries, weights=probs)[0]
    
    def _generate_studio_name(self) -> str:
        """Генерация уникального названия студии"""
        if not self.studio_templates:
            print("Нет шаблонов для названий студий!")
            return f"Studio_{len(self.generated_studio_names) + 1}"
        
        template = random.choice(self.studio_templates)
        studio_name = template
        
        word_options = ['adjectives', 'nouns', 'prefixes', 'locations', 'colors']
        if '{word1}' in studio_name:
            word1_category = random.choice(word_options)
            studio_name = studio_name.replace('{word1}', 
                self._get_random_word(word1_category))
        if '{word2}' in studio_name:
            word2_category = random.choice(word_options)
            studio_name = studio_name.replace('{word2}', 
                self._get_random_word(word2_category))
        if '{suffix}' in studio_name:
            suffix = random.choice(self.words.get('studio_suffixes', ['Games']))
            studio_name = studio_name.replace('{suffix}', suffix)
        
        studio_name = studio_name.title()
        if random.random() < 0.3:
            suffixes = ['Inc.', 'LLC', 'Corp.', 'Ltd.']
            studio_name = f"{studio_name} {random.choice(suffixes)}"
        
        while studio_name in self.generated_studio_names:
            studio_name += str(random.randint(1, 9))

        self.generated_studio_names.add(studio_name)
        return studio_name
    
    def _title_case(self, text: str) -> str:
        """Приведение к заглавным буквам как в названиях"""
        small_words = {'the', 'of', 'and', 'to', 'in', 'for', 'on', 'at', 'by'}
        
        words = text.split()
        result = []
        
        for i, word in enumerate(words):
            if i == 0 or word.lower() not in small_words:
                result.append(word.title())
            else:
                result.append(word.lower())
        
        return ' '.join(result)
    
    def _generate_game_title(self) -> str:
        """Генерация уникального названия игры"""
        if not self.game_templates:
            print("Нет шаблонов для названий игр!")
            return f"Game_{len(self.generated_game_titles) + 1}"
        
        template = random.choice(self.game_templates)
        title = template
        
        replacements = {
            '{adjective}': self._get_random_word('adjectives'),
            '{noun}': self._get_random_word('nouns'),
            '{mythical}': self._get_random_word('mythical_creatures'),
            '{color}': self._get_random_word('colors'),
            '{prefix}': self._get_random_word('prefixes'),
            '{verb}': self._get_random_word('verbs') if 'verbs' in self.words else 'Rising',
            '{location}': self._get_random_word('locations'),
            '{subtitle}': self._get_random_word('subtitles'),
            '{roman_numeral}': self._get_random_word('roman_numerals'),
            '{plural_noun}': self._get_random_word('nouns') + 's'
        }
        
        for placeholder, replacement in replacements.items():
            if placeholder in title:
                title = title.replace(placeholder, replacement)
        
        title = self._title_case(title)
        
        if random.random() < 0.1 and 'edition_suffixes' in self.words:
            edition = random.choice(self.words['edition_suffixes'])
            if edition:
                title = f"{title} - {edition}"
        
        while title in self.generated_game_titles:
            title += str(random.randint(1, 9))

        self.generated_game_titles.add(title)
        return title
    
    def _generate_genre(self) -> tuple:
        """Генерация жанра и тегов"""
        genres, probs = zip(*self.genre_distribution)
        genre_main = random.choices(genres, weights=probs)[0]
        
        base_tags = self.genre_tags.get(genre_main, ['indie', 'casual'])
        
        additional_tags = ['multiplayer', 'singleplayer', 'co-op', 'online', 
                          'offline', 'vr', 'controller-friendly', 'moddable']
        
        num_tags = random.randint(2, 4)
        selected_tags = random.sample(base_tags, min(len(base_tags), num_tags))
        
        if additional_tags and random.random() < 0.7 and len(selected_tags) < 4:
            selected_tags.append(random.choice(additional_tags))
        
        return genre_main, list(set(selected_tags))
    
    def _generate_release_date(self, current_date: datetime, developer_id: int = None) -> str:
        """Генерация даты релиза игры с учетом последней игры разработчика"""
        now = current_date
        
        if developer_id is not None and developer_id in self.last_game_dates:
            last_release = self.last_game_dates[developer_id]
            base_interval = timedelta(days=730)
            variation = timedelta(days=random.randint(-90, 90)) 
            new_release = last_release + base_interval + variation
            
            max_future_date = now + timedelta(days=180)
            if new_release > max_future_date:
                new_release = max_future_date
        else:
            new_release = now
        
        if developer_id is not None:
            self.last_game_dates[developer_id] = new_release
        
        return new_release.strftime('%Y-%m-%d')
    
    def create_user(self, current_date: Optional[datetime] = None) -> Dict:
        """
        Создает полные данные пользователя для таблицы users
        """
        if current_date is None:
            current_date = datetime.now()
        
        username = self._generate_username()
        email = self.fake.email()
        while email in self.generated_emails:
            email = self.fake.email()
        self.generated_emails.add(email)
        country_code, region = self._get_random_country_region()
        registration_date = current_date.strftime('%Y-%m-%d %H:%M:%S')
        user_data = {
            'user_id': self.user_id_counter,
            'username': username,
            'email': email,
            'country_code': country_code,
            'region': region,
            'registration_date': registration_date,
            'total_spent': 0.00,
        }
        self.user_id_counter += 1
        return user_data
    
    def create_users_batch(self, count: int, current_date: Optional[datetime] = None) -> List[Dict]:
        """Создает несколько пользователей"""
        if current_date is None:
            current_date = datetime.now()
            
        users = []
        for _ in range(count):
            users.append(self.create_user(current_date))
        return users
    
    def create_developer(self, current_date: Optional[datetime] = None) -> Dict:
        """
        Создает полные данные разработчика для таблицы developers
        """
        if current_date is None:
            current_date = datetime.now()
        
        studio_name = self._generate_studio_name()
        contact_email = self._generate_developer_email(studio_name)
        country_code = self._get_random_country()
        
        developer_data = {
            'developer_id': self.developer_id_counter,
            'studio_name': studio_name,
            'country_code': country_code,
            'foundation_year': current_date.year,
            'total_revenue': 0.00,
            'contact_email': contact_email
        }
        self.developer_id_counter += 1
        return developer_data
    
    def create_developers_batch(self, count: int, current_date: Optional[datetime] = None) -> List[Dict]:
        """Создает несколько разработчиков"""
        if current_date is None:
            current_date = datetime.now()
            
        developers = []
        for _ in range(count):
            developers.append(self.create_developer(current_date))
        return developers
    
    def create_game(self, current_date: Optional[datetime] = None, developer_id: int = -1) -> Dict:
        """
        Создает полные данные игры для таблицы games
        Args:
            current_date: текущая дата в симуляции
            developer_id: ID разработчика (по умолчанию -1)
        """
        if current_date is None:
            current_date = datetime.now()
            
        if developer_id == -1:
            try:
                dev_id = developer_repo.get_random_developer_id()
                if dev_id is not None:
                    developer_id = dev_id
                else:
                    developer_id = self.developer_id_counter - 1 if self.developer_id_counter > 0 else 0
            except Exception as e:
                print(f"Ошибка при получении разработчика из БД: {e}")
                developer_id = 0
        
        title = self._generate_game_title()
        types, probs = zip(*self.monetization_distribution)
        monetization_type = random.choices(types, weights=probs)[0]
        price = round(max(random.normalvariate(15, 12), 1), 2) if monetization_type == "paid" else 0.00
        genre_main, genre_tags = self._generate_genre()
        ratings, probs = zip(*self.age_rating_distribution)
        age_rating = random.choices(ratings, weights=probs)[0]
        release_date = self._generate_release_date(current_date, developer_id)
        
        game_data = {
            'game_id': self.game_id_counter,
            'title': title,
            'developer_id': developer_id,
            'release_date': release_date,
            'base_price': price,
            'current_price': price,
            'monetization_type': monetization_type,
            'genre_main': genre_main,
            'genre_tags': json.dumps(genre_tags),
            'age_rating': age_rating,
            'total_purchases': 0,
            'is_active': True
        }
        
        self.game_id_counter += 1
        return game_data
    
    def create_games_batch(self, count: int, current_date: Optional[datetime] = None, developer_id: int = -1) -> List[Dict]:
        """Создает несколько игр"""
        if current_date is None:
            current_date = datetime.now()
            
        games = []
        for _ in range(count):
            games.append(self.create_game(current_date, developer_id))
        return games