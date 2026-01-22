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
);

CREATE TABLE IF NOT EXISTS developers (
    developer_id INTEGER PRIMARY KEY,
    studio_name VARCHAR(255) UNIQUE NOT NULL,
    country_code CHAR(2) NOT NULL,
    foundation_year INTEGER,
    total_revenue DECIMAL(12,2) DEFAULT 0.00,
    contact_email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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
);

CREATE TABLE IF NOT EXISTS user_library (
    user_game_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id),
    game_id INTEGER REFERENCES games(game_id),
    purchase_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, game_id)
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id),
    game_id INTEGER REFERENCES games(game_id),
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    amount DECIMAL(10,2),
    developer_revenue DECIMAL(10,2),
    platform_commission DECIMAL(10,2)
);

CREATE INDEX IF NOT EXISTS idx_users_country ON users(country_code);
CREATE INDEX IF NOT EXISTS idx_games_developer ON games(developer_id);
CREATE INDEX IF NOT EXISTS idx_games_genre ON games(genre_main);
CREATE INDEX IF NOT EXISTS idx_user_library_user ON user_library(user_id);
CREATE INDEX IF NOT EXISTS idx_user_library_game ON user_library(game_id);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date);

CREATE INDEX IF NOT EXISTS idx_users_id ON users(user_id);
CREATE INDEX IF NOT EXISTS idx_developers_id ON developers(developer_id);
CREATE INDEX IF NOT EXISTS idx_games_id ON games(game_id);