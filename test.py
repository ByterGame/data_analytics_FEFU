import psycopg2
import pandas as pd

def connect_to_postgres():
    connection_params = {
        'host': 'localhost',
        'port': 5432,
        'database': 'game_platform',
        'user': 'postgres',
        'password': 'postgres'
    }
    
    print(f"Попытка подключения")
    print(f"Хост: {connection_params['host']}:{connection_params['port']}")
    print(f"База: {connection_params['database']}")
    print(f"Пользователь: {connection_params['user']}")
    
    try:
        conn = psycopg2.connect(**connection_params)
        print("✅ Подключение успешно!")
        
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        print(f"Версия PostgreSQL: {version.split(',')[0]}")
        
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        
        tables = cursor.fetchall()
        print(f"Найдено таблиц: {len(tables)}")
        
        if tables:
            print("Таблицы:")
            for table in tables:
                print(f"- {table[0]}")
        
        cursor.close()
        return conn
        
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        return None

def main():
    conn = connect_to_postgres()
    if conn:
        conn.close()
        print(f"\n✅ Соединение закрыто")
    else:
        print("\n❌ Не удалось подключиться к базе данных")

if __name__ == "__main__":
    main()