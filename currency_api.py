import os
import sys
from pathlib import Path
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, exc
from dotenv import load_dotenv
import logging
from io import StringIO

# Фикс кодировки для Windows
sys.stdout.reconfigure(encoding='utf-8')

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('currency_app.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
env_path = Path(__file__).parent / '.env'
load_dotenv(env_path)

def get_db_engine():
    """Создает подключение к PostgreSQL с обработкой ошибок"""
    try:
        # Используем переменные из .env
        conn_str = (
            f"postgresql+pg8000://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
            f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
        
        engine = create_engine(conn_str)
        
        # Проверяем подключение
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            logger.info("Подключение к PostgreSQL успешно")
            return engine
            
    except exc.OperationalError as e:
        logger.error(f"Ошибка подключения (сервер недоступен): {str(e)}")
    except exc.ProgrammingError as e:
        if "password authentication failed" in str(e):
            logger.error("Неверный логин/пароль PostgreSQL")
        elif "database" in str(e) and "does not exist" in str(e):
            logger.error("База данных не существует. Создайте базу данных currency_rates")
        else:
            logger.error(f"SQL ошибка: {str(e)}")
    except Exception as e:
        logger.error(f"Неизвестная ошибка: {str(e)}", exc_info=True)
    
    return None

def get_last_loaded_date(engine):
    """Получаем последнюю дату из базы"""
    try:
        with engine.connect() as conn:
            # Явно указываем имя столбца с учетом регистра
            result = conn.execute(text("SELECT MAX(\"Date\") FROM public.currency_rates"))
            last_date = result.scalar()
            return last_date if last_date else datetime.now().date() - timedelta(days=1)
    except Exception as e:
        logger.error(f"Ошибка получения последней даты: {str(e)}")
        return datetime.now().date() - timedelta(days=1)

def get_cbr_rates(date_req=None):
    """Получение курсов валют с ЦБ РФ"""
    try:
        date_str = date_req.strftime('%d/%m/%Y') if date_req else datetime.now().strftime('%d/%m/%Y')
        url = f'https://www.cbr.ru/scripts/XML_daily.asp?date_req={date_str}'
        
        # Отключаем проверку SSL (для теста) или укажите правильный путь к сертификатам
        response = requests.get(url, timeout=10, verify=False)
        response.raise_for_status()
        
        with StringIO(response.text) as xml_data:
            df = pd.read_xml(xml_data)
        
        # Обработка данных
        df['Date'] = datetime.strptime(date_str, '%d/%m/%Y').date()
        df['Value'] = df['Value'].str.replace(',', '.').astype(float)
        df['Nominal'] = df['Nominal'].astype(int)
        df['Rate'] = df['Value'] / df['Nominal']
        
        return df.rename(columns={
            'CharCode': 'CurrencyCode',
            'Name': 'CurrencyName'
        })[['Date', 'CurrencyCode', 'CurrencyName', 'Nominal', 'Value', 'Rate']]
    
    except Exception as e:
        logger.error(f"Ошибка получения данных ЦБ: {str(e)}")
        return pd.DataFrame()

def save_data(df, engine):
    """Сохранение данных в PostgreSQL или CSV"""
    if engine is not None:
        try:
            # Сохраняем данные с явным указанием схемы
            df.to_sql(
                'currency_rates',
                engine,
                schema='public',
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info(f"Данные сохранены в PostgreSQL ({len(df)} записей)")
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения в PostgreSQL: {str(e)}")
            return False
    
    # Резервное сохранение в CSV
    csv_path = Path(__file__).parent / 'currency_backup.csv'
    try:
        df.to_csv(csv_path, mode='a', header=not csv_path.exists(), index=False, encoding='utf-8')
        logger.info(f"Данные сохранены в CSV: {csv_path}")
        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения в CSV: {str(e)}")
        return False
    
def main():
    try:
        engine = get_db_engine()
        if engine is None:
            logger.error("Не удалось подключиться к БД, используем резервное сохранение")
            
        # Получаем последнюю загруженную дату или вчерашнюю дату, если БД пуста
        last_date = get_last_loaded_date(engine) if engine else datetime.now().date() - timedelta(days=1)
        start_date = last_date + timedelta(days=1)
        end_date = datetime.now().date()
        
        # Если новых данных нет (уже загружено до сегодня)
        if start_date > end_date:
            logger.info(f"Нет новых данных для загрузки (последняя дата: {last_date})")
            return
            
        logger.info(f"Загрузка данных с {start_date} по {end_date}")
        
        # Собираем данные только за новые даты
        dates = pd.date_range(start_date, end_date)
        all_data = []
        
        for date in dates:
            df = get_cbr_rates(date)
            if not df.empty:
                all_data.append(df)
                logger.info(f"Получены данные за {date.date()}")
        
        if not all_data:
            logger.warning("Нет новых данных для сохранения")
            return
            
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Сохраняем данные
        save_data(final_df, engine)
        
    except Exception as e:
        logger.critical(f"Критическая ошибка: {str(e)}", exc_info=True)

def check_data_exists(engine, date):
    """Проверяет, есть ли данные за указанную дату"""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT 1 FROM public.currency_rates WHERE date = :date LIMIT 1"),
                {'date': date}
            )
            return result.scalar() is not None
    except Exception:
        return False
    
if __name__ == "__main__":
    main()