import os
import sys
import time
from pathlib import Path
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging
from io import StringIO
import urllib3

# Отключаем предупреждения SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('load_historical_2000_2015.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
env_path = Path(__file__).parent / '.env'
load_dotenv(env_path)

def get_db_engine():
    """Создает подключение к PostgreSQL"""
    try:
        conn_str = (
            f"postgresql+pg8000://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
            f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Подключение к PostgreSQL успешно")
        return engine
    except Exception as e:
        logger.error(f"Ошибка подключения: {str(e)}")
        return None

def get_cbr_rates(date_req):
    """Получение курсов валют с ЦБ РФ за указанную дату"""
    try:
        date_str = date_req.strftime('%d/%m/%Y')
        url = f'https://www.cbr.ru/scripts/XML_daily.asp?date_req={date_str}'
        
        response = requests.get(url, timeout=10, verify=False)
        response.raise_for_status()
        
        with StringIO(response.text) as xml_data:
            df = pd.read_xml(xml_data)
        
        # Обработка данных
        df['Date'] = date_req.date()
        df['Value'] = df['Value'].str.replace(',', '.').astype(float)
        df['Nominal'] = df['Nominal'].astype(int)
        df['Rate'] = df['Value'] / df['Nominal']
        
        return df.rename(columns={
            'CharCode': 'CurrencyCode',
            'Name': 'CurrencyName'
        })[['Date', 'CurrencyCode', 'CurrencyName', 'Nominal', 'Value', 'Rate']]
    
    except Exception as e:
        logger.error(f"Ошибка получения данных за {date_req.date()}: {str(e)}")
        return pd.DataFrame()

def save_data(df, engine):
    """Сохранение данных в PostgreSQL"""
    try:
        df.to_sql(
            'currency_rates',
            engine,
            schema='public',
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        logger.info(f"Сохранено {len(df)} записей за {df['Date'].iloc[0]}")
        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения: {str(e)}")
        return False

def main():
    engine = get_db_engine()
    if not engine:
        sys.exit(1)
    
    # Жестко задаем период: 2000-01-02 - 2015-06-25
    start_date = datetime(2000, 1, 2).date()
    end_date = datetime(2015, 6, 24).date()
    
    logger.info(f"Начинаем загрузку данных с {start_date} по {end_date}")
    
    # Генерируем все даты для загрузки
    dates = pd.date_range(start_date, end_date, freq='D')
    
    for date in dates:
        df = get_cbr_rates(date)
        if not df.empty:
            if not save_data(df, engine):
                logger.warning(f"Пропуск данных за {date.date()} из-за ошибки сохранения")
        else:
            logger.warning(f"Нет данных за {date.date()}")
        
        # Пауза между запросами
        time.sleep(0.5)  # Уменьшил паузу для ускорения
    
    logger.info("Загрузка исторических данных завершена")

if __name__ == "__main__":
    main()