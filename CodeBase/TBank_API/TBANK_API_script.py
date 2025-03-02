# req pip install tinkoff-investments
from tinkoff.invest import Client
from API_KEYS import API_KEY

TOKEN = API_KEY

def get_figi(ticker):
    with Client(TOKEN) as client:
        instruments = client.instruments
        for method in ["currencies"]:
            for item in getattr(instruments, method)().instruments:
                if item.ticker == ticker:
                    return item.figi
    return None

if __name__ == "__main__":
    ticker = "CNYRUB_TOM"
    figi = get_figi(ticker)

    if figi:
        print(f"FIGI для {ticker}: {figi}")
    else:
        print("FIGI не найден")
        
        
import asyncio
import logging
import pandas as pd
from datetime import datetime, timezone
import nest_asyncio

from tinkoff.invest import CandleInterval
from tinkoff.invest.retrying.aio.client import AsyncRetryingClient
from tinkoff.invest.retrying.settings import RetryClientSettings
from tinkoff.invest.utils import now

nest_asyncio.apply()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler("logfile1.log")
formatter = logging.Formatter("%(asctime)s %(levelname)s:%(message)s")
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


retry_settings = RetryClientSettings(use_retry=True, max_retry_attempt=2)

start_date = datetime(2022, 3, 3, tzinfo=timezone.utc)

df = pd.DataFrame(columns=["currency", "price_rub", "timestamp"])

async def fetch_candles(client):
    global df
    async for candle in client.get_all_candles(
        figi='BBG0013HRTL0',
        from_=start_date,
        to=now(),
        interval=CandleInterval.CANDLE_INTERVAL_30_MIN,
    ):
        df = pd.concat([df, pd.DataFrame([{
            "currency": "CNY",
            "price_rub": candle.close.units + candle.close.nano / 1e9,
            "timestamp": int(candle.time.timestamp())
        }])], ignore_index=True)


async def main():
    async with AsyncRetryingClient(TOKEN, settings=retry_settings) as client:
        await fetch_candles(client)

    df.to_csv("cny_rub_dataset.csv", index=False)

if __name__ == "__main__":
    asyncio.run(main())



def get_uid(ticker):
    with Client(TOKEN) as client:
        instruments = client.instruments
        for method in ["currencies"]:
            for item in getattr(instruments, method)().instruments:
                if item.ticker == ticker:
                    return item.uid
    return None

if __name__ == "__main__":
    ticker = "GLDRUB_TOM"
    uid = get_uid(ticker)

    if uid:
        logger.info(f"UID для {ticker}: {uid}")
        print(f"UID для {ticker}: {uid}")
    else:
        logger.warning(f"UID для {ticker} не найден")
        print("UID не найден")
        
        
uid_gold='258e2b93-54e8-4f2d-ba3d-a507c47e3ae2'
def fetch_gold_prices(uid_gold):
    global df

    df_gold = pd.DataFrame()

    with Client(TOKEN) as client:
        for candle in client.get_all_candles(
            instrument_id=uid,
            from_=start_date,
            to=now(),
            interval=CandleInterval.CANDLE_INTERVAL_30_MIN,
        ):
            df_gold = pd.concat([df_gold, pd.DataFrame([{
                "timestamp": int(candle.time.timestamp()),
                "gold_price": candle.close.units + candle.close.nano / 1e9
            }])], ignore_index=True)

    df = df.merge(df_gold, on="timestamp", how="left")

    logger.info("Добавлены исторические данные о ценах на золото")

fetch_gold_prices(uid_gold)



def get_uid(ticker):
    with Client(TOKEN) as client:
        instruments = client.instruments
        for method in ["currencies"]:
            for item in getattr(instruments, method)().instruments:
                if item.ticker == ticker:
                    return item.uid
    return None

if __name__ == "__main__":
    ticker = "CNYRUB_TOM"
    uid = get_uid(ticker)

    if uid:
        logger.info(f"UID для {ticker}: {uid}")
        print(f"UID для {ticker}: {uid}")
    else:
        logger.warning(f"UID для {ticker} не найден")
        print("UID не найден")
        
        
uid_cny='4587ab1d-a9c9-4910-a0d6-86c7b9c42510'
def fetch_last_prices(figi, uid):
    global df
    with Client(TOKEN) as client:
        last_prices_response = client.market_data.get_last_prices(
            instrument_id=[uid_cny, uid_gold]
        )
        last_prices = {lp.instrument_uid: lp.price.units + lp.price.nano / 1e9 for lp in last_prices_response.last_prices}
        timestamp = int(last_prices_response.last_prices[0].time.timestamp())

        df_new = pd.DataFrame([{
            "currency": "CNY",
            "timestamp": timestamp,
            "price_rub": last_prices.get(uid_cny, None),
            "gold_price": last_prices.get(uid_gold, None)
        }])

        df = pd.concat([df, df_new], ignore_index=True)

        logger.info(f"Добавлена новая строка: CNY = {last_prices.get(uid_cny, None)} RUB, Gold = {last_prices.get(uid_gold, None)} RUB")

fetch_last_prices(uid_cny, uid_gold)



df.to_csv("/content/cny_rub_dataset.csv", index=False)
