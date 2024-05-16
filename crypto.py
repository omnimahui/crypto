# -*- coding: utf-8 -*-
# Download crypto quote data in one minute level from multiple exchange using async approach.
#
import asyncio
from pprint import pprint
import os
import sys
import time
import re
import pandas as pd
from datetime import date
import contextvars
from settings import  OKX_APIKEY, OKX_SECRET, OKX_PASSWORD


import ccxt.async_support as ccxt  # noqa: E402
# or
# import ccxtpro as ccxt


print('CCXT Version:', ccxt.__version__)
CURRENCY_PATTERN = re.compile(r"(.*)\/USDT$")
exchange_ctxvar = contextvars.ContextVar('exchange')
df_total_ctxvar = contextvars.ContextVar('df_total')
exchange_name_ctxvar = contextvars.ContextVar('exchange_name')
symbol_ctxvar = contextvars.ContextVar('symbol')
sleep_seconds_ctxvar =  contextvars.ContextVar('sleep_seconds')

def save(df_total, market_region='okx'):
    folder = os.path.dirname(os.path.realpath(__file__))+os.sep+f"test-ohlc-{market_region}"+os.sep
    if not os.path.exists(folder):
        os.mkdir(folder)

    fname = f"{folder}{df_total.index[0][0].strftime('%Y%m%d-%H%M%S')}.hdf"
    df_total.to_hdf(fname,key='ohlc',mode="a",format="t")   

def get_exchange(exchange_name):
    if exchange_name == 'okx':
        exchange = ccxt.okx({
            'apiKey': OKX_APIKEY,  # https://github.com/ccxt/ccxt/wiki/Manual#authentication
            'secret': OKX_SECRET,
            'password': OKX_PASSWORD,
            'options': {
                'defaultType': 'spot',
            },
        })
    elif  exchange_name == 'kraken':
        exchange = ccxt.kraken()
    elif  exchange_name == 'coinbase':
        exchange = ccxt.coinbase({'rateLimit': 1000})
    elif  exchange_name == 'gemini':
        exchange = ccxt.gemini()             
    
    return exchange
    
async def download_crypto_ohlc(exchange_name='okx', interval='1m', sleep_seconds='3600'):
    exchange_ctxvar.set(get_exchange(exchange_name))
    exchange_name_ctxvar.set(exchange_name)
    sleep_seconds_ctxvar.set(sleep_seconds)
    try:
        markets = await exchange_ctxvar.get().load_markets()
        exchange_ctxvar.get().verbose = False  # uncomment for debugging
        #futures_balance = await exchange.fetch_balance()
        #pprint(futures_balance)
        df_total_ctxvar.set(pd.DataFrame())
        if exchange_ctxvar.get().has['fetchOHLCV']:
            while True:
                for symbol in exchange_ctxvar.get().markets:
                    symbol_ctxvar.set(symbol)
                    m = CURRENCY_PATTERN.search(symbol_ctxvar.get())
                    if m:
                        ticker = m.group(1)
                        await asyncio.sleep (exchange_ctxvar.get().rateLimit / 1000) # time.sleep wants seconds
                        ohlc =  await exchange_ctxvar.get().fetch_ohlcv(symbol_ctxvar.get(), interval)
                        df = pd.DataFrame(ohlc, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
                        df['Date'] = pd.to_datetime(df['Date']*1000000)
                        df['Symbol'] = ticker
                        df.set_index(['Date', 'Symbol'], inplace=True)
                        df_total = pd.concat([df_total_ctxvar.get(), df], axis=0)
                        df_total = df_total[~df_total.index.duplicated()]
                        print (f"{exchange_name_ctxvar.get()} {ticker} {df_total.index[0][0]} to {df_total.index[-1][0]} done")
                        df_total_ctxvar.set(df_total)
                
                save(df_total_ctxvar.get(), market_region=exchange_name_ctxvar.get())
                df_total_ctxvar.set(pd.DataFrame())
                print (f"{exchange_name_ctxvar.get()} sleeping {sleep_seconds_ctxvar.get()} seconds")
                await asyncio.sleep(int(sleep_seconds_ctxvar.get()))
                
          
    except Exception as e:
        print (f"{e}")
    await exchange_ctxvar.get().close()


async def main():
    await asyncio.gather(
        download_crypto_ohlc(exchange_name='gemini', interval='1m', sleep_seconds='14400'),
        download_crypto_ohlc(exchange_name='coinbase', interval='1m', sleep_seconds='14400'),
        download_crypto_ohlc(exchange_name='okx', interval='1m', sleep_seconds='3600'), 
        download_crypto_ohlc(exchange_name='kraken', interval='1m', sleep_seconds='36000'),
    )    

asyncio.run(main())