import json
from urllib.request import Request, urlopen
import concurrent.futures
import logging
import os
import time
import sys

#this script downloads all json files with price data from bittrex

def market_summaries():
    url = Request('https://bittrex.com/api/v1.1/public/getmarketsummaries')
    result = json.loads(urlopen(url).read().decode())
    if result['success']:
        return result['result']

def load_url(coin, tickinterval):
    tries = 0
    while tries <= 3:
        tries += 1
        try:
            req = Request(f"https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName={coin['MarketName']}&tickInterval={tickinterval}")
            response = json.loads(urlopen(req, timeout=7).read().decode())
            if response['success']:
                logging.debug(f"candled {coin['MarketName']}")
                break
            else:
                logging.info(f"error {coin['MarketName']} {response['message']}")
                break
        except:
            logging.info(f"timeout for {coin['MarketName']} retrying in a few sec")
            time.sleep(10)
            continue

    if response['success']:
        return response['result']

if __name__ == '__main__':
    from pathlib import Path
    try:
    	markets = market_summaries()
    except Exception as e:
        print("cant get market summaries")
        exit(1)
    try:
        candleinterval = sys.argv[1]
        candlepath = f"/opt/bittrex/{candleinterval}"
        if not Path(candlepath).exists():
            os.mkdir(candlepath)
    except IndexError:
        for c in ['OneMin', 'FiveMin', 'ThirtyMin', 'Hour', 'Day']:
            if not Path(c).exists():
                os.mkdir(c)

    candles = []
    # We can use a with statement to ensure threads are cleaned up promptly
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Start the load operations and mark each future with its URL
        try:
            future_to_url = {executor.submit(load_url, url, sys.argv[1]): url for url in markets}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    data = future.result()
                    with open(f"/opt/bittrex/{sys.argv[1]}/{url['MarketName']}.json", 'w') as f:
                        json.dump(data, f)
                except Exception as exc:
                    print('%r generated an exception: %s' % (url['MarketName'], exc))
        except IndexError:
            for c in ['OneMin', 'FiveMin', 'ThirtyMin', 'Hour', 'Day']:
                future_to_url = {executor.submit(load_url, url, c): url for url in markets}
                for future in concurrent.futures.as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        data = future.result()
                        with open(f"{c}/{url['MarketName']}.json", 'w') as f:
                            json.dump(data, f)
                    except Exception as exc:
                        print('%r generated an exception: %s' % (url['MarketName'], exc))
