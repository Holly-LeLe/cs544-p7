# price_generator.py
import datetime, random, time, os
from datetime import datetime, timedelta
random.seed(544)

def get_next_price_main():
    start_date = datetime.strptime("2000-01-01", "%Y-%m-%d")

    # Seed tickers with starting prices
    tickers = {
        "AAPL": 150.0,
        "MSFT": 120.0,
        "GOOG": 1100.0,
        "AMZN": 1800.0,
        "TSLA": 220.0,
        "NFLX": 300.0,
        "META": 200.0,
        "NVDA": 180.0,
        "I": 50.0,   # special test ticker
        "J": 10.0,   # special test ticker
    }

    i = 0
    while True:
        for ticker, price in tickers.items():
            # I and J are special tickers for testing; others follow a random walk
            if ticker == "I":
                new_price = 50.0  # fixed
            elif ticker == "J":
                new_price = (10.0 + i) % 20  # predictable, bounded change
            else:
                # Random walk with small daily drift and noise
                drift = 0.0005  # upward drift
                shock = random.gauss(0, 1.5)
                
                # to prevent synthetic price from overshooting
                anchor = 200.0
                mean_revert = 0.02 * (anchor - price)
                new_price = price * (1 + drift) + shock + mean_revert
                new_price = min(600.0, max(1.0, new_price))
                
            tickers[ticker] = new_price
            yield start_date.strftime("%Y-%m-%d"), float(new_price), ticker
        start_date += timedelta(days=1)
        i += 1

def get_next_price(delay_sec):
    if 'AUTOGRADER_DELAY_OVERRIDE_VAL' in os.environ:
        delay_sec = float(os.environ['AUTOGRADER_DELAY_OVERRIDE_VAL'])
    price_generator = get_next_price_main()
    while True:
        yield next(price_generator)
        time.sleep(delay_sec)

if __name__ == "__main__":
    # initialize python-mysql connection
    from sqlalchemy import create_engine, text
    project = os.environ.get('PROJECT', 'p7')
    mysql_host = f'{project}-mysql-1'
    engine = create_engine(f"mysql+mysqlconnector://root:abc@{mysql_host}:3306/CS544")
    conn = engine.connect()

    # Runs infinitely because price data is continuously generated
    for date, price, ticker in get_next_price(delay_sec=0.01):
        #print(date, price, ticker)

        query = text(f"""
                    INSERT INTO stock_prices (ticker, date, price)
                    VALUES ('{ticker}', '{date}', {price})
                """)
        conn.execute(query)
        conn.commit()
