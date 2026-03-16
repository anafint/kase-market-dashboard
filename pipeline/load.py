import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

PROCESSED_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed')

# ── Database connection ────────────────────────────────────────────────────────

def get_engine():
    user     = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host     = os.getenv('DB_HOST')
    port     = os.getenv('DB_PORT')
    name     = os.getenv('DB_NAME')
    url      = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}'
    return create_engine(url)


# ── Exchanges reference table ──────────────────────────────────────────────────

EXCHANGES = [
    {'ticker': '^GSPC',      'name': 'S&P 500',          'country': 'United States', 'region': 'North America',  'category': 'equity_index'},
    {'ticker': 'XU100.IS',   'name': 'Borsa Istanbul',   'country': 'Turkey',        'region': 'Eastern Europe', 'category': 'equity_index'},
    {'ticker': 'IMOEX.ME',   'name': 'MOEX Russia',      'country': 'Russia',        'region': 'Eastern Europe', 'category': 'equity_index'},
    {'ticker': 'BZ=F',       'name': 'Brent Crude Oil',  'country': 'Global',        'region': 'Global',         'category': 'commodity'},
    {'ticker': 'WIG20',      'name': 'WIG20 Poland',     'country': 'Poland',        'region': 'Eastern Europe', 'category': 'equity_index'},
    {'ticker': 'KASE_INDEX', 'name': 'KASE Index',       'country': 'Kazakhstan',    'region': 'Central Asia',   'category': 'equity_index'},
    {'ticker': 'KZT=X',      'name': 'USD/KZT',          'country': 'Kazakhstan',    'region': 'Central Asia',   'category': 'fx'},
    {'ticker': 'TRY=X',      'name': 'USD/TRY',          'country': 'Turkey',        'region': 'Eastern Europe', 'category': 'fx'},
    {'ticker': 'PLN=X',      'name': 'USD/PLN',          'country': 'Poland',        'region': 'Eastern Europe', 'category': 'fx'},
    {'ticker': 'RUB=X',      'name': 'USD/RUB',          'country': 'Russia',        'region': 'Eastern Europe', 'category': 'fx'},
]

def load_exchanges(engine) -> pd.DataFrame:
    print('  Loading exchanges reference table...')
    df = pd.DataFrame(EXCHANGES)
    with engine.connect() as conn:
        conn.execute(text('DELETE FROM derived_metrics'))
        conn.execute(text('DELETE FROM market_prices'))
        conn.execute(text('DELETE FROM exchanges'))
        conn.execute(text('ALTER SEQUENCE exchanges_exchange_id_seq RESTART WITH 1'))
        conn.commit()
    df.to_sql('exchanges', engine, if_exists='append', index=False)
    
    # read back actual IDs from database
    exchanges_df = pd.read_sql('SELECT * FROM exchanges', engine)
    print(f'  Inserted {len(exchanges_df)} exchanges\n')
    return exchanges_df


# ── Market prices ──────────────────────────────────────────────────────────────

def load_market_prices(engine, exchanges_df: pd.DataFrame):
    print('  Loading market prices...')

    # build ticker -> exchange_id from actual database IDs
    ticker_map = dict(zip(exchanges_df['ticker'], exchanges_df['exchange_id']))

    equity = pd.read_csv(os.path.join(PROCESSED_DIR, 'equity_prices_clean.csv'), parse_dates=['date'])
    kase   = pd.read_csv(os.path.join(PROCESSED_DIR, 'kase_index_clean.csv'),    parse_dates=['date'])
    wig20  = pd.read_csv(os.path.join(PROCESSED_DIR, 'wig20_clean.csv'),         parse_dates=['date'])
    df     = pd.concat([equity, kase, wig20], ignore_index=True)

    df['exchange_id'] = df['ticker'].map(ticker_map)
    df = df[['exchange_id', 'date', 'open', 'high', 'low', 'close', 'volume']]
    df = df.dropna(subset=['exchange_id', 'close'])
    df['exchange_id'] = df['exchange_id'].astype(int)

    with engine.connect() as conn:
        conn.execute(text('DELETE FROM market_prices'))
        conn.commit()
    df.to_sql('market_prices', engine, if_exists='append', index=False)
    print(f'  Inserted {len(df)} price rows\n')


# ── FX rates ───────────────────────────────────────────────────────────────────

def load_fx_rates(engine):
    print('  Loading FX rates...')
    df = pd.read_csv(os.path.join(PROCESSED_DIR, 'fx_rates_clean.csv'), parse_dates=['date'])
    df = df[['currency_pair', 'country', 'date', 'rate']]

    with engine.connect() as conn:
        conn.execute(text('DELETE FROM fx_rates'))
        conn.commit()
    df.to_sql('fx_rates', engine, if_exists='append', index=False)
    print(f'  Inserted {len(df)} FX rows\n')


# ── Macro indicators ───────────────────────────────────────────────────────────

def load_macro(engine):
    print('  Loading macro indicators...')
    df = pd.read_csv(os.path.join(PROCESSED_DIR, 'macro_indicators_clean.csv'))
    df = df[['country', 'indicator_code', 'indicator_name', 'year', 'value']]

    with engine.connect() as conn:
        conn.execute(text('DELETE FROM macro_indicators'))
        conn.commit()
    df.to_sql('macro_indicators', engine, if_exists='append', index=False)
    print(f'  Inserted {len(df)} macro rows\n')


# ── Derived metrics ────────────────────────────────────────────────────────────

def load_derived(engine, exchanges_df: pd.DataFrame):
    print('  Loading derived metrics...')
    ticker_map = dict(zip(exchanges_df['ticker'], exchanges_df['exchange_id']))

    df = pd.read_csv(os.path.join(PROCESSED_DIR, 'derived_metrics_clean.csv'), parse_dates=['date'])
    df['exchange_id'] = df['ticker'].map(ticker_map)
    df = df[['exchange_id', 'date', 'daily_return', 'rolling_vol_30',
             'rolling_vol_90', 'rolling_ret_30', 'drawdown']]
    df = df.dropna(subset=['exchange_id'])
    df['exchange_id'] = df['exchange_id'].astype(int)

    with engine.connect() as conn:
        conn.execute(text('DELETE FROM derived_metrics'))
        conn.commit()
    df.to_sql('derived_metrics', engine, if_exists='append', index=False)
    print(f'  Inserted {len(df)} derived metric rows\n')


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    engine = get_engine()
    print('=== Loading exchanges ===')
    exchanges_df = load_exchanges(engine)

    print('=== Loading market prices ===')
    load_market_prices(engine, exchanges_df)

    print('=== Loading FX rates ===')
    load_fx_rates(engine)

    print('=== Loading macro indicators ===')
    load_macro(engine)

    print('=== Loading derived metrics ===')
    load_derived(engine, exchanges_df)

    print('Done. All data loaded into PostgreSQL.')