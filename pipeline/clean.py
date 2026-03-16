import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

RAW_DIR       = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
PROCESSED_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed')
os.makedirs(PROCESSED_DIR, exist_ok=True)

# ── Equity prices ──────────────────────────────────────────────────────────────

def clean_equity(filename: str = 'equity_prices.csv') -> pd.DataFrame:
    print('  Cleaning equity prices...')
    path = os.path.join(RAW_DIR, filename)
    df = pd.read_csv(path, parse_dates=['date'])

    df = df.rename(columns={'ticker_name': 'name'})
    df = df[['date', 'name', 'ticker', 'open', 'high', 'low', 'close', 'volume']]
    df = df.dropna(subset=['close'])
    df = df.sort_values(['name', 'date']).reset_index(drop=True)

    path_out = os.path.join(PROCESSED_DIR, 'equity_prices_clean.csv')
    df.to_csv(path_out, index=False)
    print(f'  Saved equity_prices_clean.csv ({len(df)} rows)\n')
    return df


# ── KASE index ─────────────────────────────────────────────────────────────────

def clean_kase(filename: str = 'kase_index_clean.csv') -> pd.DataFrame:
    print('  Cleaning KASE index...')
    path = os.path.join(RAW_DIR, filename)
    df = pd.read_csv(path, parse_dates=['date'])

    df = df[['date', 'ticker_name', 'ticker', 'open', 'high', 'low', 'close', 'volume_kzt_m']]
    df = df.rename(columns={'ticker_name': 'name', 'volume_kzt_m': 'volume'})
    df = df.dropna(subset=['close'])
    df = df.sort_values('date').reset_index(drop=True)

    path_out = os.path.join(PROCESSED_DIR, 'kase_index_clean.csv')
    df.to_csv(path_out, index=False)
    print(f'  Saved kase_index_clean.csv ({len(df)} rows)\n')
    return df


# ── FX rates ───────────────────────────────────────────────────────────────────

def clean_fx(filename: str = 'fx_rates.csv') -> pd.DataFrame:
    print('  Cleaning FX rates...')
    path = os.path.join(RAW_DIR, filename)
    df = pd.read_csv(path, parse_dates=['date'])

    df = df.rename(columns={'ticker_name': 'currency_pair'})
    df = df[['date', 'currency_pair', 'close']]
    df = df.rename(columns={'close': 'rate'})

    country_map = {
        'USD_KZT': 'Kazakhstan',
        'USD_TRY': 'Turkey',
        'USD_PLN': 'Poland',
        'USD_RUB': 'Russia',
    }
    df['country'] = df['currency_pair'].map(country_map)
    df = df.dropna(subset=['rate'])
    df = df.sort_values(['currency_pair', 'date']).reset_index(drop=True)

    path_out = os.path.join(PROCESSED_DIR, 'fx_rates_clean.csv')
    df.to_csv(path_out, index=False)
    print(f'  Saved fx_rates_clean.csv ({len(df)} rows)\n')
    return df


# ── Macro indicators ───────────────────────────────────────────────────────────

def clean_macro(filename: str = 'macro_indicators.csv') -> pd.DataFrame:
    print('  Cleaning macro indicators...')
    path = os.path.join(RAW_DIR, filename)
    df = pd.read_csv(path)

    df = df.dropna(subset=['value'])
    df = df.sort_values(['country', 'indicator_code', 'year']).reset_index(drop=True)

    path_out = os.path.join(PROCESSED_DIR, 'macro_indicators_clean.csv')
    df.to_csv(path_out, index=False)
    print(f'  Saved macro_indicators_clean.csv ({len(df)} rows)\n')
    return df


# ── Derived metrics ────────────────────────────────────────────────────────────

def compute_derived(equity_df: pd.DataFrame, kase_df: pd.DataFrame) -> pd.DataFrame:
    print('  Computing derived metrics...')

    # combine equity and KASE into one dataframe
    combined = pd.concat([equity_df, kase_df], ignore_index=True)
    combined = combined.sort_values(['name', 'date']).reset_index(drop=True)

    results = []
    for name, group in combined.groupby('name'):
        group = group.sort_values('date').copy()

        # daily return
        group['daily_return'] = group['close'].pct_change()

        # 30 and 90 day rolling volatility (annualized)
        group['rolling_vol_30'] = group['daily_return'].rolling(30).std() * np.sqrt(252)
        group['rolling_vol_90'] = group['daily_return'].rolling(90).std() * np.sqrt(252)

        # 30 day cumulative return
        group['rolling_ret_30'] = group['close'].pct_change(30)

        # drawdown from rolling peak
        group['peak']     = group['close'].cummax()
        group['drawdown'] = (group['close'] - group['peak']) / group['peak']
        group = group.drop(columns=['peak'])

        results.append(group)

    df = pd.concat(results, ignore_index=True)
    df = df[['date', 'name', 'ticker', 'daily_return', 'rolling_vol_30',
             'rolling_vol_90', 'rolling_ret_30', 'drawdown']]
    df = df.dropna(subset=['daily_return'])

    path_out = os.path.join(PROCESSED_DIR, 'derived_metrics_clean.csv')
    df.to_csv(path_out, index=False)
    print(f'  Saved derived_metrics_clean.csv ({len(df)} rows)\n')
    return df


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print('=== Cleaning equity prices ===')
    equity_df = clean_equity()

    print('=== Cleaning KASE index ===')
    kase_df = clean_kase()

    print('=== Cleaning FX rates ===')
    clean_fx()

    print('=== Cleaning macro indicators ===')
    clean_macro()

    print('=== Computing derived metrics ===')
    compute_derived(equity_df, kase_df)

    print('Done. Check data/processed/ for output files.')