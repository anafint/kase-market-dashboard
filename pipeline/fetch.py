import os
import yfinance as yf
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
os.makedirs(RAW_DIR, exist_ok=True)

START_DATE = '2020-01-01'
END_DATE   = pd.Timestamp.today().strftime('%Y-%m-%d')

# ── Yahoo Finance ──────────────────────────────────────────────────────────────

EQUITY_TICKERS = {
    'SP500': '^GSPC',
    'XU100': 'XU100.IS',
    'IMOEX': 'IMOEX.ME',
    'BRENT': 'BZ=F',
    'WIG20': 'WIG20.WA'   # Warsaw top 20 index — works on Yahoo
}

FX_TICKERS = {
    'USD_KZT': 'KZT=X',
    'USD_TRY': 'TRY=X',
    'USD_PLN': 'PLN=X',
    'USD_RUB': 'RUB=X',
}

def fetch_yahoo(tickers: dict, label: str) -> pd.DataFrame:
    frames = []
    for name, ticker in tickers.items():
        print(f'  Fetching {name} ({ticker})...')
        try:
            df = yf.download(ticker, start=START_DATE, end=END_DATE, auto_adjust=True, progress=False)
            if df.empty:
                print(f'  WARNING: no data returned for {ticker}')
                continue
            df = df[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
            df.columns = ['open', 'high', 'low', 'close', 'volume']
            df.index.name = 'date'
            df['ticker_name'] = name
            df['ticker']      = ticker
            frames.append(df.reset_index())
        except Exception as e:
            print(f'  ERROR fetching {ticker}: {e}')

    if not frames:
        print(f'  No data fetched for {label}')
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    path = os.path.join(RAW_DIR, f'{label}.csv')
    combined.to_csv(path, index=False)
    print(f'  Saved {label}.csv ({len(combined)} rows)\n')
    return combined

def load_kase_excel(filename: str = 'Index_KASE_260316.xlsx') -> pd.DataFrame:
    path = os.path.join(RAW_DIR, filename)
    print(f'  Loading KASE index from {filename}...')
    try:
        df = pd.read_excel(path, skiprows=1)
        df.columns = ['date', 'open', 'high', 'low', 'close', 'volume_kzt_m', 'volume_usd_th']
        df['date'] = pd.to_datetime(df['date'], format='%d.%m.%y')
        df = df.dropna(subset=['close'])
        df['ticker_name'] = 'KASE'
        df['ticker'] = 'KASE_INDEX'
        df = df.sort_values('date').reset_index(drop=True)
        path_out = os.path.join(RAW_DIR, 'kase_index_clean.csv')
        df.to_csv(path_out, index=False)
        print(f'  Saved kase_index_clean.csv ({len(df)} rows)\n')
        return df
    except Exception as e:
        print(f'  ERROR loading KASE excel: {e}')
        return pd.DataFrame()


# ── World Bank ─────────────────────────────────────────────────────────────────

COUNTRIES = {
    'Kazakhstan':   'KZ',
    'Turkey':       'TR',
    'Poland':       'PL',
    'Russia':       'RU',
    'UnitedStates': 'US',
}

INDICATORS = {
    'NY.GDP.MKTP.CD':        'GDP (current USD)',
    'FP.CPI.TOTL.ZG':        'Inflation rate (%)',
    'SL.UEM.TOTL.ZS':        'Unemployment rate (%)',
    'BX.KLT.DINV.WD.GD.ZS': 'FDI net inflows (% of GDP)',
}

def fetch_world_bank(countries: dict, indicators: dict) -> pd.DataFrame:
    frames = []
    for country_name, iso2 in countries.items():
        for code, name in indicators.items():
            url = (
                f'https://api.worldbank.org/v2/country/{iso2}/indicator/{code}'
                f'?format=json&per_page=20&mrv=10'
            )
            print(f'  Fetching {name} for {country_name}...')
            try:
                resp = requests.get(url, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                if len(data) < 2 or not data[1]:
                    print(f'  WARNING: no data for {code} / {country_name}')
                    continue
                rows = [
                    {
                        'country':        country_name,
                        'indicator_code': code,
                        'indicator_name': name,
                        'year':           int(entry['date']),
                        'value':          entry['value'],
                    }
                    for entry in data[1]
                    if entry['value'] is not None
                ]
                frames.append(pd.DataFrame(rows))
            except Exception as e:
                print(f'  ERROR fetching {code} / {country_name}: {e}')

    if not frames:
        print('  No World Bank data fetched')
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    path = os.path.join(RAW_DIR, 'macro_indicators.csv')
    combined.to_csv(path, index=False)
    print(f'  Saved macro_indicators.csv ({len(combined)} rows)\n')
    return combined


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print('=== Fetching equity & commodity data ===')
    fetch_yahoo(EQUITY_TICKERS, 'equity_prices')

    print('=== Fetching FX data ===')
    fetch_yahoo(FX_TICKERS, 'fx_rates')

    print('=== Fetching World Bank macro data ===')
    fetch_world_bank(COUNTRIES, INDICATORS)

    print('=== Loading KASE index from Excel ===')
    load_kase_excel()

    print('Done. Check data/raw/ for output files.')