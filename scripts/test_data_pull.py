import os
import time
import pandas as pd
from tiingo import TiingoClient
from datetime import datetime, date
from dotenv import load_dotenv

load_dotenv()

TIINGO_API_KEY = os.getenv("TIINGO_API_KEY")

TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "META", "TSLA", "AMD", "INTC", "ORCL",
]

START_DATE = "2015-01-01"
END_DATE   = datetime.today().strftime("%Y-%m-%d")

config = {
    "session": True,
    "api_key": TIINGO_API_KEY,
}
client = TiingoClient(config)


def fetch_daily_prices(ticker: str) -> pd.DataFrame:
    print(f"  Fetching prices: {ticker}...")
    try:
        # We use get_ticker_price for full OHLCV (get_dataframe has issues/is unused)
        raw = client.get_ticker_price(
            ticker,
            startDate=START_DATE,
            endDate=END_DATE,
            frequency="daily",
            fmt="json",
        )

        if not raw:
            print(f"  WARNING: No data for {ticker}")
            return pd.DataFrame()

        df = pd.DataFrame(raw)
        df = df.rename(columns={
            "date":      "date",
            "open":      "open",
            "high":      "high",
            "low":       "low",
            "close":     "close",
            "volume":    "volume",
            "adjClose":  "adj_close",
        })

        keep = [c for c in ["date","open","high","low","close","adj_close","volume"] if c in df.columns]
        df = df[keep]
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["ticker"] = ticker
        df["ingested_at"] = datetime.now().isoformat()
        df = df.sort_values("date").reset_index(drop=True)

        print(f"  {ticker}: {len(df)} rows OK")
        return df

    except Exception as e:
        print(f"  ERROR {ticker}: {e}")
        return pd.DataFrame()


def fetch_company_metadata(ticker: str) -> dict:
    print(f"  Fetching metadata: {ticker}...")
    try:
        meta = client.get_ticker_metadata(ticker)
        return {
            "ticker":        meta.get("ticker", ticker).upper(),
            "company_name":  meta.get("name", ""),
            "description":   (meta.get("description") or "")[:300],
            "exchange":      meta.get("exchangeCode", ""),
            "start_date":    meta.get("startDate", ""),
            "end_date":      meta.get("endDate", ""),
            "fetched_at":    datetime.now().isoformat(),
        }
    except Exception as e:
        print(f"  METADATA ERROR {ticker}: {e}")
        return {"ticker": ticker}


def run_pull(tickers: list):
    print(f"\nTiingo pull: {len(tickers)} tickers | {START_DATE} → {END_DATE}")
    print("Rate limit: 50,000 req/month free — no significant delay needed.\n")

    all_prices   = []
    all_metadata = []

    for i, ticker in enumerate(tickers):
        df = fetch_daily_prices(ticker)
        if not df.empty:
            all_prices.append(df)

        time.sleep(0.5)  # gentle delay — Tiingo is not strict

        meta = fetch_company_metadata(ticker)
        all_metadata.append(meta)

        time.sleep(0.5)

        if i < len(tickers) - 1:
            print()

    if not all_prices:
        print("\nNo data fetched. Double-check your TIINGO_API_KEY in .env")
        return pd.DataFrame(), pd.DataFrame()

    prices_df   = pd.concat(all_prices, ignore_index=True)
    metadata_df = pd.DataFrame(all_metadata)

    os.makedirs("data/raw", exist_ok=True)
    prices_df.to_csv("data/raw/stock_prices_test.csv",   index=False)
    metadata_df.to_csv("data/raw/stock_metadata_test.csv", index=False)

    print("\n--- Summary ---")
    print(f"Total price rows   : {len(prices_df):,}")
    print(f"Tickers with data  : {prices_df['ticker'].nunique()}")
    print(f"Date range         : {prices_df['date'].min()} → {prices_df['date'].max()}")
    print(f"Metadata rows      : {len(metadata_df)}")

    print("\nColumns in price data:")
    print(list(prices_df.columns))

    print("\nSample (AAPL, last 5 rows):")
    print(prices_df[prices_df["ticker"] == "AAPL"].tail().to_string(index=False))

    print("\nMetadata:")
    print(metadata_df[["ticker","company_name","exchange"]].to_string(index=False))

    return prices_df, metadata_df


if __name__ == "__main__":
    print("Verifying Tiingo API key...")
    try:
        test = client.get_ticker_metadata("AAPL")
        print(f"Key OK — connected. Test ticker: {test.get('name')}\n")
        prices, metadata = run_pull(TICKERS)
        print("\nDone. Files saved to data/raw/")
    except Exception as e:
        print(f"Key check failed: {e}")
        print("Make sure TIINGO_API_KEY is set correctly in your .env file")