import yfinance as yf
import pandas as pd
import logging
import time
from typing import List, Dict

#configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StockIngester:
    def __init__(self, tickers: List[str],period: str='1Y',interval: str='1d',max_retries: int=3, retry_delay: int=5):
        self.tickers= tickers
        self.period = period
        self.interval = interval
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        logger.info(f"Initialized StockIngester with tickers: {tickers}, period: {period}, interval: {interval}")

    def _fetch_single(self,ticker: str) -> pd.DataFrame:
        attempt = 0
        while attempt<self.max_retries:
            try:
                logger.info(f"fetching data for {ticker}, attempt {attempt+1}")
                df = yf.download(ticker, period=self.period, interval=self.interval,progress=False)
                if df.empty:
                    raise ValueError(f"No data found for ticker {ticker}")
                df['Ticker'] = ticker
                df.reset_index(inplace=True)    
                logger.info(f"Successfully fetched data for {ticker}")
                return df
            
            except Exception as e:
                logger.error(f"Error fetching data for {ticker}: {e}")
                attempt += 1
                logger.warning(f"{ticker} attempt {attempt} failed, retrying in {self.retry_delay} seconds...")
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay)

                logger.error(f"Failed to fetch data for {ticker} after {self.max_retries} attempts.")
                return None
    



    def ingest(self) -> pd.DataFrame:
        all_data=[]

        for ticker in self.tickers:
            df=self._fetch_single(ticker)
            if df is not None:
                all_data.append(df)
        if not all_data:
            raise RuntimeError("No data fetched for any ticker.")
        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Successfully ingested data for {len(self.tickers)} tickers.")
        return combined_df
    
    def get_metadata(self) -> dict:
        try:
            info = yf.Ticker(self.tickers).info
            return {
                "ticker": self.tickers,
                "company_name": info.get("LongName"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "market_cap": info.get("marketCap"),
                "exchange": info.get("exchange")
            }
            
        except Exception as e:
            logger.error(f"Error fetching metadata for {self.tickers}: {e}")


# Add this at the bottom temporarily
