import yfinance as yf
import pandas as pd

from datetime import datetime

from constants import (
    DIVIDENDS_SHEET, 
    TICKER_COLUMN, 
    DATE_COLUMN, 
    BUY_COLUMN, 
    SALE_COLUMN, 
    TOTAL_COLUMN,
)
from tools import SheetAction

DATE = "Date"
PRICE = "Price"
YEAR = "Year"

class DividendsAction(SheetAction):
    """
    Dividends For Each Year
    """
    SHEET_NAME = DIVIDENDS_SHEET

    def __init__(self, df: pd.DataFrame) -> None:
        super().__init__(df)

    def generate_table(self) -> pd.DataFrame:
        tickers = set(self.df[TICKER_COLUMN])
        tickers = sorted(list(tickers))

        df_dividends = pd.DataFrame()

        for ticker_inner in tickers:
            df_dividends_by_ticker = self.get_stock_dividends(ticker_inner)
            ticker_filter = self.df[TICKER_COLUMN] == ticker_inner
            df_ticker = pd.DataFrame(columns=[DATE_COLUMN, TOTAL_COLUMN])
            for row_inner in range(df_dividends_by_ticker.shape[0]):
                date_inner = df_dividends_by_ticker.loc[row_inner, DATE].tz_localize(None)
                datetime_inner = datetime.fromtimestamp(int(date_inner.value / 1e9))
                
                # TBD: TimeZone
                ###############

                date_filter = self.df[DATE_COLUMN] < datetime_inner
                df_filter = self.df.loc[(date_filter & ticker_filter), [BUY_COLUMN, SALE_COLUMN]]
                if df_filter.empty:
                    continue

                holdings = sum(df_filter[BUY_COLUMN] - df_filter[SALE_COLUMN])
                dividends = df_dividends_by_ticker.loc[row_inner, PRICE]
                total = holdings * 1000 * dividends
                date = self.only_date(datetime_inner)
                
                new_row = pd.DataFrame({DATE_COLUMN: [date], TOTAL_COLUMN: [total]})
                df_ticker = pd.concat([df_ticker, new_row], ignore_index=True)
            
            # Handle Stocks That Have Never Paid Dividends Up To Current Time
            if df_ticker.empty:
                new_row = pd.DataFrame({DATE_COLUMN: [datetime.now()], TOTAL_COLUMN: [0]})
                df_ticker = pd.concat([df_ticker, new_row], ignore_index=True)

            df_ticker[YEAR] = df_ticker[DATE].dt.year
            df_aggregate = self.aggregate_df(df_ticker, groups=[YEAR], aggregate={TOTAL_COLUMN: "sum"})
            df_aggregate[TICKER_COLUMN] = ticker_inner
            
            df_pivot = self.pivot_df(df_aggregate, index=TICKER_COLUMN, columns=YEAR, values=TOTAL_COLUMN)
            df_dividends = pd.concat([df_dividends, df_pivot], ignore_index=True)

        df_dividends = self.process_output_df(df_dividends)
        df_dividends = self.calculate_total(df_dividends)
        
        return df_dividends
    
    def process_output_df(self, df: pd.DataFrame) -> pd.DataFrame:
        # Fill NAN
        df = df.fillna(0)

        # Sorted By Year
        years = []
        others = []
        for col in df.columns: 
            if str(col).isdigit():
                years.append(col)
            else:
                others.append(col)
        
        years.sort()
        df = df[others + years]
        return df

    @staticmethod
    def get_stock_dividends(ticker: str) -> pd.DataFrame:
        stock = yf.Ticker(ticker)
        dividends = stock.dividends

        # TODO: 最後回補日
        ################
        
        df_dividends = dividends.reset_index()
        df_dividends.columns = [DATE, PRICE]
        
        return df_dividends

    @staticmethod
    def only_date(dt: datetime) -> datetime:
        date_only = dt.date()
        dt_only_date = datetime.combine(date_only, datetime.min.time())
        return dt_only_date
