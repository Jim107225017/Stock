import yfinance as yf
import pandas as pd

from datetime import datetime, UTC, timedelta

from constants import (
    UNREALIZE_GAIN_LOSS_SHEET, 
    DATE_COLUMN,
    TICKER_COLUMN,
    BUY_COLUMN,
    SALE_COLUMN,
    PRICE_COLUMN, 
    YEAR_COLUMN
)
from tools import SheetAction

AVG_PRICE = "BuyAvgPrice"
QUANTITY = "Quantity"
DATE = "Date"
PRICE = "Price"
UNREWARD = "UnReward"

class UnRealizeGainLoss(SheetAction):
    SHEET_NAME = UNREALIZE_GAIN_LOSS_SHEET

    def __init__(self, df: pd.DataFrame) -> None:
        super().__init__(df)

    def generate_table(self) -> pd.DataFrame:
        df_quantities = self.calculate_quantity()

        df_quantities = self.pretain_last(df_quantities)
        df_quantities = self.fill_missing_years(df_quantities)
        
        df_unrewards = self.calculate_unrealize(df_quantities)

        df_aggregate = self.aggregate_df(df_unrewards, groups=[TICKER_COLUMN, YEAR_COLUMN], aggregate={UNREWARD: "sum"})

        df_pivot = self.pivot_df(df_aggregate, index=TICKER_COLUMN, columns=YEAR_COLUMN, values=UNREWARD)

        df_unrealize_gain_loss = self.process_output_df(df_pivot)
        
        return df_unrealize_gain_loss

    def calculate_quantity(self) -> pd.DataFrame:
        
        quantities = []
        positions = {}   # 紀錄 Ticker 持倉

        for _, row in self.df.iterrows():
            ticker = row[TICKER_COLUMN]

            if ticker not in positions:
                self.update_positions_init(positions, ticker)
            
            if pd.notna(row[BUY_COLUMN]) and row[BUY_COLUMN] > 0:
                self.update_positions_buy(row, positions, ticker, quantities)

            if pd.notna(row[SALE_COLUMN]) and row[SALE_COLUMN] > 0:
                self.update_positions_sale(row, positions, ticker, quantities)

        df_quantities = pd.DataFrame(quantities)
        df_quantities = df_quantities.sort_values(DATE_COLUMN)
        df_quantities[YEAR_COLUMN] = df_quantities[DATE_COLUMN].dt.year
        
        return df_quantities

    def update_positions_init(self, positions: dict, ticker: str) -> None:
        positions[ticker] = {'quantity': 0, 'avg_buy_price': 0}
    
    def update_positions_buy(self, row: pd.DataFrame, positions: dict, ticker: str, quantities: list) -> None:
        total_cost = positions[ticker]['quantity'] * positions[ticker]['avg_buy_price'] + row[BUY_COLUMN] * row[PRICE_COLUMN]
        positions[ticker]['quantity'] += row[BUY_COLUMN]
        positions[ticker]['avg_buy_price'] = total_cost / positions[ticker]['quantity']
        self.update_quantities(row, positions, ticker, quantities)
    
    def update_positions_sale(self, row: pd.DataFrame, positions: dict, ticker: str, quantities: list) -> None:
        positions[ticker]['quantity'] -= row[SALE_COLUMN]
        self.update_quantities(row, positions, ticker, quantities)
        
    def update_quantities(self, row: pd.DataFrame, positions: dict, ticker: str, quantities: list) -> None:
        quantity = positions[ticker]['quantity']
        avg_buy_price = positions[ticker]['avg_buy_price']
        quantities.append({
            DATE_COLUMN: row[DATE_COLUMN],
            TICKER_COLUMN: ticker,
            QUANTITY: quantity,
            AVG_PRICE: avg_buy_price
        })
    
    def pretain_last(self, df: pd.DataFrame) -> pd.DataFrame:
        df_last = df.sort_values(DATE_COLUMN).groupby([df[TICKER_COLUMN], df[YEAR_COLUMN]], as_index=False).last()
        df_last = df_last.sort_values(DATE_COLUMN)
        return df_last
    
    def fill_missing_years(self, df: pd.DataFrame) -> pd.DataFrame:
        year_ranges = tuple(range(df[YEAR_COLUMN].min(), df[YEAR_COLUMN].max()+1))
        tickers = df[TICKER_COLUMN].unique()
        new_rows = []
        for ticker_inner in tickers:
            df_tmp = df[df[TICKER_COLUMN] == ticker_inner]

            for year_inner in year_ranges:
                if not df_tmp[df_tmp[YEAR_COLUMN] == year_inner].empty:
                    continue
                
                df_before_year = df_tmp[df_tmp[YEAR_COLUMN] <= year_inner]
                if df_before_year.empty:
                    new_rows.append({
                        DATE_COLUMN: datetime(year=year_inner, month=1, day=1),
                        TICKER_COLUMN: ticker_inner,
                        QUANTITY: 0,
                        AVG_PRICE: 0
                    })
                else:
                    df_last_before_year = df_before_year.iloc[-1, :]
                    last_before_year_dict = df_last_before_year.to_dict()
                    new_rows.append({
                        DATE_COLUMN: datetime(year=year_inner, month=1, day=1),
                        TICKER_COLUMN: ticker_inner,
                        QUANTITY: last_before_year_dict[QUANTITY],
                        AVG_PRICE: last_before_year_dict[AVG_PRICE]
                    })

        df_news = pd.DataFrame(new_rows)
        df_news[YEAR_COLUMN] = df_news[DATE_COLUMN].dt.year
        df_fill = pd.concat([df, df_news], ignore_index=True)
        df_fill = df_fill.sort_values(DATE_COLUMN)

        return df_fill
    
    def calculate_unrealize(self, df: pd.DataFrame) -> pd.DataFrame:
        unrewards = []
        
        for _, row in df.iterrows():
            tw_ticker = f"{row[TICKER_COLUMN]}.TW"
            year = row[YEAR_COLUMN]
            closed_price = self.get_stock_closed_price_end_of_year(tw_ticker, year)
            avg_buy_price = row[AVG_PRICE]
            unreward = (closed_price - avg_buy_price) * row[QUANTITY] * 1000
            unrewards.append({
                DATE_COLUMN: row[DATE_COLUMN],
                TICKER_COLUMN: row[TICKER_COLUMN],
                UNREWARD: unreward
            })
        
        df_unrewards = pd.DataFrame(unrewards)
        df_unrewards[YEAR_COLUMN] = df_unrewards[DATE_COLUMN].dt.year

        return df_unrewards
    
    @staticmethod
    def get_stock_closed_price_end_of_year(ticker: str, year: int) -> float:
        stock = yf.Ticker(ticker)
        end_date = datetime(year=year, month=12, day=31, hour=23, minute=59, second=59)
        if end_date > datetime.now():
            end_date = datetime.now()
        
        start_date = end_date - timedelta(weeks=1)

        end_date_str = end_date.strftime("%Y-%m-%d")
        start_date_str = start_date.strftime("%Y-%m-%d")
        
        history = stock.history(start=start_date_str, end=end_date_str)
        closed_price = history.Close.values[-1]
        
        return closed_price


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