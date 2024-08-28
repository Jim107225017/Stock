import pandas as pd

from constants import (
    REALIZE_GAIN_LOSS_SHEET, 
    DATE_COLUMN,
    TICKER_COLUMN,
    BUY_COLUMN,
    SALE_COLUMN,
    PRICE_COLUMN, 
    YEAR_COLUMN
)
from tools import SheetAction

AVG_PRICE = "BuyAvgPrice"
REWARD = "Reward"

class RealizeGainLoss(SheetAction):
    SHEET_NAME = REALIZE_GAIN_LOSS_SHEET

    def __init__(self, df: pd.DataFrame, df_dividends: pd.DataFrame) -> None:
        self.df_dividends = df_dividends.copy(deep=True)
        super().__init__(df)

    def generate_table(self) -> pd.DataFrame:
        df_rewards = self.calculate_reward()
        df_rewards[YEAR_COLUMN] = df_rewards[DATE_COLUMN].dt.year

        df_aggregate = self.aggregate_df(df_rewards, groups=[TICKER_COLUMN, YEAR_COLUMN], aggregate={REWARD: "sum"})

        df_pivot = self.pivot_df(df_aggregate, index=TICKER_COLUMN, columns=YEAR_COLUMN, values=REWARD)

        df_realize_gain_loss = pd.concat([df_pivot, self.df_dividends]).groupby(TICKER_COLUMN, as_index=False).sum()
        df_realize_gain_loss = self.process_output_df(df_realize_gain_loss)
        df_realize_gain_loss = self.calculate_total(df_realize_gain_loss)
        
        return df_realize_gain_loss

    def calculate_reward(self) -> pd.DataFrame:
        
        rewards = []
        positions = {}   # 紀錄 Ticker 持倉

        for _, row in self.df.iterrows():
            ticker = row[TICKER_COLUMN]

            if ticker not in positions:
                self.update_positions_init(positions, ticker)
            
            if pd.notna(row[BUY_COLUMN]) and row[BUY_COLUMN] > 0:
                self.update_positions_buy(row, positions, ticker)

            if pd.notna(row[SALE_COLUMN]) and row[SALE_COLUMN] > 0:
                self.update_positions_sale(row, positions, ticker, rewards)

        df_rewards = pd.DataFrame(rewards)
        return df_rewards

    def update_positions_init(self, positions: dict, ticker: str) -> None:
        positions[ticker] = {'quantity': 0, 'avg_buy_price': 0}
    
    def update_positions_buy(self, row: pd.DataFrame, positions: dict, ticker: str) -> None:
        total_cost = positions[ticker]['quantity'] * positions[ticker]['avg_buy_price'] + row[BUY_COLUMN] * row[PRICE_COLUMN]
        positions[ticker]['quantity'] += row[BUY_COLUMN]
        positions[ticker]['avg_buy_price'] = total_cost / positions[ticker]['quantity']
    
    def update_positions_sale(self, row: pd.DataFrame, positions: dict, ticker: str, rewards: list) -> None:
        reward = (row[PRICE_COLUMN] - positions[ticker]['avg_buy_price']) * row[SALE_COLUMN] * 1000
        rewards.append({
            DATE_COLUMN: row[DATE_COLUMN],
            TICKER_COLUMN: ticker,
            REWARD: reward
        })
        positions[ticker]['quantity'] -= row[SALE_COLUMN]
    
    def process_output_df(self, df: pd.DataFrame) -> pd.DataFrame:
        # Drop Columns
        df =df.drop(columns=[self.TOTAL])

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