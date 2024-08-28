import pandas as pd
from constants import (
    HISTORY_SHEET, 
    EXCEL_FILE, 
    DATE_COLUMN, 
    TICKER_COLUMN, 
    BUY_COLUMN, 
    SALE_COLUMN, 
    PRICE_COLUMN, 
    TOTAL_COLUMN, 
    YEAR_COLUMN,
)


def load_data() -> pd.DataFrame:
    USECOLS = [DATE_COLUMN, TICKER_COLUMN, BUY_COLUMN, SALE_COLUMN, PRICE_COLUMN, TOTAL_COLUMN]
    df = pd.read_excel(EXCEL_FILE, sheet_name=HISTORY_SHEET, engine="openpyxl", usecols=USECOLS)
    
    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN])
    df = df.sort_values(by=DATE_COLUMN)

    df[YEAR_COLUMN] = df[DATE_COLUMN].dt.year

    df[TICKER_COLUMN] = df[TICKER_COLUMN].astype(str)

    df[BUY_COLUMN] = df[BUY_COLUMN].fillna(0)
    df[BUY_COLUMN] = df[BUY_COLUMN].astype(float)

    df[SALE_COLUMN] = df[SALE_COLUMN].fillna(0)
    df[SALE_COLUMN] = df[SALE_COLUMN].astype(float)

    df[PRICE_COLUMN] = df[PRICE_COLUMN].astype(float)

    df[TOTAL_COLUMN] = (df[SALE_COLUMN] - df[BUY_COLUMN]) * 1000 * df[PRICE_COLUMN]
    df[TOTAL_COLUMN] = df[TOTAL_COLUMN].astype(float)

    return df


class SheetAction():
    SHEET_NAME = ""
    TOTAL = "Total"
    
    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df.copy(deep=True)
        self.df_pivot = self.generate_table()
    
    def generate_table(self) -> pd.DataFrame:
        # Polymorphism
        return pd.DataFrame()

    def write_table(self):
        with pd.ExcelWriter(EXCEL_FILE, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            self.df_pivot.to_excel(writer, sheet_name=self.SHEET_NAME, index=False)
    
    def get_table(self) -> pd.DataFrame:
        return self.df_pivot

    def calculate_total(self, df_pivot: pd.DataFrame) -> pd.DataFrame:
        df_years = df_pivot.drop(columns=[TICKER_COLUMN])
        df_years = df_years.fillna(0)

        df_years_sum = df_years.sum(axis=1).astype(int)
        
        df_pivot[self.TOTAL] = df_years_sum
        df_pivot = df_pivot.fillna(0)
        
        return df_pivot

    @staticmethod
    def aggregate_df(df: pd.DataFrame, groups: list, aggregate: dict) -> pd.DataFrame:
        grouped = df.groupby(groups).agg(aggregate).reset_index()

        columns = groups + list(aggregate.keys())
        result_df = pd.DataFrame(grouped, columns=columns)
        return result_df

    @staticmethod
    def pivot_df(df: pd.DataFrame, index: str, columns: str, values: str) -> pd.DataFrame:
        df_pivot = df.pivot(index=index, columns=columns, values=values).reset_index()
        df_pivot.columns.name = None
        df_pivot.columns = [TICKER_COLUMN] + df_pivot.columns[1:].tolist()
        return df_pivot
    