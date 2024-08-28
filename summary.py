import pandas as pd

from constants import SUMMARY_SHEET, TICKER_COLUMN, YEAR_COLUMN, TOTAL_COLUMN
from tools import SheetAction

TOTAL = "Total"

class SummaryAction(SheetAction):
    """
    Cost For Each Year And Year Cumulative
    """
    SHEET_NAME = SUMMARY_SHEET

    def __init__(self, df: pd.DataFrame) -> None:
        super().__init__(df)

    def generate_table(self) -> pd.DataFrame:
        df_aggregate = self.aggregate_df(self.df, groups=[TICKER_COLUMN, YEAR_COLUMN], aggregate={TOTAL_COLUMN: "sum"})

        df_summary = self.pivot_df(df_aggregate, index=TICKER_COLUMN, columns=YEAR_COLUMN, values=TOTAL_COLUMN)
        df_summary = self.calculate_total(df_summary)
        
        return df_summary