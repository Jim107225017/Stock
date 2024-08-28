from tools import load_data
from summary import SummaryAction
from dividends import DividendsAction
from realize_gain_loss import RealizeGainLoss
from unrealize_gain_loss import UnRealizeGainLoss

df = load_data()

summary = SummaryAction(df)
summary.write_table()

dividends = DividendsAction(df)
dividends.write_table()

realize_gain_loss = RealizeGainLoss(df, dividends.get_table())
realize_gain_loss.write_table()

unrealize_gain_loss = UnRealizeGainLoss(df)
unrealize_gain_loss.write_table()

print("DONE")
