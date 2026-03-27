import pandas as pd

df = pd.read_csv(
    '../data/debs2022-gc-trading-day-08-11-21.csv',
    comment='#',
)

print(df.head())