import pandas as pd
from datetime import date


def transform_fun():
    df = pd.read_csv('./msftstock.csv')

    df['week'] = pd.to_datetime(df['week'])
    df['open'] = df['open'].astype(float)
    df['close'] = df['close'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['volume'] = df['volume'].astype(int)
    df.to_csv('mstfstock2.csv', index=False)