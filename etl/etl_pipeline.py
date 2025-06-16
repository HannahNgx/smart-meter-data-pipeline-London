import pandas as pd
import glob
import os

#Extract
INPUT_DIR = os.path.join(os.path.dirname(__file__), '../data/raw_data')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '../data/merged_data')

all_files = glob.glob(os.path.join(INPUT_DIR, 'block_*.csv'))

#Transform
dfs = []
for file in all_files:
    df = pd.read_csv(file)
    df.rename(columns={
        'LCLid': 'customer_id',
        'day': 'date',
        'energy_sum': 'daily_energy_usage_kwh'
    }, inplace=True)
    df['date'] = pd.to_datetime(df['date'])
    dfs.append(df)

#Load
merged_df = pd.concat(dfs, ignore_index=True)
merged_df = merged_df[['customer_id', 'date', 'daily_energy_usage_kwh']]

OUTPUT_PATH = os.path.join(OUTPUT_DIR, 'merged_data.csv')
merged_df.to_csv(OUTPUT_PATH, index=False)
print(f"ETL complete. Merged file saved to {OUTPUT_PATH}")

