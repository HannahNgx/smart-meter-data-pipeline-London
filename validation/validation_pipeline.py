import pandas as pd
import os

MERGED_PATH = os.path.join(os.path.dirname(__file__), '../data/merged_data/merged_data.csv')
df = pd.read_csv(MERGED_PATH)

missing_customer_id = df[df['customer_id'].isnull()]
print(f"Missing customer_id rows: {len(missing_customer_id)}")

missing_date = df[df['date'].isnull()]
print(f"Missing date rows: {len(missing_date)}")

missing_usage = df[df['daily_energy_usage_kwh'].isnull()]
print(f"Missing energy usage rows: {len(missing_usage)}")

invalid_usage = df[(df['daily_energy_usage_kwh'] < 0) | (df['daily_energy_usage_kwh'] > 100)]
print(f"Invalid energy usage rows: {len(invalid_usage)}")

duplicates = df.duplicated(subset=['customer_id', 'date'])
print(f"Duplicate rows: {duplicates.sum()}")

PROBLEM_DIR = os.path.join(os.path.dirname(__file__), '../data/validation_reports')
os.makedirs(PROBLEM_DIR, exist_ok=True)

missing_customer_id.to_csv(os.path.join(PROBLEM_DIR, 'missing_customer_id.csv'), index=False)
missing_date.to_csv(os.path.join(PROBLEM_DIR, 'missing_date.csv'), index=False)
missing_usage.to_csv(os.path.join(PROBLEM_DIR, 'missing_usage.csv'), index=False)
invalid_usage.to_csv(os.path.join(PROBLEM_DIR, 'invalid_usage.csv'), index=False)
df[duplicates].to_csv(os.path.join(PROBLEM_DIR, 'duplicates.csv'), index=False)

print("Validation complete. Reports saved to validation_reports/")
