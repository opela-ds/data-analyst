import pandas as pd
import json
import os

script_dir = os.path.dirname(os.path.abspath(__file__))

root_dir = os.path.abspath(os.path.join(script_dir, ".."))

csv_path = os.path.join(script_dir, "scraped_data.csv")
df = pd.read_csv(csv_path)

df.columns = [col.strip() for col in df.columns]
df.fillna("", inplace=True)
data = df.to_dict(orient="records")

json_root_path = os.path.join(root_dir, "scraped_data.json")
json_local_path = os.path.join(script_dir, "scraped_data.json")

for path in [json_root_path, json_local_path]:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✅ Saved JSON to {path}")
