# scripts/collect_fundflow.py
import pandas as pd
import glob
import os
import shutil
import json
from tqdm import tqdm

INPUT_DIR = "all_fundflow"
SMALL_DIR = "fundflow_small"           # 已修改
FINAL_FILE = "full_fundflow.parquet"
QC_FILE = "data_quality_report_fundflow.json"

os.makedirs(SMALL_DIR, exist_ok=True)

# 收集小文件
file_list = glob.glob(f"{INPUT_DIR}/**/*.parquet", recursive=True)
for f in tqdm(file_list, desc="收集资金流小文件"):
    shutil.copy2(f, os.path.join(SMALL_DIR, os.path.basename(f)))

# 合并
dfs = [pd.read_parquet(f) for f in file_list]
merged = pd.concat(dfs, ignore_index=True)
merged = merged.sort_values(['code', 'date']).reset_index(drop=True)
merged.to_parquet(FINAL_FILE, compression='zstd', index=False, row_group_size=100000)

# 简单质检（可扩展为您原来的完整版）
report = {
    "total_records": len(merged),
    "total_stocks": merged['code'].nunique(),
    "date_range": [merged['date'].min().strftime('%Y-%m-%d'), merged['date'].max().strftime('%Y-%m-%d')]
}
with open(QC_FILE, 'w', encoding='utf-8') as f:
    json.dump(report, f, indent=2, ensure_ascii=False, default=str)

print("资金流合并+质检完成")
