# scripts/download_baostock_kdata.py
import os
import json
import baostock as bs
import pandas as pd
from tqdm import tqdm

OUTPUT_DIR = "data_kline"
START_DATE = "2005-01-01"
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_kdata(code):
    rs = bs.query_history_k_data_plus(
        code,
        "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg,isST",
        start_date=START_DATE, end_date="", frequency="d", adjustflag="3"
    )
    if rs.error_code != '0':
        return pd.DataFrame()
    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    return pd.DataFrame(data_list, columns=rs.fields) if data_list else pd.DataFrame()

def main():
    print(f"K线下载 - 分区 {TASK_INDEX + 1}")
    task_file = f"tasks/task_slice_{TASK_INDEX}.json"
    with open(task_file) as f:
        subset = json.load(f)

    lg = bs.login()
    if lg.error_code != '0':
        exit(1)

    success = 0
    try:
        for s in tqdm(subset, desc=f"分区 {TASK_INDEX+1}"):
            df = get_kdata(s["code"])
            if not df.empty:
                df.to_parquet(f"{OUTPUT_DIR}/{s['code']}.parquet", index=False)
                success += 1
    finally:
        bs.logout()

    if success == 0 and len(subset) > 0:
        exit(1)

if __name__ == "__main__":
    main()
