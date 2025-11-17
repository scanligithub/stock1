# scripts/download_sina_fundflow.py
import os
import json
import requests
import pandas as pd
import time
from tqdm import tqdm

OUTPUT_DIR = "data_fundflow"
PAGE_SIZE = 50
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))
os.makedirs(OUTPUT_DIR, exist_ok=True)

SINA_API = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://vip.stock.finance.sina.com.cn/'
}

COLUMN_MAP = {
    'opendate': 'date', 'trade': 'close', 'changeratio': 'pct_change',
    'turnover': 'turnover_rate', 'netamount': 'net_flow_amount',
    'r0_net': 'main_net_flow', 'r1_net': 'super_large_net_flow',
    'r2_net': 'large_net_flow', 'r3_net': 'medium_small_net_flow'
}

def get_fundflow(code):
    all_data = []
    page = 1
    code_api = code.replace('.', '')
    with tqdm(total=None, desc=f"{code} 分页", leave=False) as pbar:
        while True:
            url = f"{SINA_API}?page={page}&num={PAGE_SIZE}&sort=opendate&asc=0&daima={code_api}"
            try:
                resp = requests.get(url, headers=HEADERS, timeout=30)
                resp.raise_for_status()
                resp.encoding = 'gbk'
                data = resp.json()
                if not data:
                    break
                all_data.extend(data)
                pbar.update(1)
                if len(data) < PAGE_SIZE:
                    break
                page += 1
                time.sleep(0.32)
            except:
                break
    return pd.DataFrame(all_data) if all_data else pd.DataFrame()

def main():
    print(f"资金流下载 - 分区 {TASK_INDEX + 1}")
    task_file = f"tasks/task_slice_{TASK_INDEX}.json"
    with open(task_file) as f:
        subset = json.load(f)

    success = 0
    for s in tqdm(subset, desc=f"分区 {TASK_INDEX+1}"):
        df = get_fundflow(s["code"])
        if not df.empty and all(k in df.columns for k in COLUMN_MAP):
            df = df[list(COLUMN_MAP.keys())].rename(columns=COLUMN_MAP)
            df['date'] = pd.to_datetime(df['date'])
            df[df.columns[1:]] = df[df.columns[1:]].apply(pd.to_numeric, errors='coerce')
            df['code'] = s["code"]
            df.to_parquet(f"{OUTPUT_DIR}/{s['code']}.parquet", index=False)
            success += 1

    print(f"资金流分区 {TASK_INDEX+1} 完成，成功 {success}/{len(subset)}")

if __name__ == "__main__":
    main()
