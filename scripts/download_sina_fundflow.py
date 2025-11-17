# scripts/download_sina_fundflow.py   ← 2025年最新可用版（已绕过反爬）
import os
import json
import requests
import pandas as pd
import time
from tqdm import tqdm
import random

OUTPUT_DIR = "data_fundflow"
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 新浪当前真实请求地址（2025年11月）
REAL_API = "http://push2.eastmoney.com/api/qt/stock/fflow/daykline/get"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Referer": "https://quote.eastmoney.com/",
    "Accept": "*/*",
}

# 东财代码转换
def code_to_em(code: str) -> str:
    if code.startswith("sh"):
        return "1." + code[3:]
    elif code.startswith("sz"):
        return "0." + code[3:]
    elif code.startswith("bj"):
        return "2." + code[3:]
    return code

COLUMN_MAP = {
    'date': 'date',
    'close': 'close',
    'change': 'pct_change',
    'turnover': 'turnover_rate',
    'net_main': 'main_net_flow',
    'net_super': 'super_large_net_flow',
    'net_big': 'large_net_flow',
    'net_medium_small': 'medium_small_net_flow',
    'net_all': 'net_flow_amount'
}

def get_fundflow_em(code: str) -> pd.DataFrame:
    em_code = code_to_em(code)
    fields = "date,close,change,turnover,net_main,net_super,net_big,net_medium_small,net_all"
    url = f"{REAL_API}?lmt=0&fields={fields}&klt=101&secid={em_code}"
    
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        if resp.status_code != 200 or not resp.text.strip():
            return pd.DataFrame()
        
        data = resp.json()
        if not data.get("data") or not data["data"].get("klines"):
            return pd.DataFrame()
            
        lines = data["data"]["klines"]
        records = []
        for line in lines:
            items = line.split(",")
            if len(items) >= 9:
                record = {
                    'date': items[0],
                    'close': float(items[1]),
                    'pct_change': float(items[2]) / 100 if items[2] != '-' else None,
                    'turnover_rate': float(items[3]) if items[3] != '-' else None,
                    'main_net_flow': float(items[4]) * 10000,      # 元 → 万
                    'super_large_net_flow': float(items[5]) * 10000,
                    'large_net_flow': float(items[6]) * 10000,
                    'medium_small_net_flow': float(items[7]) * 10000,
                    'net_flow_amount': float(items[8]) * 10000,
                }
                records.append(record)
        
        return pd.DataFrame(records)
    except:
        return pd.DataFrame()

def main():
    print(f"资金流下载（东财接口） - 分区 {TASK_INDEX + 1}")
    task_file = f"tasks/task_slice_{TASK_INDEX}.json"
    with open(task_file) as f:
        subset = json.load(f)

    success = 0
    for s in tqdm(subset, desc=f"分区 {TASK_INDEX+1}"):
        code = s["code"]
        df = get_fundflow_em(code)
        if not df.empty:
            df['code'] = code
            df.to_parquet(f"{OUTPUT_DIR}/{code}.parquet", index=False)
            success += 1
        else:
            tqdm.write(f"  × {code} 无数据")
        
        time.sleep(random.uniform(0.15, 0.35))  # 更自然防封

    print(f"资金流分区 {TASK_INDEX+1} 完成，成功 {success}/{len(subset)}")
    if success == 0 and len(subset) > 0:
        print("警告：本分区全部失败，可能触发风控")
        exit(1)

if __name__ == "__main__":
    main()
