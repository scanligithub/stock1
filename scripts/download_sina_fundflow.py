# scripts/download_sina_fundflow.py
import os
import json
import requests
import pandas as pd
from tqdm import tqdm
import time

OUTPUT_DIR = "data_fundflow"
PAGE_SIZE = 50
SINA_API = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page={page}&num={num}&sort=opendate&asc=0&daima={code}"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://vip.stock.finance.sina.com.cn/'
}
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_full_history_fund_flow(code_with_prefix):
    all_data = []
    page = 1
    code_api = code_with_prefix.replace('.', '')
    while True:
        url = SINA_API.format(page=page, num=PAGE_SIZE, code=code_api)
        try:
            r = requests.get(url, headers=HEADERS, timeout=45)
            r.raise_for_status()
            r.encoding = 'gbk'
            data = r.json()
            if not data: break
            all_data.extend(data)
            if len(data) < PAGE_SIZE: break
            page += 1
            time.sleep(0.3)
        except Exception as e:
            print(f"请求 {code_with_prefix} 第{page}页失败: {e}")
            break
    return pd.DataFrame(all_data) if all_data else pd.DataFrame()

def main():
    print(f"开始新浪资金流下载 - 分区 {TASK_INDEX + 1}")
    task_file = f"tasks/task_slice_{TASK_INDEX}.json"
    with open(task_file, "r", encoding="utf-8") as f:
        stocks = json.load(f)
    print(f"本分区 {len(stocks)} 只股票")

    success = 0
    for s in tqdm(stocks, desc=f"分区{TASK_INDEX+1}"):
        code = s["code"]
        df = get_full_history_fund_flow(code)
        if df.empty: continue

        keep = {'opendate':'date','trade':'close','changeratio':'pct_change','turnover':'turnover_rate',
                'netamount':'net_flow_amount','r0_net':'main_net_flow','r1_net':'super_large_net_flow',
                'r2_net':'large_net_flow','r3_net':'medium_small_net_flow'}
        if all(c in df.columns for c in keep):
            df = df[list(keep.keys())].rename(columns=keep)
            df['date'] = pd.to_datetime(df['date'])
            df[df.columns[1:]] = df[df.columns[1:]].apply(pd.to_numeric, errors='coerce')
            df['code'] = code
            df.to_parquet(f"{OUTPUT_DIR}/{code}.parquet", index=False, compression='zstd')
            success += 1

    print(f"分区 {TASK_INDEX+1} 完成，成功 {success}/{len(stocks)}")
    if success == 0 and len(stocks) > 0:
        exit(1)

if __name__ == "__main__":
    main()
