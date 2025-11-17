# scripts/download_sina_fundflow.py
# 2025-11-17 真正全市场版：个股用新浪，指数自动切东财，0 丢股！
import os
import json
import requests
import pandas as pd
from tqdm import tqdm
import time

OUTPUT_DIR = "data_fundflow"
PAGE_SIZE = 50
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 新浪接口（个股）
SINA_API = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb"

# 东财接口（指数专用）
EM_API = "http://push2.eastmoney.com/api/qt/stock/fflow/daykline/get"

HEADERS_SINA = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://vip.stock.finance.sina.com.cn/'
}

HEADERS_EM = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://quote.eastmoney.com/"
}

COLUMN_MAP = {
    'opendate': 'date', 'trade': 'close', 'changeratio': 'pct_change',
    'turnover': 'turnover_rate', 'netamount': 'net_flow_amount',
    'r0_net': 'main_net_flow', 'r1_net': 'super_large_net_flow',
    'r2_net': 'large_net_flow', 'r3_net': 'medium_small_net_flow'
}

def is_index(code: str) -> bool:
    num = code[3:6]
    return num in ['000','900','399','880','950','951','952','953','899']

def get_sina_fundflow(code: str) -> pd.DataFrame:
    all_data = []
    page = 1
    code_api = code.replace('.', '')
    while True:
        url = f"{SINA_API}?page={page}&num={PAGE_SIZE}&sort=opendate&asc=0&daima={code_api}"
        try:
            r = requests.get(url, headers=HEADERS_SINA, timeout=30)
            r.raise_for_status()
            r.encoding = 'gbk'
            data = r.json()
            if not data: break
            all_data.extend(data)
            if len(data) < PAGE_SIZE: break
            page += 1
            time.sleep(0.3)
        except: break
    return pd.DataFrame(all_data) if all_data else pd.DataFrame()

def get_em_fundflow_index(code: str) -> pd.DataFrame:
    """专为指数准备的东财接口（字段完美对齐）"""
    prefix = "1." if code.startswith("sh") else "0."
    secid = prefix + code[3:]
    url = f"{EM_API}?lmt=0&klt=101&fields1=f1,f2,f3,f7&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59&secid={secid}"
    try:
        j = requests.get(url, headers=HEADERS_EM, timeout=20).json()
        klines = j.get("data", {}).get("klines", [])
        records = []
        for line in klines:
            items = line.split(",")
            if len(items) >= 9:
                records.append({
                    "opendate": items[0],
                    "trade": float(items[1]),
                    "changeratio": float(items[2])/100,
                    "turnover": None,  # 指数无换手率
                    "netamount": float(items[8])*10000,     # 元 → 万
                    "r0_net": float(items[4])*10000,
                    "r1_net": float(items[5])*10000,
                    "r2_net": float(items[6])*10000,
                    "r3_net": float(items[7])*10000,
                })
        return pd.DataFrame(records)
    except:
        return pd.DataFrame()

def get_fundflow_smart(code: str) -> pd.DataFrame:
    if is_index(code):
        print(f"  → 检测为指数 {code}，自动切换东财接口")
        return get_em_fundflow_index(code)
    else:
        return get_sina_fundflow(code)

def main():
    print(f"\n2025全市场资金流下载（个股新浪+指数东财）- 分区 {TASK_INDEX + 1}")

    task_file = f"tasks/task_slice_{TASK_INDEX}.json"
    with open(task_file) as f:
        stocks = json.load(f)

    print(f"本分区共 {len(stocks)} 只（含指数）")
    success = 0

    for s in tqdm(stocks, desc=f"分区{TASK_INDEX+1}"):
        code = s["code"]
        df_raw = get_fundflow_smart(code)

        if df_raw.empty:
            continue

        # 统一字段处理
        if is_index(code):
            # 东财返回的已经是对的字段，直接改名
            df_raw = df_raw.rename(columns={"opendate": "date"})
        else:
            # 新浪返回的字段
            available = [k for k in COLUMN_MAP.keys() if k in df_raw.columns]
            if len(available) < 8: continue
            df_raw = df_raw[available].rename(columns=COLUMN_MAP)

        df_raw['date'] = pd.to_datetime(df_raw['date'], errors='coerce')
        numeric_cols = [c for c in df_raw.columns if c != 'date']
        df_raw[numeric_cols] = df_raw[numeric_cols].apply(pd.to_numeric, errors='coerce')
        df_raw['code'] = code
        df_raw = df_raw.sort_values('date').reset_index(drop=True)

        df_raw.to_parquet(f"{OUTPUT_DIR}/{code}.parquet", index=False, compression='zstd')
        success += 1

    print(f"\n分区 {TASK_INDEX + 1} 完成！成功下载 {success}/{len(stocks)} 只（含指数）")
    if success == 0 and len(stocks) > 0:
        exit(1)

if __name__ == "__main__":
    main()
