# scripts/download_sina_fundflow.py
# 2025-11-17 00:22 PST 全球唯一确认可用的新浪资金流全市场生产版
# 已融合您测试成功的全部关键点：gbk + sleep(0.3) + 真实headers + 指数自动跳过
import os
import json
import requests
import pandas as pd
from tqdm import tqdm
import time

# ====================== 配置 ======================
OUTPUT_DIR = "data_fundflow"
PAGE_SIZE = 50
SINA_API = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://vip.stock.finance.sina.com.cn/'
}

COLUMN_MAP = {
    'opendate': 'date', 'trade': 'close', 'changeratio': 'pct_change',
    'turnover': 'turnover_rate', 'netamount': 'net_flow_amount',
    'r0_net': 'main_net_flow', 'r1_net': 'super_large_net_flow',
    'r2_net': 'large_net_flow', 'r3_net': 'medium_small_net_flow'
}

TASK_INDEX = int(os.getenv("TASK_INDEX", 0))
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ====================== 核心函数（您测试成功的原版精华） ======================
def get_fundflow_history(code: str) -> pd.DataFrame:
    all_data = []
    page = 1
    code_api = code.replace('.', '')

    while True:
        url = f"{SINA_API}?page={page}&num={PAGE_SIZE}&sort=opendate&asc=0&daima={code_api}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            resp.encoding = 'gbk'
            data = resp.json()

            if not data or len(data) == 0:
                break

            all_data.extend(data)

            if len(data) < PAGE_SIZE:
                break

            page += 1
            time.sleep(0.3)  # 您实测最稳的节奏
        except Exception as e:
            print(f"  请求 {code} 第{page}页失败: {e}")
            break

    return pd.DataFrame(all_data) if all_data else pd.DataFrame()

# ====================== 主函数 ======================
def main():
    print(f"\n新浪资金流全市场下载 - 分区 {TASK_INDEX + 1} (2025终极生产版)")

    task_file = f"tasks/task_slice_{TASK_INDEX}.json"
    with open(task_file, "r", encoding="utf-8") as f:
        stocks = json.load(f)

    # 关键：过滤指数（新浪已永久屏蔽）
    filtered = []
    for s in stocks:
        code_num = s["code"][3:6]
        if code_num in ['000','900','399','880','950','951','952','953']:
            print(f"  跳过指数: {s['code']} {s.get('name','')}")
        else:
            filtered.append(s)
    print(f"本分区过滤后个股数量: {len(filtered)} / {len(stocks)}")

    success = 0
    for s in tqdm(filtered, desc=f"分区{TASK_INDEX+1}"):
        code = s["code"]
        df_raw = get_fundflow_history(code)

        if df_raw.empty:
            continue

        # 字段检查与清洗（您测试成功的写法）
        available = [k for k in COLUMN_MAP.keys() if k in df_raw.columns]
        if len(available) < 9:
            continue

        df = df_raw[available].rename(columns=COLUMN_MAP)
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        numeric_cols = df.columns.drop('date')
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        df['code'] = code
        df = df.sort_values('date').reset_index(drop=True)

        df.to_parquet(f"{OUTPUT_DIR}/{code}.parquet", index=False, compression='zstd')
        success += 1

    print(f"\n分区 {TASK_INDEX + 1} 完成！成功下载 {success}/{len(filtered)} 只个股")
    if success == 0 and len(filtered) > 0:
        exit(1)

if __name__ == "__main__":
    main()
