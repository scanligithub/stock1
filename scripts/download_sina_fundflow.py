# scripts/download_sina_fundflow.py
# 2025-11-20 最终高容错版：无论成功与否，都确保产出目录非空

import os
import json
import requests
import pandas as pd
from tqdm import tqdm
import time
import sys
import traceback
from pathlib import Path # 引入 pathlib

# ==================== 配置 ====================
OUTPUT_DIR = "data_fundflow"
PAGE_SIZE = 50
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))
# (关键) 确保目录在脚本开始时就存在
os.makedirs(OUTPUT_DIR, exist_ok=True)

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

# ==================== 下载函数 (保持不变) ====================
def get_fundflow(code: str) -> pd.DataFrame:
    # ... (此函数内容与您之前的版本完全相同)
    all_data = []
    page = 1
    code_api = code.replace('.', '')
    while True:
        url = f"{SINA_API}?page={page}&num={PAGE_SIZE}&sort=opendate&asc=0&daima={code_api}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            r.encoding = 'gbk'
            data = r.json()
            if not data: break
            all_data.extend(data)
            if len(data) < PAGE_SIZE: break
            page += 1
            time.sleep(0.3)
        except Exception:
            break
    return pd.DataFrame(all_data) if all_data else pd.DataFrame()

# ==================== 主流程 (已修改) ====================
def main():
    print(f"\n2025全市场资金流下载（统一信源：新浪财经）- 分区 {TASK_INDEX + 1}")

    task_file = f"tasks/task_slice_{TASK_INDEX}.json"
    try:
        with open(task_file) as f:
            stocks = json.load(f)
    except FileNotFoundError:
        print(f"❌ 致命错误: 未找到任务分片文件 {task_file}！"); sys.exit(1)

    if not stocks:
        print("🟡 本分区任务列表为空，正常结束。")
        # 确保 upload-artifact 能找到目录
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
        return

    print(f"本分区共 {len(stocks)} 只标的")
    success_count = 0

    for s in tqdm(stocks, desc=f"分区 {TASK_INDEX+1} 下载中"):
        code = s["code"]
        name = s.get("name", "")
        
        df_raw = get_fundflow(code)

        if df_raw.empty:
            continue

        try:
            # ... (您的数据清洗逻辑保持不变) ...
            available_cols = [k for k in COLUMN_MAP.keys() if k in df_raw.columns]
            if not available_cols:
                continue
            df_cleaned = df_raw[available_cols].copy().rename(columns=COLUMN_MAP)
            df_cleaned['code'] = code
            if 'date' in df_cleaned.columns:
                df_cleaned['date'] = pd.to_datetime(df_cleaned['date'], errors='coerce')
            numeric_cols = [c for c in df_cleaned.columns if c not in ['date', 'code']]
            df_cleaned[numeric_cols] = df_cleaned[numeric_cols].apply(pd.to_numeric, errors='coerce')
            money_cols = [c for c in df_cleaned.columns if 'amount' in c or 'flow' in c]
            if money_cols:
                df_cleaned[money_cols] = df_cleaned[money_cols] * 10000
            df_final = df_cleaned.sort_values('date').reset_index(drop=True)
            output_path = f"{OUTPUT_DIR}/{code}.parquet"
            df_final.to_parquet(output_path, index=False)
            success_count += 1
        except Exception as e:
            print(f"  -> ❌ 在处理 {name} ({code}) 的数据时出错: {e}")

    # --- (这是唯一的、关键的修正) ---
    print(f"\n分区 {TASK_INDEX + 1} 完成！成功下载 {success_count}/{len(stocks)} 只标的")
    
    # 无论下载是否成功，都确保输出目录非空
    if not os.listdir(OUTPUT_DIR):
        print(f"⚠️ 警告: 输出目录 '{OUTPUT_DIR}' 为空。创建一个占位文件以确保 Artifact 上传。")
        # 创建一个空的 .gitkeep 文件作为占位符
        Path(f"{OUTPUT_DIR}/.gitkeep").touch()
    # ---------------------------------------------

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌❌❌ 在 main 函数顶层捕获到致命异常: {e} ❌❌❌")
        traceback.print_exc()
        exit(1)
