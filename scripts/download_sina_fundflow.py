# scripts/download_sina_fundflow.py
# 2025-11-21 最终统一流程版：无论有无数据，都为每只股票生成一个 Parquet 文件

import os
import json
import requests
import pandas as pd
from tqdm import tqdm
import time
import sys
import traceback

# ==================== 配置 ====================
OUTPUT_DIR = "data_fundflow"
PAGE_SIZE = 50
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))
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
# (新增) 定义最终的标准列
FINAL_COLS = [
    'date', 'code', 'close', 'pct_change', 'turnover_rate',
    'net_flow_amount', 'main_net_flow', 'super_large_net_flow',
    'large_net_flow', 'medium_small_net_flow'
]

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
        return

    print(f"本分区共 {len(stocks)} 只标的")
    success_count = 0

    for s in tqdm(stocks, desc=f"分区 {TASK_INDEX+1} 下载中"):
        code = s["code"]
        name = s.get("name", "")
        
        # --- (这是唯一的、关键的修正) ---
        try:
            # 1. 无论如何都先获取数据
            df_raw = get_fundflow(code)
            
            # 2. 数据清洗和格式化
            if not df_raw.empty:
                available_cols = [k for k in COLUMN_MAP.keys() if k in df_raw.columns]
                if available_cols:
                    df_cleaned = df_raw[available_cols].copy().rename(columns=COLUMN_MAP)
                    # (后续所有清洗步骤都在 df_cleaned 上进行)
                else: # 如果返回的数据不包含任何我们认识的列
                    df_cleaned = pd.DataFrame(columns=FINAL_COLS) # 创建一个标准空DataFrame
            else:
                # 如果一开始就没下载到数据，也创建一个标准空DataFrame
                df_cleaned = pd.DataFrame(columns=FINAL_COLS)

            # 3. 统一处理 (无论 df_cleaned 是有数据还是空的)
            df_cleaned['code'] = code # 总是添加 code 列
            
            if 'date' in df_cleaned.columns:
                df_cleaned['date'] = pd.to_datetime(df_cleaned['date'], errors='coerce')
            
            numeric_cols = [c for c in FINAL_COLS if c not in ['date', 'code']]
            for col in numeric_cols:
                if col not in df_cleaned.columns:
                    df_cleaned[col] = pd.NA # 确保所有数值列都存在
                df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')

            # 单位转换（只对有数据的DataFrame有效）
            if not df_cleaned.empty:
                money_cols = [c for c in df_cleaned.columns if 'amount' in c or 'flow' in c]
                if money_cols:
                    df_cleaned.loc[:, money_cols] = df_cleaned[money_cols] * 10000

            # 4. 最终排序与保存
            # 确保列序一致
            df_final = df_cleaned.reindex(columns=FINAL_COLS)
            if not df_final.empty:
                df_final = df_final.sort_values('date').reset_index(drop=True)
            
            output_path = f"{OUTPUT_DIR}/{code}.parquet"
            df_final.to_parquet(output_path, index=False) # to_parquet可以完美处理空DataFrame
            
            if not df_final.empty:
                success_count += 1
            
        except Exception as e:
            print(f"  -> ❌ 在处理 {name} ({code}) 时发生严重错误: {e}")
    # --------------------------------------------------

    print(f"\n分区 {TASK_INDEX + 1} 完成！其中包含有效数据的股票有 {success_count}/{len(stocks)} 只。")
    # 不再需要任何 if success_count == 0 的判断

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌❌❌ 在 main 函数顶层捕获到致命异常: {e} ❌❌❌")
        traceback.print_exc()
        exit(1)
