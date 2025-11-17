# scripts/download_sina_fundflow.py
# 2025年11月16日更新版：新浪资金流向批量稳定下载（已绕过并发封禁）
import os
import json
import requests
import pandas as pd
import time
import random
from tqdm import tqdm

# ====================== 配置 ======================
OUTPUT_DIR = "data_fundflow"
PAGE_SIZE = 50
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 新浪资金流向真实可用接口（2025年11月仍稳定）
SINA_API = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb"

# 关键：更真实的浏览器头 + 动态 Referer
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Referer": "https://vip.stock.finance.sina.com.cn/moneyflow/",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "X-Requested-With": "XMLHttpRequest",
}

# 字段映射（与您测试程序完全一致）
COLUMN_MAP = {
    'opendate': 'date',
    'trade': 'close',
    'changeratio': 'pct_change',
    'turnover': 'turnover_rate',
    'netamount': 'net_flow_amount',
    'r0_net': 'main_net_flow',
    'r1_net': 'super_large_net_flow',
    'r2_net': 'large_net_flow',
    'r3_net': 'medium_small_net_flow'
}

# ====================== 核心函数 ======================
def get_fundflow_sina(code: str) -> pd.DataFrame:
    """分页获取单只股票全部历史资金流向（带防封优化）"""
    all_data = []
    page = 1
    code_api = code.replace('.', '')  # sh600519 格式

    while True:
        url = f"{SINA_API}?page={page}&num={PAGE_SIZE}&sort=opendate&asc=0&daima={code_api}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            resp.encoding = 'gbk'  # 必须！
            data = resp.json()

            if not data or len(data) == 0:
                break

            all_data.extend(data)

            if len(data) < PAGE_SIZE:
                break

            page += 1
            time.sleep(random.uniform(0.25, 0.55))  # 关键：随机延时，模拟人类
        except Exception as e:
            tqdm.write(f"  [错误] {code} 第{page}页请求失败: {e}")
            break

    return pd.DataFrame(all_data) if all_data else pd.DataFrame()

# ====================== 主函数 ======================
def main():
    print(f"新浪资金流向下载（优化防封版） - 分区 {TASK_INDEX + 1}")
    task_file = f"tasks/task_slice_{TASK_INDEX}.json"

    try:
        with open(task_file, "r", encoding="utf-8") as f:
            subset = json.load(f)
        print(f"本分区共 {len(subset)} 只股票")
    except FileNotFoundError:
        print(f"未找到任务文件 {task_file}，跳过")
        return

    if not subset:
        print("本分区为空，直接完成")
        return

    success_count = 0
    for stock in tqdm(subset, desc=f"分区 {TASK_INDEX+1} 进度"):
        code = stock["code"]
        try:
            df = get_fundflow_sina(code)
            if not df.empty and all(col in df.columns for col in COLUMN_MAP.keys()):
                df_clean = df[list(COLUMN_MAP.keys())].rename(columns=COLUMN_MAP).copy()
                df_clean['date'] = pd.to_datetime(df_clean['date'], errors='coerce')
                numeric_cols = df_clean.columns.drop('date')
                df_clean[numeric_cols] = df_clean[numeric_cols].apply(pd.to_numeric, errors='coerce')
                df_clean['code'] = code
                df_clean = df_clean.sort_values('date').reset_index(drop=True)

                output_path = f"{OUTPUT_DIR}/{code}.parquet"
                df_clean.to_parquet(output_path, index=False, compression='zstd')
                success_count += 1
            else:
                tqdm.write(f"  无数据或字段缺失: {code}")
        except Exception as e:
            tqdm.write(f"  处理失败 {code}: {e}")

        # 每只股票后随机延时（关键防封）
        time.sleep(random.uniform(0.3, 0.7))

    print(f"\n资金流分区 {TASK_INDEX + 1} 完成！成功下载 {success_count}/{len(subset)} 只股票")

    # 若全部分区都失败，主动报错让 Job 失败
    if success_count == 0 and len(subset) > 0:
        print("本分区全部失败，可能触发风控！")
        exit(1)

if __name__ == "__main__":
    main()
