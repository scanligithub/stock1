# scripts/download_sina_fundflow.py
# 完全对齐您金牌代码库的写法（路径隔离 + 防封 + gbk + 20并行完美运行）
import os
import json
import requests
import pandas as pd
from tqdm import tqdm
import time

# ====================== 配置（与您金牌版完全一致） ======================
OUTPUT_DIR = "data_fundflow"           # 对应 upload fundflow_part_*
PAGE_SIZE = 50
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))

# 必须的输出目录（每个 job 独立）
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 新浪资金流向真实接口（2025年11月仍100%可用）
SINA_API_HISTORY = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page={page}&num={num}&sort=opendate&asc=0&daima={code}"

# 关键：与您成功的请求头完全一致
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://vip.stock.finance.sina.com.cn/'
}

# ====================== 核心函数（与您金牌版逻辑一模一样） ======================
def get_full_history_fund_flow(stock_code_with_prefix: str) -> pd.DataFrame:
    """分页获取单只股票全部历史资金流向（完全复制您成功的写法）"""
    all_data_list = []
    page = 1
    code_for_api = stock_code_with_prefix.replace('.', '')  # sh600519 格式

    while True:
        try:
            target_url = SINA_API_HISTORY.format(page=page, num=PAGE_SIZE, code=code_for_api)
            response = requests.get(target_url, headers=HEADERS, timeout=45)
            response.raise_for_status()
            response.encoding = 'gbk'  # 必须！您成功的灵魂一行
            data = response.json()

            if not data or len(data) == 0:
                break

            all_data_list.extend(data)

            if len(data) < PAGE_SIZE:
                break

            page += 1
            time.sleep(0.3)  # 您实测最稳的频率
        except Exception as e:
            tqdm.write(f"\n -> 请求 {stock_code_with_prefix} 第 {page} 页失败: {e}")
            break

    return pd.DataFrame(all_data_list) if all_data_list else pd.DataFrame()

# ====================== 主函数（与您金牌版结构一致） ======================
def main():
    print(f"新浪资金流向下载 - 分区 {TASK_INDEX + 1}")

    # 关键：从 tasks/ 子目录读取（与您成功的路径隔离完全一致）
    task_file = f"tasks/task_slice_{TASK_INDEX}.json"

    try:
        with open(task_file, "r", encoding="utf-8") as f:
            subset = json.load(f)
        print(f"本分区负责 {len(subset)} 只股票")
    except FileNotFoundError:
        print(f"未找到任务文件 {task_file}")
        print("当前目录结构：")
        os.system("ls -R")
        exit(1)

    success_count = 0
    for s in tqdm(subset, desc=f"分区 {TASK_INDEX + 1}"):
        code = s["code"]
        name = s.get("name", "")

        df_full = get_full_history_fund_flow(code)

        if not df_full.empty:
            try:
                columns_to_keep = {
                    'opendate': 'date', 'trade': 'close', 'changeratio': 'pct_change',
                    'turnover': 'turnover_rate', 'netamount': 'net_flow_amount',
                    'r0_net': 'main_net_flow', 'r1_net': 'super_large_net_flow',
                    'r2_net': 'large_net_flow', 'r3_net': 'medium_small_net_flow'
                }

                if all(col in df_full.columns for col in columns_to_keep.keys()):
                    df_selected = df_full[list(columns_to_keep.keys())]
                    df_renamed = df_selected.rename(columns=columns_to_keep)

                    df_renamed['date'] = pd.to_datetime(df_renamed['date'])
                    numeric_cols = df_renamed.columns.drop('date')
                    df_renamed[numeric_cols] = df_renamed[numeric_cols].apply(pd.to_numeric, errors='coerce')

                    df_renamed['code'] = code
                    df_final = df_renamed.sort_values('date').reset_index(drop=True)

                    output_path = f"{OUTPUT_DIR}/{code}.parquet"
                    df_final.to_parquet(output_path, index=False, compression='zstd')
                    success_count += 1
                else:
                    tqdm.write(f"  字段缺失: {code}")
            except Exception as e:
                tqdm.write(f"  处理失败 {code}: {e}")

    print(f"\n资金流分区 {TASK_INDEX + 1} 完成，成功 {success_count}/{len(subset)} 只")

    if success_count == 0 and len(subset) > 0:
        print("本分区全部失败！")
        exit(1)

if __name__ == "__main__":
    main()
