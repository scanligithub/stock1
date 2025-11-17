# scripts/download_sina_fundflow.py
# 增强调试版：每一步都打印详细信息，便于定位“成功 0/N”的根本原因
import os
import json
import requests
import pandas as pd
from tqdm import tqdm
import time

# ====================== 配置 ======================
OUTPUT_DIR = "data_fundflow"
PAGE_SIZE = 50
SINA_API_HISTORY = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page={page}&num={num}&sort=opendate&asc=0&daima={code}"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://vip.stock.finance.sina.com.cn/'
}

TASK_INDEX = int(os.getenv("TASK_INDEX", 0))
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_full_history_fund_flow(stock_code_with_prefix):
    """分页获取资金流向，带详细调试日志"""
    all_data_list = []
    page = 1
    code_for_api = stock_code_with_prefix.replace('.', '')

    print(f"  → 开始下载 {stock_code_with_prefix} (API代码: {code_for_api})")

    while True:
        url = SINA_API_HISTORY.format(page=page, num=PAGE_SIZE, code=code_for_api)
        try:
            response = requests.get(url, headers=HEADERS, timeout=45)
            print(f"    · 第 {page} 页 → Status {response.status_code} | 长度 {len(response.text)} 字符", end="")

            response.raise_for_status()
            response.encoding = 'gbk'

            data = response.json()
            print(f" → 解析得到 {len(data)} 条记录")

            if not data or len(data) == 0:
                print(f"    → 第 {page} 页返回空，结束循环")
                break

            all_data_list.extend(data)

            if len(data) < PAGE_SIZE:
                print(f"    → 已到最后一页（< {PAGE_SIZE} 条），结束")
                break

            page += 1
            time.sleep(0.3)
        except requests.exceptions.RequestException as e:
            print(f"\n    → 请求异常: {e}")
            break
        except Exception as e:
            print(f"\n    → 解析异常: {e}")
            print(f"      原始响应前200字符: {response.text[:200]!r}")
            break

    print(f"  → {stock_code_with_prefix} 总共获取到 {len(all_data_list)} 条原始记录")
    return pd.DataFrame(all_data_list) if all_data_list else pd.DataFrame()

def main():
    print("="*70)
    print(f"新浪资金流向下载（增强调试版） - 分区 {TASK_INDEX + 1}")
    print(f"当前工作目录: {os.getcwd()}")
    print("列出根目录内容:")
    os.system("ls -la")
    print("-"*70)

    task_file = f"tasks/task_slice_{TASK_INDEX}.json"
    print(f"准备读取任务文件: {task_file}")

    if not os.path.exists(task_file):
        print(f"文件不存在！当前 tasks/ 目录内容:")
        os.system("ls -la tasks/ || echo 'tasks/ 目录不存在'")
        exit(1)

    with open(task_file, "r", encoding="utf-8") as f:
        subset = json.load(f)

    print(f"成功加载任务分片，包含 {len(subset)} 只股票")
    if len(subset) <= 10:
        print("前几只股票示例:", [s["code"] for s in subset[:5]])

    success_count = 0
    for s in tqdm(subset, desc=f"分区 {TASK_INDEX + 1}"):
        code = s["code"]
        name = s.get("name", "")

        df_full = get_full_history_fund_flow(code)

        if df_full.empty:
            print(f"  → {code} 返回空 DataFrame，跳过保存")
            continue

        # 检查关键字段
        required = {'opendate', 'trade', 'changeratio', 'turnover', 'netamount',
                    'r0_net', 'r1_net', 'r2_net', 'r3_net'}
        missing = required - set(df_full.columns)
        if missing:
            print(f"  → {code} 缺少关键字段: {missing}，实际列: {list(df_full.columns)}")
            continue

        try:
            columns_to_keep = {
                'opendate': 'date', 'trade': 'close', 'changeratio': 'pct_change',
                'turnover': 'turnover_rate', 'netamount': 'net_flow_amount',
                'r0_net': 'main_net_flow', 'r1_net': 'super_large_net_flow',
                'r2_net': 'large_net_flow', 'r3_net': 'medium_small_net_flow'
            }
            df_clean = df_full[list(columns_to_keep.keys())].rename(columns=columns_to_keep)
            df_clean['date'] = pd.to_datetime(df_clean['date'])
            numeric_cols = df_clean.columns.drop('date')
            df_clean[numeric_cols] = df_clean[numeric_cols].apply(pd.to_numeric, errors='coerce')
            df_clean['code'] = code
            df_clean = df_clean.sort_values('date').reset_index(drop=True)

            output_path = f"{OUTPUT_DIR}/{code}.parquet"
            df_clean.to_parquet(output_path, index=False, compression='zstd')
            print(f"  → 已保存 {output_path} ({len(df_clean)} 行)")
            success_count += 1
        except Exception as e:
            print(f"  → 保存 {code} 时出错: {e}")

    print("="*70)
    print(f"分区 {TASK_INDEX + 1} 最终结果：成功 {success_count}/{len(subset)} 只")
    if success_count == 0 and len(subset) > 0:
        print("本分区全部失败！请根据上面日志定位问题")
        exit(1)
    print("本分区正常结束")
    print("="*70)

if __name__ == "__main__":
    main()
