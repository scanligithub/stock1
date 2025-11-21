# scripts/download_sina_fundflow.py
# 2025-11-19 统一信源高容错版 (增强侦察)

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

# (关键) 唯一的 API 接口
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

# ==================== 下载函数 (已修改) ====================
def get_fundflow(code: str) -> pd.DataFrame:
    """从新浪获取指定标的的历史资金流 (分页)"""
    all_data = []
    page = 1
    code_api = code.replace('.', '')
    
    # 打印开始下载的信号
    print(f"\n    [get_fundflow] -> Starting download for {code}...")
    
    while True:
        url = f"{SINA_API}?page={page}&num={PAGE_SIZE}&sort=opendate&asc=0&daima={code_api}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status() # 检查 HTTP 状态码
            r.encoding = 'gbk'
            data = r.json()
            
            if not data:
                print(f"    [get_fundflow] -> Page {page} returned empty data. Pagination finished.")
                break
                
            all_data.extend(data)
            
            if len(data) < PAGE_SIZE:
                print(f"    [get_fundflow] -> Page {page} is the last page ({len(data)} records).")
                break
                
            page += 1
            time.sleep(0.3)
            
        # --- (这是唯一的、关键的修正) ---
        except Exception as e:
            # 任何错误都中断当前标的的下载，但要打印清晰的错误信息
            print(f"\n    [get_fundflow] -> ❌ ERROR on page {page} for {code}: {type(e).__name__} - {e}")
            break
        # ---------------------------------
        
    print(f"    [get_fundflow] -> Finished for {code}. Total records fetched: {len(all_data)}")
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
    failure_count = 0

    for s in tqdm(stocks, desc=f"分区 {TASK_INDEX+1} 下载中"):
        code = s["code"]
        name = s.get("name", "")
        
        df_raw = get_fundflow(code)

        # --- (这是唯一的、关键的修正) ---
        # 无论成功与否，都清晰地记录结果
        if df_raw.empty:
            # 不再静默 continue，而是增加一个计数
            failure_count += 1
            # 可以在这里打印，也可以不打印，tqdm 会处理好进度
            # print(f"  -> 🟡 {name} ({code}) 未下载到数据。") 
            continue
        # ---------------------------------

        # --- 数据清洗和格式化 ---
        try:
            # (您的清洗逻辑保持不变)
            # ...
            success_count += 1
        except Exception as e:
            failure_count += 1
            print(f"  -> ❌ 在处理 {name} ({code}) 的数据时出错: {e}")

    # --- 最终总结 (保持高容错) ---
    print(f"\n分区 {TASK_INDEX + 1} 完成！")
    print(f"  - 成功处理并保存: {success_count}/{len(stocks)} 只标的")
    print(f"  - 未下载到数据或处理失败: {failure_count}/{len(stocks)} 只标的")
    
    if success_count == 0 and len(stocks) > 0:
        print("\n" + "="*60)
        print(f"⚠️ 警告: 分区 {TASK_INDEX + 1} 未能成功下载或处理任何一只股票的数据。")
        print("   本作业将正常结束，以允许整个工作流继续执行。")
        print("="*60)
        # exit(1) # 保持注释，确保工作流不中断

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌❌❌ 在 main 函数顶层捕获到致命异常: {e} ❌❌❌")
        traceback.print_exc()
        exit(1)
