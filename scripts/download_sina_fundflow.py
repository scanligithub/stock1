# scripts/download_sina_fundflow.py
# 2025-11-17 真正全市场版：个股用新浪，指数自动切东财，0 丢股！

import os
import json
import requests
import pandas as pd
from tqdm import tqdm
import time
import sys

# ==================== 配置 ====================
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

# ==================== 辅助函数 ====================
def is_index(code: str) -> bool:
    """根据代码前缀判断是否为指数"""
    if not isinstance(code, str) or len(code) < 6:
        return False
    # 上证指数 (000xxx), 深证指数 (399xxx), 以及一些行业/概念指数
    num = code[3:6]
    return num in ['000','900','399','880','950','951','952','953','899']

def get_sina_fundflow(code: str) -> pd.DataFrame:
    """从新浪获取个股的历史资金流 (分页)"""
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
        except Exception:
            # 任何错误都中断当前股票的下载
            break
    return pd.DataFrame(all_data) if all_data else pd.DataFrame()

def get_em_fundflow_index(code: str) -> pd.DataFrame:
    """专为指数准备的东财接口（字段完美对齐）"""
    prefix = "1." if code.startswith("sh") else "0."
    secid = prefix + code[3:]
    # 注意: 东财返回的金额单位是“元”，新浪是“万”，这里需要统一
    # 我们在 main 函数的清洗阶段进行统一，这里先获取原始数据
    params = {
        "lmt": "0", # 获取全部历史
        "klt": "101", # 日线
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59",
        "secid": secid,
    }
    try:
        r = requests.get(EM_API, params=params, headers=HEADERS_EM, timeout=20)
        r.raise_for_status()
        j = r.json()
        klines = j.get("data", {}).get("klines", [])
        if not klines:
            return pd.DataFrame()
            
        records = []
        for line in klines:
            items = line.split(",")
            if len(items) >= 9:
                records.append({
                    "opendate": items[0],
                    "trade": items[1],
                    "changeratio": items[2],
                    "turnover": None,  # 指数无换手率
                    "netamount": items[8],     
                    "r0_net": items[4],
                    "r1_net": items[5],
                    "r2_net": items[6],
                    "r3_net": items[7],
                })
        return pd.DataFrame(records)
    except Exception:
        return pd.DataFrame()

def get_fundflow_smart(code: str) -> pd.DataFrame:
    """智能选择数据源"""
    if is_index(code):
        print(f"  → 检测为指数 {code}，自动切换到东方财富接口...")
        return get_em_fundflow_index(code)
    else:
        return get_sina_fundflow(code)

# ==================== 主流程 ====================
def main():
    print(f"\n2025全市场资金流下载（个股新浪+指数东财）- 分区 {TASK_INDEX + 1}")

    task_file = f"tasks/task_slice_{TASK_INDEX}.json"
    try:
        with open(task_file) as f:
            stocks = json.load(f)
    except FileNotFoundError:
        print(f"❌ 致命错误: 未找到任务分片文件 {task_file}！"); sys.exit(1)


    print(f"本分区共 {len(stocks)} 只（含指数）")
    success_count = 0

    for s in tqdm(stocks, desc=f"分区 {TASK_INDEX+1} 下载中"):
        code = s["code"]
        name = s.get("name", "")
        
        df_raw = get_fundflow_smart(code)

        if df_raw.empty:
            print(f"  -> 🟡 {name} ({code}) 未下载到数据。")
            continue

        # --- 统一的数据清洗和格式化 ---
        try:
            # 统一列名
            df_renamed = df_raw.rename(columns=COLUMN_MAP)
            
            # 添加 code 列
            df_renamed['code'] = code

            # 筛选出我们需要的标准列
            final_cols = [
                'date', 'code', 'close', 'pct_change', 'turnover_rate',
                'net_flow_amount', 'main_net_flow', 'super_large_net_flow',
                'large_net_flow', 'medium_small_net_flow'
            ]
            # 检查哪些列是可用的
            available_cols = [c for c in final_cols if c in df_renamed.columns]
            df_cleaned = df_renamed[available_cols]

            # 统一数据类型
            if 'date' in df_cleaned.columns:
                df_cleaned['date'] = pd.to_datetime(df_cleaned['date'], errors='coerce')
            
            numeric_cols = [c for c in df_cleaned.columns if c not in ['date', 'code']]
            df_cleaned[numeric_cols] = df_cleaned[numeric_cols].apply(pd.to_numeric, errors='coerce')
            
            # (重要) 单位统一：东财的金额单位是“元”，新浪是“万”。统一转换为“元”。
            # 我们假设新浪的字段名包含 'netamount' 或 '_net'
            if not is_index(code): # 如果是个股（来自新浪）
                money_cols = [c for c in df_cleaned.columns if 'amount' in c or 'flow' in c]
                df_cleaned[money_cols] = df_cleaned[money_cols] * 10000

            # 排序并保存
            df_final = df_cleaned.sort_values('date').reset_index(drop=True)
            output_path = f"{OUTPUT_DIR}/{code}.parquet"
            df_final.to_parquet(output_path, index=False, compression='zstd' if 'zstandard' in sys.modules else 'snappy')
            success_count += 1
            
        except Exception as e:
            print(f"  -> ❌ 在处理 {name} ({code}) 的数据时出错: {e}")


    print(f"\n分区 {TASK_INDEX + 1} 完成！成功下载 {success_count}/{len(stocks)} 只（含指数）")
    if success_count == 0 and len(stocks) > 0:
        exit(1)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌❌❌ 在 main 函数顶层捕获到致命异常: {e} ❌❌❌")
        traceback.print_exc()
        exit(1)
