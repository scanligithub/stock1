# scripts/collect_fundflow.py
# 2025-11-17 内存优化版：通过分块处理解决超大文件合并时的内存溢出问题

import os
import pandas as pd
import glob
from tqdm import tqdm
import json
from datetime import datetime
import shutil
import sys

# ==================== 配置 ====================
INPUT_BASE_DIR = "all_fundflow"
SMALL_OUTPUT_DIR = "fundflow_small"
FINAL_PARQUET_FILE = "full_fundflow.parquet"
QUALITY_REPORT_FILE = "data_quality_report_fundflow.json"

os.makedirs(SMALL_OUTPUT_DIR, exist_ok=True)

# ==================== 统一字段修复函数 ====================
def unify_columns(df: pd.DataFrame) -> pd.DataFrame:
    """把个股(新浪)和指数(东财)的字段彻底统一"""
    # 1. 东财指数返回的 open → close（最常见的冲突）
    if 'open' in df.columns and 'close' not in df.columns:
        df = df.rename(columns={'open': 'close'})
    elif 'open' in df.columns and 'close' in df.columns:
        df = df.drop(columns=['open'])

    # 2. 删除指数特有的干扰列
    unwanted = ['high', 'low', 'volume', 'amount', 'pre_close', 'open_interest']
    df = df.drop(columns=[c for c in unwanted if c in df.columns], errors='ignore')

    # 3. 确保标准列存在（缺失的补 NaN）
    required_cols = [
        'date', 'code', 'close', 'pct_change', 'turnover_rate',
        'net_flow_amount', 'main_net_flow', 'super_large_net_flow',
        'large_net_flow', 'medium_small_net_flow'
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.NA

    # 4. 强制数值列为 numeric
    numeric_cols = [
        'close', 'pct_change', 'turnover_rate', 'net_flow_amount',
        'main_net_flow', 'super_large_net_flow', 'large_net_flow', 'medium_small_net_flow'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 5. 日期列统一为 datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

    return df[required_cols]

# ==================== 主流程 ====================
def main():
    print("开始 资金流数据收集与合并流程...")

    search_pattern = os.path.join(INPUT_BASE_DIR, "**", "*.parquet")
    files = glob.glob(search_pattern, recursive=True)
    
    if not files:
        print("没有找到任何分片文件，退出。")
        return
        
    print(f"发现 {len(files)} 个资金流分片文件，开始处理...")

    # --- (这是唯一的、关键的修正) ---
    # --- 分块处理以节省内存 ---
    
    # 1. 先只复制小文件
    if os.path.exists(SMALL_OUTPUT_DIR):
        shutil.rmtree(SMALL_OUTPUT_DIR)
    os.makedirs(SMALL_OUTPUT_DIR, exist_ok=True)
    for f in tqdm(files, desc="复制资金流小文件"):
        filename = os.path.basename(f)
        shutil.copy2(f, os.path.join(SMALL_OUTPUT_DIR, filename))
    print(f"所有小文件已收集至 {SMALL_OUTPUT_DIR}/")

    # 2. 分块读取、处理并合并
    chunk_size = 2000 # 每次处理 2000 个文件，可以根据内存情况调整
    final_df = pd.DataFrame()
    
    print(f"\n将分块读取和合并，每块 {chunk_size} 个文件...")

    for i in tqdm(range(0, len(files), chunk_size), desc="分块合并中"):
        chunk_files = files[i : i + chunk_size]
        # 读取当前块的所有 DataFrame
        dfs = [unify_columns(pd.read_parquet(f)) for f in chunk_files]
        # 合并当前块
        chunk_df = pd.concat(dfs, ignore_index=True)
        # 将当前块合并到最终的 DataFrame 中
        final_df = pd.concat([final_df, chunk_df], ignore_index=True)
        
    # ------------------------------------------
    
    print(f"\n合并完成，总行数：{len(final_df):,}")

    # 3. 按 code + date 排序（极大提升压缩率）
    print("按 code + date 排序优化压缩...")
    final_df = final_df.sort_values(['code', 'date']).reset_index(drop=True)

    # 4. 保存最终大文件（优先 ZSTD，失败回退 snappy）
    print(f"正在写入最终合并文件：{FINAL_PARQUET_FILE}...")
    try:
        final_df.to_parquet(FINAL_PARQUET_FILE, index=False, compression='zstd')
        print("ZSTD 压缩成功！")
    except Exception as e:
        print(f"ZSTD失败，回退 snappy：{e}")
        final_df.to_parquet(FINAL_PARQUET_FILE, index=False, compression='snappy')

    # 5. 生成质检报告
    print("\n正在生成质检报告...")
    report = {
        "generate_time": datetime.now().isoformat(),
        "total_rows": len(final_df),
        "total_stocks": final_df['code'].nunique(),
        "date_range": {
            "min": final_df['date'].min().date().isoformat() if pd.notna(final_df['date'].min()) else None,
            "max": final_df['date'].max().date().isoformat() if pd.notna(final_df['date'].max()) else None
        },
        "columns": list(final_df.columns),
        "dtypes": final_df.dtypes.apply(lambda x: str(x)).to_dict()
    }
    with open(QUALITY_REPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"质检报告已生成：{QUALITY_REPORT_FILE}")

    print("\n资金流全市场数据合并完成！")
    print(f"→ 总行数：{report['total_rows']:,}")
    print(f"→ 股票数：{report['total_stocks']:,}")
    print(f"→ 日期范围：{report['date_range']['min']} ~ {report['date_range']['max']}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌❌❌ 在 main 函数顶层捕获到致命异常: {e} ❌❌❌")
        import traceback
        traceback.print_exc()
        exit(1)
