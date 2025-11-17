# scripts/collect_fundflow.py
# 2025-11-17 终极生产版：完美兼容个股(新浪)+指数(东财)，ZSTD永不崩
import os
import pandas as pd
import glob
from tqdm import tqdm
import json
from datetime import datetime

# ==================== 配置 ====================
INPUT_BASE_DIR = "all_fundflow"           # download-artifact 下载下来的目录
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
    print(f"发现 {len(files)} 个资金流分片文件，开始收集...")

    # 1. 复制小文件（便于手动检查）
    for f in tqdm(files, desc="复制资金流小文件"):
        filename = os.path.basename(f)
        os.system(f"cp '{f}' '{SMALL_OUTPUT_DIR}/{filename}'")
    print(f"所有小文件已收集至 {SMALL_OUTPUT_DIR}/")

    # 2. 读取并合并
    print("正在读取并合并所有资金流数据...")
    dfs = []
    for f in tqdm(files, desc="读取分片"):
        try:
            df = pd.read_parquet(f)
            df = unify_columns(df)          # 关键修复在这儿
            dfs.append(df)
        except Exception as e:
            print(f"读取失败 {f} : {e}")

    if not dfs:
        print("没有成功读取任何文件，退出")
        return

    merged = pd.concat(dfs, ignore_index=True)
    print(f"合并完成，总行数：{len(merged):,}")

    # 3. 按 code + date 排序（极大提升压缩率）
    print("按 code + date 排序优化压缩...")
    merged = merged.sort_values(['code', 'date']).reset_index(drop=True)

    # 4. 保存最终大文件（优先 ZSTD，失败回退 snappy）
    print(f"正在写入最终合并文件：{FINAL_PARQUET_FILE}（ZSTD压缩）")
    try:
        merged.to_parquet(FINAL_PARQUET_FILE, index=False, compression='zstd')
        print("ZSTD 压缩成功！")
    except Exception as e:
        print(f"ZSTD失败，回退 snappy：{e}")
        merged.to_parquet(FINAL_PARQUET_FILE, index=False, compression='snappy')

    # 5. 生成质检报告
    report = {
        "generate_time": datetime.now().isoformat(),
        "total_rows": len(merged),
        "total_stocks": merged['code'].nunique(),
        "date_range": {
            "min": merged['date'].min().date().isoformat() if pd.notna(merged['date'].min()) else None,
            "max": merged['date'].max().date().isoformat() if
            pd.notna(merged['date'].max()) else None
        },
        "columns": list(merged.columns),
        "dtypes": merged.dtypes.apply(lambda x: str(x)).to_dict()
    }
    with open(QUALITY_REPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"质检报告已生成：{QUALITY_REPORT_FILE}")

    print("\n资金流全市场数据合并完成！")
    print(f"→ 总行数：{report['total_rows']:,}")
    print(f"→ 股票数：{report['total_stocks']:,}")
    print(f"→ 日期范围：{report['date_range']['min']} ~ {report['date_range']['max']}")

if __name__ == "__main__":
    main()
