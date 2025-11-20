# scripts/collect_fundflow.py
# 2025-11-19 终极合并版：流式写入 + DuckDB排序 + 高级数据质量检查

import os
import pandas as pd
import glob
from tqdm import tqdm
import json
from datetime import datetime
import shutil
import sys
import traceback

# 尝试导入核心库
try:
    import pyarrow.parquet as pq
    import pyarrow as pa
    import duckdb
    PYARROW_DUCKDB_AVAILABLE = True
except ImportError:
    PYARROW_DUCKDB_AVAILABLE = False

# ==================== 配置 ====================
INPUT_BASE_DIR = "all_fundflow"
SMALL_OUTPUT_DIR = "fundflow_small"
TEMP_UNSORTED_FILE = "full_fundflow_unsorted.parquet"
FINAL_PARQUET_FILE = "full_fundflow.parquet"
QUALITY_REPORT_FILE = "data_quality_report_fundflow.json"

os.makedirs(SMALL_OUTPUT_DIR, exist_ok=True)

# ==================== 系统资源监控函数 (保持不变) ====================
def print_system_stats():
    # ... (此函数内容与您之前的版本完全相同)
    pass

# ==================== 统一字段修复函数 (保持不变) ====================
def unify_columns(df: pd.DataFrame) -> pd.DataFrame:
    # ... (此函数内容与您之前的版本完全相同)
    pass

# ==================== (新增) 高级数据质量检查函数 ====================
def run_advanced_quality_check():
    """
    直接分析 'fundflow_small/' 下的所有独立小文件，生成高级质检报告。
    """
    print("\n" + "="*50)
    print("🔍 [QC] 开始进行高级数据质量检查...")
    
    small_files_path = os.path.join(SMALL_OUTPUT_DIR, "*.parquet")
    small_files = glob.glob(small_files_path)

    if not small_files:
        print("⚠️ [QC] 未在 fundflow_small/ 目录中找到任何文件，无法生成质检报告。")
        return

    print(f"  -> [QC] 将对 {len(small_files)} 个独立的股票文件进行分析...")
    
    stock_reports = []
    total_records = 0
    total_error_records = 0
    all_dates = []

    for f in tqdm(small_files, desc="[QC] 正在分析每只股票"):
        try:
            df = pd.read_parquet(f)
            if df.empty:
                continue

            # 假设文件名就是股票代码 (sh.600000.parquet)
            code = os.path.splitext(os.path.basename(f))[0]
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df.dropna(subset=['date'], inplace=True)
            
            start_date = df['date'].min()
            end_date = df['date'].max()
            record_count = len(df)
            total_records += record_count
            all_dates.extend([start_date, end_date])
            
            expected_dates = pd.date_range(start=start_date, end=end_date, freq='B')
            missing_days = len(expected_dates.difference(df['date']))
            
            flow_cols = ['net_flow_amount', 'main_net_flow', 'super_large_net_flow', 'large_net_flow', 'medium_small_net_flow']
            error_rows = df[df[flow_cols].isnull().all(axis=1) | (df[flow_cols] == 0).all(axis=1)].shape[0]
            total_error_records += error_rows
            
            stock_reports.append({
                "code": code,
                "record_count": record_count,
                "start_date": start_date.strftime('%Y-%m-%d'),
                "end_date": end_date.strftime('%Y-%m-%d'),
                "missing_business_days": missing_days,
                "error_records_count": error_rows
            })
        except Exception as e:
            print(f"\n⚠️ [QC] 分析文件 {f} 失败: {e}")

    print("\n... [QC] 正在生成最终汇总报告 ...")
    final_report = {
        "generate_time": datetime.now().isoformat(),
        "total_stocks_processed": len(stock_reports),
        "total_records_analyzed": total_records,
        "total_error_records_found": total_error_records,
        "global_date_range": {
            "min": min(all_dates).strftime('%Y-%m-%d') if all_dates else None,
            "max": max(all_dates).strftime('%Y-%m-%d') if all_dates else None
        },
        "per_stock_details": stock_reports
    }

    with open(QUALITY_REPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(final_report, f, ensure_ascii=False, indent=2)
    print(f"✅ [QC] 高级质检报告已生成：{QUALITY_REPORT_FILE}")

    print("\n--- 资金流数据质量简报 ---")
    print(f"→ 标的总数（分析成功）: {final_report['total_stocks_processed']:,}")
    print(f"→ 总记录数：{final_report['total_records_analyzed']:,}")
    print(f"→ 异常记录数（全为0或空）: {final_report['total_error_records_found']:,}")
    date_range = final_report.get('global_date_range', {})
    print(f"→ 全局日期范围：{date_range.get('min')} ~ {date_range.get('max')}")

# ==================== 主流程 ====================
def main():
    if not PYARROW_DUCKDB_AVAILABLE:
        print("❌ 致命错误: 未找到 'pyarrow' 或 'duckdb' 库。")
        sys.exit(1)
        
    print("开始 资金流数据收集与合并流程...")
    print_system_stats()

    search_pattern = os.path.join(INPUT_BASE_DIR, "**", "*.parquet")
    files = glob.glob(search_pattern, recursive=True)
    if not files: print("没有找到任何分片文件，退出。"); return
    print(f"发现 {len(files)} 个资金流分片文件，开始处理...")

    # --- 阶段 1: 复制小文件 ---
    if os.path.exists(SMALL_OUTPUT_DIR): shutil.rmtree(SMALL_OUTPUT_DIR)
    os.makedirs(SMALL_OUTPUT_DIR, exist_ok=True)
    for f in tqdm(files, desc="复制资金流小文件"):
        filename = os.path.basename(f)
        shutil.copy2(f, os.path.join(SMALL_OUTPUT_DIR, filename))
    print(f"所有小文件已收集至 {SMALL_OUTPUT_DIR}/")

    # --- 阶段 2: 流式写入未排序的合并文件 ---
    chunk_size = 2000
    writer = None
    print(f"\n将以流式写入模式合并，每块 {chunk_size} 个文件...")
    try:
        for i in tqdm(range(0, len(files), chunk_size), desc="分块写入 Parquet 中"):
            chunk_files = files[i : i + chunk_size]
            
            # (重要) 在这里，我们不再需要调用 unify_columns，因为我们信任上游
            # 但为了修复 code 丢失的问题，我们必须在这里处理
            dfs = []
            for f in chunk_files:
                df = pd.read_parquet(f)
                # 从文件名注入 code
                filename = os.path.basename(f)
                code = os.path.splitext(filename)[0]
                df['code'] = code
                dfs.append(df)

            chunk_df = pd.concat(dfs, ignore_index=True)
            table = pa.Table.from_pandas(chunk_df, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(TEMP_UNSORTED_FILE, table.schema, compression='zstd' if 'zstandard' in sys.modules else 'snappy')
            writer.write_table(table)
            print(f"\n块 {i//chunk_size + 1} 写入完成。")
            print_system_stats()
    finally:
        if writer:
            writer.close()
            print("\nParquet writer 已关闭。")

    # --- 阶段 3: 使用 DuckDB 进行内存安全的外部排序 ---
    print(f"\n合并写入完成... 准备使用 DuckDB 进行外部排序...")
    # ... (DuckDB 排序逻辑保持不变) ...

    # --- 阶段 4: 生成高级质检报告 ---
    # 调用新的、功能更强大的质检函数
    run_advanced_quality_check()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌❌❌ 在 main 函数顶层捕获到致命异常: {e} ❌❌❌")
        traceback.print_exc()
        exit(1)
