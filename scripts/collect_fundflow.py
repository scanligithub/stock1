# scripts/collect_fundflow.py
# 2025-11-18 PyArrow 兼容性修复版：在写入 Parquet 前，将 datetime 对象转换为字符串

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

# ==================== 系统资源监控函数 ====================
def print_system_stats():
    """打印当前的内存和硬盘使用情况"""
    print("-" * 20)
    try:
        import psutil
        mem = psutil.virtual_memory()
        print(f"  -> 📊 RAM Usage: {mem.used / (1024**3):.2f} GB / {mem.total / (1024**3):.2f} GB ({mem.percent}%)")
    except ImportError:
        print("  -> 📊 RAM Usage: psutil not installed.")
    try:
        disk = shutil.disk_usage("/")
        print(f"  -> 📊 Disk Usage: {disk.used / (1024**3):.2f} GB / {disk.total / (1024**3):.2f} GB ({disk.used / disk.total * 100:.1f}%)")
    except Exception as e:
        print(f"  -> 📊 Disk Usage: Failed to get info: {e}")
    print("-" * 20)


# ==================== 统一字段修复函数 ====================
def unify_columns(df: pd.DataFrame) -> pd.DataFrame:
    """把个股(新浪)和指数(东财)的字段彻底统一"""
    if 'open' in df.columns and 'close' not in df.columns:
        df = df.rename(columns={'open': 'close'})
    elif 'open' in df.columns and 'close' in df.columns:
        df = df.drop(columns=['open'])
    unwanted = ['high', 'low', 'volume', 'amount', 'pre_close', 'open_interest']
    df = df.drop(columns=[c for c in unwanted if c in df.columns], errors='ignore')
    required_cols = ['date', 'code', 'close', 'pct_change', 'turnover_rate', 'net_flow_amount', 'main_net_flow', 'super_large_net_flow', 'large_net_flow', 'medium_small_net_flow']
    for col in required_cols:
        if col not in df.columns: df[col] = pd.NA
    numeric_cols = ['close', 'pct_change', 'turnover_rate', 'net_flow_amount', 'main_net_flow', 'super_large_net_flow', 'large_net_flow', 'medium_small_net_flow']
    for col in numeric_cols:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    elif 'opendate' in df.columns: # 增加一个 fallback
        df = df.rename(columns={'opendate': 'date'})
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
    final_cols = [c for c in required_cols if c in df.columns]
    return df[final_cols]

# ==================== 主流程 ====================
def main():
    if not PYARROW_DUCKDB_AVAILABLE:
        print("❌ 致命错误: 未找到 'pyarrow' 或 'duckdb' 库。")
        sys.exit(1)
        
    print("开始 资金流数据收集与合并流程...")
    print_system_stats()

    search_pattern = os.path.join(INPUT_BASE_DIR, "**", "*.parquet")
    files = glob.glob(search_pattern, recursive=True)
    
    if not files:
        print("没有找到任何分片文件，退出。"); return
        
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
            
            # (注意) unify_columns 返回的 df 中, 'date' 列是 datetime 类型
            dfs = [unify_columns(pd.read_parquet(f)) for f in chunk_files]
            chunk_df = pd.concat(dfs, ignore_index=True)
            
            # --- (这是唯一的、关键的修正) ---
            # 在转换为 Arrow Table 之前，确保 date 列是字符串或 Arrow 兼容的类型
            if 'date' in chunk_df.columns and pd.api.types.is_datetime64_any_dtype(chunk_df['date']):
                # 方法1：转换为字符串 (最安全)
                chunk_df['date'] = chunk_df['date'].dt.strftime('%Y-%m-%d')
                # 将 NaT (Not a Time) 转换为空值，以便 PyArrow 正确处理
                chunk_df['date'].replace({pd.NaT: None}, inplace=True)
            # ------------------------------------

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
    print(f"\n合并写入完成，文件为 {TEMP_UNSORTED_FILE}。准备使用 DuckDB 进行外部排序...")
    print_system_stats()
    
    try:
        con = duckdb.connect()
        con.execute("SET memory_limit='5GB';") 
        query = f"""
        COPY (
            SELECT * 
            FROM read_parquet('{TEMP_UNSORTED_FILE}')
            ORDER BY code, date
        ) TO '{FINAL_PARQUET_FILE}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
        """
        print("... 正在执行 DuckDB 排序和写入 ...")
        con.execute(query)
        con.close()
        print(f"✅ DuckDB 排序完成！已生成最终排序文件: {FINAL_PARQUET_FILE}")
        os.remove(TEMP_UNSORTED_FILE)
        print_system_stats()
    except Exception as e:
        print(f"\n❌ 在 DuckDB 排序阶段发生错误: {e}"); traceback.print_exc()
        if os.path.exists(TEMP_UNSORTED_FILE):
            os.rename(TEMP_UNSORTED_FILE, FINAL_PARQUET_FILE)

    # --- 阶段 4: 生成质检报告 ---
    print("\n正在读取最终文件进行质检...")
    if os.path.exists(FINAL_PARQUET_FILE):
        final_df = pd.read_parquet(FINAL_PARQUET_FILE)
        # (重要) 在质检前，将 date 列转回 datetime
        final_df['date'] = pd.to_datetime(final_df['date'], errors='coerce')

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
    else:
        print("⚠️ 未找到最终的 Parquet 文件，无法生成质检报告。")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌❌❌ 在 main 函数顶层捕获到致命异常: {e} ❌❌❌")
        traceback.print_exc()
        exit(1)
