# scripts/collect_fundflow.py
# 2025-11-19 最终全功能版：流式写入 + DuckDB排序 + 高级质检 + PyArrow兼容性修复 + 增强安检门

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

# 大文件预警阈值 (MB)，超过此大小仅打印日志，不跳过
LARGE_FILE_WARNING_THRESHOLD_MB = 50 

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
def unify_columns(df: pd.DataFrame, code: str) -> pd.DataFrame:
    """把个股(新浪)和指数(东财)的字段彻底统一"""
    df['code'] = code
    
    if 'opendate' in df.columns:
        df = df.rename(columns={'opendate': 'date'})

    # (重要) 确保 date 列是 datetime 类型以便后续操作
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # 动态构建需要转换和保留的列
    required_cols = [
        'date', 'code', 'close', 'pct_change', 'turnover_rate',
        'net_flow_amount', 'main_net_flow', 'super_large_net_flow',
        'large_net_flow', 'medium_small_net_flow'
    ]
    
    final_df = pd.DataFrame()
    for col in required_cols:
        if col in df.columns:
            final_df[col] = df[col]
        else:
            final_df[col] = pd.NA
    
    numeric_cols = [c for c in final_df.columns if c not in ['date', 'code']]
    final_df[numeric_cols] = final_df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    
    return final_df

# ==================== 高级数据质量检查函数 ====================
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

            code = df['code'].iloc[0]
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df.dropna(subset=['date'], inplace=True)
            
            if df.empty: continue
            
            start_date, end_date = df['date'].min(), df['date'].max()
            record_count = len(df)
            total_records += record_count
            all_dates.extend([start_date, end_date])
            
            expected_dates = pd.date_range(start=start_date, end=end_date, freq='B')
            missing_days = len(expected_dates.difference(df['date']))
            
            flow_cols = ['net_flow_amount', 'main_net_flow'] # 只检查核心指标
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
    print(f"→ 异常记录数（核心指标为0或空）: {final_report['total_error_records_found']:,}")
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

    # --- 阶段 1: 复制小文件 (带安检门) ---
    if os.path.exists(SMALL_OUTPUT_DIR): shutil.rmtree(SMALL_OUTPUT_DIR)
    os.makedirs(SMALL_OUTPUT_DIR, exist_ok=True)
    
    files_copied = 0
    skipped_files = 0
    
    for f in tqdm(files, desc="复制资金流小文件"):
        # [安检 1] 严格检查文件名后缀，防止 core dump 或 git 文件混入
        if not f.lower().endswith(".parquet"):
            print(f"⚠️ [安检拦截] 跳过非 Parquet 文件: {f}")
            skipped_files += 1
            continue
            
        # [安检 2] 检查文件大小 (仅报警，不跳过)
        f_size = os.path.getsize(f)
        f_size_mb = f_size / (1024 * 1024)
        
        if f_size_mb > LARGE_FILE_WARNING_THRESHOLD_MB:
            print(f"⚠️ [大文件提示] 文件 {os.path.basename(f)} 大小为 {f_size_mb:.2f} MB (超过 {LARGE_FILE_WARNING_THRESHOLD_MB}MB)，确认复制。")
        
        # 执行复制
        filename = os.path.basename(f)
        shutil.copy2(f, os.path.join(SMALL_OUTPUT_DIR, filename))
        files_copied += 1
        
    print(f"\n✅ 小文件收集完毕。成功: {files_copied}, 拦截非Parquet: {skipped_files}")
    
    # [新增] 调试打印：列出输出目录中最大的前5个文件
    print(f"🔍 [调试] 检查 {SMALL_OUTPUT_DIR} 目录中最大的文件:")
    try:
        output_files = glob.glob(os.path.join(SMALL_OUTPUT_DIR, "*"))
        # 按大小排序，取前5
        output_files.sort(key=os.path.getsize, reverse=True)
        for i, f in enumerate(output_files[:5]):
            size_mb = os.path.getsize(f) / (1024 * 1024)
            print(f"   {i+1}. {os.path.basename(f)} - {size_mb:.2f} MB")
        if not output_files:
            print("   (目录为空)")
    except Exception as e:
        print(f"   调试检查失败: {e}")

    # --- 阶段 2: 流式写入未排序的合并文件 ---
    chunk_size = 2000
    writer = None
    print(f"\n将以流式写入模式合并，每块 {chunk_size} 个文件...")
    try:
        # 重新获取刚刚复制过去的文件列表，确保来源纯净
        target_files = glob.glob(os.path.join(SMALL_OUTPUT_DIR, "*.parquet"))
        
        for i in tqdm(range(0, len(target_files), chunk_size), desc="分块写入 Parquet 中"):
            chunk_files = target_files[i : i + chunk_size]
            
            dfs = []
            for f in chunk_files:
                try:
                    df = pd.read_parquet(f)
                    filename = os.path.basename(f)
                    code = os.path.splitext(filename)[0]
                    clean_df = unify_columns(df, code)
                    dfs.append(clean_df)
                except Exception as e:
                    print(f"读取或清洗文件 {f} 失败: {e}")

            if not dfs: continue
            
            chunk_df = pd.concat(dfs, ignore_index=True)
            
            if 'date' in chunk_df.columns and pd.api.types.is_datetime64_any_dtype(chunk_df['date']):
                chunk_df['date'] = chunk_df['date'].dt.strftime('%Y-%m-%d')
                chunk_df['date'].replace({pd.NaT: None}, inplace=True)

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
    try:
        con = duckdb.connect()
        con.execute("SET memory_limit='5GB';") 
        query = f"""COPY (SELECT * FROM read_parquet('{TEMP_UNSORTED_FILE}') ORDER BY code, date) TO '{FINAL_PARQUET_FILE}' (FORMAT PARQUET, COMPRESSION 'ZSTD');"""
        con.execute(query)
        con.close()
        print(f"✅ DuckDB 排序完成！已生成最终文件: {FINAL_PARQUET_FILE}")
        os.remove(TEMP_UNSORTED_FILE)
    except Exception as e:
        print(f"\n❌ 在 DuckDB 排序阶段发生错误: {e}"); traceback.print_exc()
        if os.path.exists(TEMP_UNSORTED_FILE):
            os.rename(TEMP_UNSORTED_FILE, FINAL_PARQUET_FILE)

    # --- 阶段 4: 生成高级质检报告 ---
    run_advanced_quality_check()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌❌❌ 在 main 函数顶层捕获到致命异常: {e} ❌❌❌")
        traceback.print_exc()
        exit(1)
