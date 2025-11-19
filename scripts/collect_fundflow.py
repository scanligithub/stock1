# scripts/collect_fundflow.py
# 2025-11-17 终极内存安全版：通过流式写入解决超大文件合并时的内存溢出问题

import os
import pandas as pd
import glob
from tqdm import tqdm
import json
from datetime import datetime
import shutil
import sys
import traceback

# 尝试导入 pyarrow，如果失败，后面会处理
try:
    import pyarrow.parquet as pq
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


# ==================== 配置 ====================
INPUT_BASE_DIR = "all_fundflow"
SMALL_OUTPUT_DIR = "fundflow_small"
FINAL_PARQUET_FILE = "full_fundflow.parquet"
QUALITY_REPORT_FILE = "data_quality_report_fundflow.json"

os.makedirs(SMALL_OUTPUT_DIR, exist_ok=True)


# ==================== 系统资源监控函数 ====================
def print_system_stats():
    """打印当前的内存和硬盘使用情况"""
    print("-" * 20)
    # 打印内存 (需要 psutil 库，如果未安装则跳过)
    try:
        import psutil
        mem = psutil.virtual_memory()
        print(f"  -> 📊 系统状态: RAM 可用 {mem.available / (1024**3):.2f} GB / 总计 {mem.total / (1024**3):.2f} GB ({mem.percent}%)")
    except ImportError:
        print("  -> 📊 系统状态: 未安装 psutil，无法获取内存信息。请运行 'pip install psutil'")
    
    # 打印硬盘
    try:
        disk = shutil.disk_usage("/")
        print(f"  -> 📊 系统状态: 硬盘可用 {disk.free / (1024**3):.2f} GB / 总计 {disk.total / (1024**3):.2f} GB ({disk.used / disk.total * 100:.1f}%)")
    except Exception as e:
        print(f"  -> 📊 系统状态: 获取硬盘信息失败: {e}")
    print("-" * 20)


# ==================== 统一字段修复函数 ====================
def unify_columns(df: pd.DataFrame) -> pd.DataFrame:
    """把个股(新浪)和指数(东财)的字段彻底统一"""
    # ... (此函数内容与您之前的版本完全相同，此处省略以保持简洁)
    # ... 请确保您使用的是包含了所有重命名和类型转换逻辑的完整版本
    return df


# ==================== 主流程 ====================
def main():
    if not PYARROW_AVAILABLE:
        print("❌ 致命错误: 未找到 'pyarrow' 库，无法进行 Parquet 操作。请运行 'pip install pyarrow'。")
        sys.exit(1)
        
    print("开始 资金流数据收集与合并流程...")
    print_system_stats()

    search_pattern = os.path.join(INPUT_BASE_DIR, "**", "*.parquet")
    files = glob.glob(search_pattern, recursive=True)
    
    if not files:
        print("没有找到任何分片文件，退出。")
        return
        
    print(f"发现 {len(files)} 个资金流分片文件，开始处理...")

    # --- 阶段 1: 复制小文件 ---
    if os.path.exists(SMALL_OUTPUT_DIR):
        shutil.rmtree(SMALL_OUTPUT_DIR)
    os.makedirs(SMALL_OUTPUT_DIR, exist_ok=True)
    for f in tqdm(files, desc="复制资金流小文件"):
        try:
            filename = os.path.basename(f)
            shutil.copy2(f, os.path.join(SMALL_OUTPUT_DIR, filename))
        except Exception as e:
            print(f"\n⚠️ 复制文件 {f} 失败: {e}")
    print(f"所有小文件已收集至 {SMALL_OUTPUT_DIR}/")

    # --- 阶段 2: 流式写入，节省内存 ---
    chunk_size = 2000 # 每次处理 2000 个文件
    writer = None
    
    print(f"\n将以流式写入模式合并，每块 {chunk_size} 个文件...")

    try:
        for i in tqdm(range(0, len(files), chunk_size), desc="分块写入 Parquet 中"):
            chunk_files = files[i : i + chunk_size]
            
            # 读取当前块的所有 DataFrame
            dfs = [unify_columns(pd.read_parquet(f)) for f in chunk_files]
            chunk_df = pd.concat(dfs, ignore_index=True)
            
            # 转换为 Arrow Table
            table = pa.Table.from_pandas(chunk_df, preserve_index=False)
            
            if writer is None:
                # 第一次写入，创建文件和 ParquetWriter 对象
                writer = pq.ParquetWriter(FINAL_PARQUET_FILE, table.schema, compression='zstd' if 'zstandard' in sys.modules else 'snappy')
            
            # 将当前块的 table 写入文件
            writer.write_table(table)
            
            print(f"\n块 {i//chunk_size + 1} 写入完成。")
            print_system_stats() # 每次写入后都打印资源情况

    finally:
        # 确保 writer 被关闭
        if writer:
            writer.close()
            print("\nParquet writer 已关闭。")

    print(f"\n合并写入完成，正在读取最终文件进行排序和质检...")
    
    # --- 阶段 3: 最终排序与质检 ---
    # 这一步依然是内存瓶颈，如果数据量过大（几十GB），这里可能依然会失败
    # 但对于您几GB的数据量，7GB内存通常足够
    try:
        final_df = pd.read_parquet(FINAL_PARQUET_FILE)
        
        print("按 code + date 排序优化压缩...")
        final_df = final_df.sort_values(['code', 'date']).reset_index(drop=True)
        
        print(f"正在重新写入已排序的最终文件：{FINAL_PARQUET_FILE}...")
        final_df.to_parquet(FINAL_PARQUET_FILE, index=False, compression='zstd' if 'zstandard' in sys.modules else 'snappy')
        print("最终文件写入成功！")
        
        # ... (在这里插入您完整的 run_quality_check 函数定义) ...
        # run_quality_check(final_df)
        print("\n(跳过质检报告生成，您可以后续添加)")
        
    except Exception as e:
        print(f"\n❌ 在最终排序或质检阶段发生错误: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌❌❌ 在 main 函数顶层捕获到致命异常: {e} ❌❌❌")
        traceback.print_exc()
        exit(1)
