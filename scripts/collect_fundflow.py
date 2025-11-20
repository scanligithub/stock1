# scripts/collect_fundflow.py
# 2025-11-19 终极版：流式写入 + DuckDB排序 + 高级数据质量检查

import os
import pandas as pd
import glob
from tqdm import tqdm
import json
from datetime import datetime
import shutil
import sys
import traceback

# ... (所有配置、函数和 main 函数的前三个阶段，与您提供的版本完全相同) ...
# ... 我们只修改最后一个阶段 ...

def main():
    # ... 
    # ... (阶段 1, 2, 3 的代码保持不变)
    # ...
    
    # --- 阶段 4: 生成高级质检报告 (全新重构) ---
    print("\n" + "="*50)
    print("🔍 [QC] 开始进行高级数据质量检查...")
    
    # 我们不再读取巨大的合并文件，而是直接分析 'fundflow_small/' 下的小文件
    # 这样更内存安全，且能进行更细致的个股分析
    small_files_path = os.path.join(SMALL_OUTPUT_DIR, "*.parquet")
    small_files = glob.glob(small_files_path)

    if not small_files:
        print("⚠️ [QC] 未在 fundflow_small/ 目录中找到任何文件，无法生成质检报告。")
        return

    print(f"  -> [QC] 将对 {len(small_files)} 个独立的股票文件进行分析...")
    
    # 存储每只股票的统计信息
    stock_reports = []
    # 全局统计
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
            df.dropna(subset=['date'], inplace=True) # 删除日期无效的行
            
            start_date = df['date'].min()
            end_date = df['date'].max()
            record_count = len(df)
            total_records += record_count
            all_dates.extend([start_date, end_date])
            
            # 计算缺失天数
            expected_dates = pd.date_range(start=start_date, end=end_date, freq='B')
            missing_days = len(expected_dates.difference(df['date']))
            
            # 统计数据错误（例如，所有资金流指标都为空或0）
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

    # 生成最终的汇总报告
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

    # 打印简报
    print("\n--- 资金流数据质量简报 ---")
    print(f"→ 标的总数（分析成功）: {final_report['total_stocks_processed']:,}")
    print(f"→ 总记录数：{final_report['total_records_analyzed']:,}")
    print(f"→ 异常记录数（全为0或空）: {final_report['total_error_records_found']:,}")
    date_range = final_report.get('global_date_range', {})
    print(f"→ 全局日期范围：{date_range.get('min')} ~ {date_range.get('max')}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌❌❌ 在 main 函数顶层捕获到致命异常: {e} ❌❌❌")
        traceback.print_exc()
        exit(1)
