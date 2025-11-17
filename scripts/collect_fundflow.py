# scripts/collect_fundflow.py
# 功能：收集资金流分片 → 复制为单个股票小文件 → 合并为 ZSTD 大文件 → 专业资金流质检
import pandas as pd
import glob
import os
import shutil
import json
from tqdm import tqdm
from pathlib import Path

# ====================== 配置 ======================
INPUT_BASE_DIR = "all_fundflow"                    # download-artifact 后所有 fundflow_part_* 在这里
OUTPUT_DIR_SMALL_FILES = "fundflow_small"          # 单个股票文件目录（上传为 fundflow-small-files）
FINAL_PARQUET_FILE = "full_fundflow.parquet"      # 最终合并大文件
QC_REPORT_FILE = "data_quality_report_fundflow.json"

# ====================== 资金流专用质检函数 ======================
def run_fundflow_quality_check(df: pd.DataFrame):
    print("\n" + "="*60)
    print("开始执行 资金流数据质量检查 (FundFlow Data Quality Check)...")
    try:
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')

        report = {
            "data_type": "fundflow",
            "total_records": int(len(df)),
            "total_stocks": int(df['code'].nunique()),
            "date_range": [
                df['date'].min().strftime('%Y-%m-%d') if pd.notna(df['date'].min()) else None,
                df['date'].max().strftime('%Y-%m-%d') if pd.notna(df['date'].max()) else None
            ],
        }

        # 1. 主力资金平衡校验（核心！）
        flow_cols = ['main_net_flow', 'super_large_net_flow', 'large_net_flow', 'medium_small_net_flow']
        if all(col in df.columns for col in flow_cols + ['net_flow_amount']):
            df_flow = df[flow_cols + ['net_flow_amount']].copy()
            df_flow = df_flow.fillna(0)
            df_flow['calculated_total'] = df_flow[flow_cols].sum(axis=1)
            df_flow['diff'] = df_flow['net_flow_amount'] - df_flow['calculated_total']
            tolerance = 1e-6
            imbalance = df_flow[abs(df_flow['diff']) > tolerance]
            report["fund_balance_check"] = {
                "imbalance_records": int(len(imbalance)),
                "max_absolute_diff": float(abs(imbalance['diff']).max()) if not imbalance.empty else 0.0,
                "imbalance_ratio": round(len(imbalance) / len(df_flow), 6)
            }
        else:
            report["fund_balance_check"] = {"error": "缺少必要资金流字段"}

        # 2. 异常值检测
        report["abnormal_detection"] = {
            "zero_main_flow_days": int(df['main_net_flow'].abs() < 1).sum(),
            "extreme_main_flow_days": int(df['main_net_flow'].abs() > 1e10).sum(),  # >100亿
            "negative_turnover_rate": int(df['turnover_rate'].lt(0).sum()),
            "pct_change_over_9pct": int(df['pct_change'].abs() > 0.099).sum(),  # 接近涨跌停
        }

        # 3. 缺失值统计
        nan_summary = df.isnull().sum()
        report["missing_values"] = nan_summary[nan_summary > 0].to_dict()

        # 4. 数据分布统计
        records_per_stock = df.groupby('code').size()
        report["distribution"] = {
            "avg_records_per_stock": round(records_per_stock.mean(), 2),
            "median_records_per_stock": int(records_per_stock.median()),
            "stocks_with_over_10_years": int((records_per_stock > 250*10).sum()),
            "stocks_with_over_5_years":  int((records_per_stock > 250*5).sum()),
            "stocks_with_less_than_1_year": int((records_per_stock < 250).sum()),
        }

        # 5. 完整性抽样（最长股票）
        if len(records_per_stock) > 0:
            longest_stock = records_per_stock.idxmax()
            df_long = df[df['code'] == longest_stock].copy()
            if len(df_long) > 1:
                df_long = df_long.set_index('date').sort_index()
                expected = pd.date_range(start=df_long.index.min(), end=df_long.index.max(), freq='B')
                missing = expected.difference(df_long.index)
                report["completeness_sample"] = {
                    "sample_stock": longest_stock,
                    "period_years": round((df_long.index.max() - df_long.index.min()).days / 365.25, 2),
                    "missing_business_days": len(missing),
                }

        # 保存报告
        with open(QC_REPORT_FILE, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)

        print(f"资金流质检报告已生成：{QC_REPORT_FILE}")
        print(f"→ 股票数：{report['total_stocks']:,}  |  总记录：{report['total_records']:,}")
        print(f"→ 数据区间：{report['date_range'][0]} 至 {report['date_range'][1]}")
        balance = report.get("fund_balance_check", {})
        if "imbalance_records" in balance:
            print(f"→ 主力资金平衡异常记录：{balance['imbalance_records']:,}（占比 {balance['imbalance_ratio']:.2%}）")
        print(f"→ 超过10年历史的股票：{report['distribution']['stocks_with_over_10_years']:,}")
        print("="*60)

    except Exception as e:
        print(f"资金流质检过程出错：{e}")
        import traceback
        traceback.print_exc()

# ====================== 主函数 ======================
def main():
    print("\n开始 资金流数据收集与合并流程...")

    # 1. 创建干净的小文件输出目录
    if os.path.exists(OUTPUT_DIR_SMALL_FILES):
        shutil.rmtree(OUTPUT_DIR_SMALL_FILES)
    os.makedirs(OUTPUT_DIR_SMALL_FILES, exist_ok=True)

    # 2. 查找所有分片中的 parquet 文件
    pattern = os.path.join(INPUT_BASE_DIR, "**", "*.parquet")
    file_list = glob.glob(pattern, recursive=True)

    if not file_list:
        print("致命错误：未在 all_fundflow/ 中找到任何 .parquet 文件！")
        exit(1)

    print(f"发现 {len(file_list):,} 个资金流分片文件，开始收集...")

    # 3. 复制为单个股票小文件（用于 fundflow-small-files artifact）
    for src in tqdm(file_list, desc="复制资金流小文件"):
        filename = os.path.basename(src)
        dst = os.path.join(OUTPUT_DIR_SMALL_FILES, filename)
        shutil.copy2(src, dst)

    print(f"所有小文件已收集至 {OUTPUT_DIR_SMALL_FILES}/")

    # 4. 读取并合并所有数据
    print("正在读取并合并所有资金流数据...")
    dfs = []
    for f in tqdm(file_list, desc="读取分片"):
        try:
            df = pd.read_parquet(f)
            dfs.append(df)
        except Exception as e:
            print(f"读取失败 {f}：{e}")

    if not dfs:
        print("致命错误：所有文件读取失败，无法合并！")
        exit(1)

    merged = pd.concat(dfs, ignore_index=True)
    print(f"合并完成，总行数：{len(merged):,}")

    # 5. 数据类型统一
    if 'date' in merged.columns:
        merged['date'] = pd.to_datetime(merged['date'], errors='coerce')
    numeric_cols = ['close', 'pct_change', 'turnover_rate', 'net_flow_amount',
                    'main_net_flow', 'super_large_net_flow', 'large_net_flow', 'medium_small_net_flow']
    for col in numeric_cols:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors='coerce')

    # 6. 按 code + date 排序（最佳压缩）
    print("按 code + date 排序优化压缩...")
    merged = merged.sort_values(['code', 'date']).reset_index(drop=True)

    # 7. 保存最终大文件（ZSTD 高压缩）
    print(f"正在写入最终合并文件：{FINAL_PARQUET_FILE}（ZSTD压缩）")
    try:
        merged.to_parquet(
            FINAL_PARQUET_FILE,
            index=False,
            compression='zstd',
            row_group_size=100_000,
            engine='pyarrow'
        )
        print("最终资金流大文件写入成功！")
    except Exception as e:
        print(f"ZSTD失败，回退 snappy：{e}")
        merged.to_parquet(FINAL_PARQUET_FILE, index=False, compression='snappy')

    # 8. 执行专业资金流质检
    run_fundflow_quality_check(merged)

    print("\n资金流数据收集、合并、专业质检全部完成！")
    print(f"→ 小文件目录：{OUTPUT_DIR_SMALL_FILES}/")
    print(f"→ 合并大文件：{FINAL_PARQUET_FILE}")
    print(f"→ 质检报告：{QC_REPORT_FILE}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n严重错误：{e}")
        import traceback
        traceback.print_exc()
        exit(1)
