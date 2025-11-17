# scripts/prepare_tasks.py
# 修改版：仅生成 100 只测试股票（第1000~1099名），用于快速验证资金流下载是否完全正常
import json
import random
import os

STOCK_LIST_FILE = "stock_list.json"
OUTPUT_DIR = "tasks"               # 保持原来路径，下载脚本直接认这个目录
os.makedirs(OUTPUT_DIR, exist_ok=True)

def main():
    print("准备测试任务：取全市场第1000~1099名，共100只股票（含指数）")

    with open(STOCK_LIST_FILE, "r", encoding="utf-8") as f:
        stocks = json.load(f)

    total = len(stocks)
    print(f"全市场总股票数：{total} 只")

    # 取第1000~1099名（索引 1000:1100）
    test_stocks = stocks[1000:1100]

    print(f"本次取测试股票：{len(test_stocks)} 只（第1001~1100名）")
    print("前3只：", [s["code"] for s in test_stocks[:3]])
    print("后3只：", [s["code"] for s in test_stocks[-3:]])

    # 只生成一个任务文件，文件名固定，下载脚本直接认 TASK_INDEX=0
    task_file = os.path.join(OUTPUT_DIR, "task_slice_0.json")
    with open(task_file, "w", encoding="utf-8") as f:
        json.dump(test_stocks, f, ensure_ascii=False, indent=2)

    print(f"\n测试任务已生成：{task_file}")
    print("接下来直接跑您原来的资金流下载 workflow 即可（会自动只跑这100只）")

if __name__ == "__main__":
    main()
