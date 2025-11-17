# scripts/prepare_tasks.py
import baostock as bs
import json
import random
import os
from datetime import datetime, timedelta

TASK_COUNT = 20
OUTPUT_DIR = "task_slices"
TEST_STOCK_LIMIT = 1000          # 删除此行 + 下方切片即为全量

os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_recent_trade_day():
    for i in range(1, 7):
        day = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        rs = bs.query_trade_dates(start_date=day, end_date=day)
        if rs.error_code == '0' and rs.next() and rs.get_row_data()[1] == '1':
            print(f"最近交易日: {day}")
            return day
    raise Exception("未找到交易日")

def main():
    print("开始获取股票列表...")
    lg = bs.login()
    if lg.error_code != '0':
        raise Exception(f"登录失败: {lg.error_msg}")

    try:
        trade_day = get_recent_trade_day()
        rs = bs.query_all_stock(day=trade_day)
        stock_df = rs.get_data()

        stock_list = []
        for _, row in stock_df.iterrows():
            code, name = row['code'], row['code_name']
            if code.startswith(('sh.', 'sz.', 'bj.')) and 'ST' not in name and '退' not in name:
                stock_list.append({'code': code, 'name': name})

        print(f"获取到 {len(stock_list)} 只股票")
        stock_list = stock_list[:TEST_STOCK_LIMIT]   # 删除此行即全量
        print(f"测试模式：仅处理前 {len(stock_list)} 只")

        random.shuffle(stock_list)
        chunk_size = (len(stock_list) + TASK_COUNT - 1) // TASK_COUNT

        for i in range(TASK_COUNT):
            subset = stock_list[i * chunk_size: (i + 1) * chunk_size]
            path = os.path.join(OUTPUT_DIR, f"task_slice_{i}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(subset, f, ensure_ascii=False, indent=2)

        print(f"成功生成 {TASK_COUNT} 个任务分片")
    finally:
        bs.logout()

if __name__ == "__main__":
    main()
