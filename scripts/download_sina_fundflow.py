# scripts/download_sina_fundflow.py
# 变态级调试版 —— 绝对让“成功 0/5”无处遁形
import os
import json
import requests
import pandas as pd
from tqdm import tqdm
import time

# ====================== 配置 ======================
OUTPUT_DIR = "data_fundflow"
PAGE_SIZE = 50
SINA_API = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page={page}&num={num}&sort=opendate&asc=0&daima={code}"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://vip.stock.finance.sina.com.cn/'
}

TASK_INDEX = int(os.getenv("TASK_INDEX", 0))
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_full_history_fund_flow(code_with_prefix):
    code_api = code_with_prefix.replace('.', '')
    print(f"\n开始下载 → {code_with_prefix}  (API代码: {code_api})")
    all_data = []
    page = 1

    while True:
        url = SINA_API.format(page=page, num=PAGE_SIZE, code=code_api)
        print(f"  → 第 {page} 页请求 → {url[:80]}...")

        try:
            r = requests.get(url, headers=HEADERS, timeout=45)
            print(f"  ← 响应状态码: {r.status_code}   长度: {len(r.text)} 字符")

            if r.status_code != 200:
                print(f"  ← HTTP错误！原始内容前500字符:\n{r.text[:500]}")
                break

            r.encoding = 'gbk'
            raw_text = r.text.strip()

            # 关键调试：直接打印原始返回
            if not raw_text or raw_text == "[]":
                print("  ← 返回空数据 []，结束分页")
                break
            if raw_text.startswith("<") or "html" in raw_text.lower():
                print(f"  ← 返回HTML！被反爬或封禁了，前200字符:\n{raw_text[:200]}")
                break

            data = r.json()
            print(f"  ← json()解析成功，得到 {len(data)} 条记录")

            if not data:
                print("  ← json解析后为空列表，结束")
                break

            all_data.extend(data)
            print(f"  ← 累计记录数: {len(all_data)}")

            if len(data) < PAGE_SIZE:
                print("  ← 已到最后一页")
                break

            page += 1
            time.sleep(0.3)

        except requests.exceptions.RequestException as e:
            print(f"  ← 请求异常: {e}")
            break
        except json.JSONDecodeError as e:
            print(f"  ← JSON解析失败！原始文本前500字符:\n{raw_text[:500]}")
            print(f"  ← 异常详情: {e}")
            break
        except Exception as e:
            print(f"  ← 未知异常: {type(e).__name__}: {e}")
            break

    print(f"{code_with_prefix} 总计原始记录: {len(all_data)} 条")
    return pd.DataFrame(all_data) if all_data else pd.DataFrame()

def main():
    print("\n" + "="*80)
    print(f"新浪资金流下载 - 分区 {TASK_INDEX + 1}  (变态调试版)")
    print(f"当前工作目录: {os.getcwd()}")
    print("根目录文件列表:")
    os.system("ls -la")
    print("tasks/ 目录内容:")
    os.system("ls -la tasks/ || echo 'tasks/ 不存在'")
    print("-"*80)

    task_file = f"tasks/task_slice_{TASK_INDEX}.json"
    if not os.path.exists(task_file):
        print(f"致命错误：找不到任务文件 {task_file}")
        exit(1)

    with open(task_file, "r", encoding="utf-8") as f:
        stocks = json.load(f)

    print(f"本分区共 {len(stocks)} 只股票，前5只示例:")
    for s in stocks[:5]:
        print(f"  - {s['code']}  {s.get('name','')}")

    success = 0
    for s in tqdm(stocks, desc=f"分区{TASK_INDEX+1}"):
        code = s["code"]
        df = get_full_history_fund_flow(code)

        if df.empty:
            print(f"{code} → 返回空DataFrame，跳过保存")
            continue

        required = {'opendate','trade','changeratio','turnover','netamount',
                    'r0_net','r1_net','r2_net','r3_net'}
        missing = required - set(df.columns)
        if missing:
            print(f"{code} → 字段缺失: {missing}")
            print(f"    实际列: {list(df.columns)}")
            continue

        try:
            rename_map = {
                'opendate':'date','trade':'close','changeratio':'pct_change',
                'turnover':'turnover_rate','netamount':'net_flow_amount',
                'r0_net':'main_net_flow','r1_net':'super_large_net_flow',
                'r2_net':'large_net_flow','r3_net':'medium_small_net_flow'
            }
            df2 = df[list(rename_map.keys())].rename(columns=rename_map)
            df2['date'] = pd.to_datetime(df2['date'])
            df2[df2.columns[1:]] = df2[df2.columns[1:]].apply(pd.to_numeric, errors='coerce')
            df2['code'] = code

            path = f"{OUTPUT_DIR}/{code}.parquet"
            df2.to_parquet(path, index=False, compression='zstd')
            print(f"{code} → 保存成功 {path}  ({len(df2)} 行)")
            success += 1
        except Exception as e:
            print(f"{code} → 保存阶段出错: {e}")

    print("\n" + "="*80)
    print(f"分区 {TASK_INDEX + 1} 最终结果：成功 {success}/{len(stocks)} 只")
    if success == 0 and len(stocks) > 0:
        print("全部失败！请根据上面日志判断是请求被拒、返回HTML、还是JSON解析失败")
        exit(1)
    print("本分区正常完成")
    print("="*80)

if __name__ == "__main__":
    main()
