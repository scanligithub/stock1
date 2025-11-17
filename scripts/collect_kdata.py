# scripts/collect_kdata.py
import pandas as pd
import glob
import os
import shutil
import json
from tqdm import tqdm

INPUT_DIR = "all_kline"
SMALL_DIR = "kdata"
FINAL_FILE = "full_kdata.parquet"
QC_FILE = "data_quality_report_kline.json"

os.makedirs(SMALL_DIR, exist_ok=True)

# （此处省略您原来的完整质检逻辑，为节省篇幅，仅保留框架）
# 请把您原来的 collect_and_compress.py 内容复制进来，只改下面几行路径名即可
# INPUT_BASE_DIR → INPUT_DIR
# OUTPUT_DIR_SMALL_FILES → SMALL_DIR
# FINAL_PARQUET_FILE → FINAL_FILE
# QC_REPORT_FILE → QC_FILE

# ...（完整质检代码同您原版）
