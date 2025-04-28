import pandas as pd
import sqlite3
import logging
from pathlib import Path
from typing import List, Dict
from contextlib import contextmanager
import argparse

# 設置日誌格式和級別
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 配置參數，包括資料庫路徑和 Excel 文件路徑
CONFIG = {
    'db_path': './db/source.db',  # 資料庫文件路徑
    'file_path_1': './data/刷卡資料.xlsx',  # 第一個 Excel 文件路徑（刷卡資料）
    'file_path_2': './data/list.xlsx',  # 第二個 Excel 文件路徑（班別資料）
}

# 使用 contextmanager 創建一個資料庫連接的上下文管理器
@contextmanager
def get_db_connection(db_path: str):
    """
    獲取 SQLite 資料庫連接的上下文管理器。
    :param db_path: 資料庫文件路徑
    :yield: 資料庫連接對象
    """
    conn = sqlite3.connect(db_path)  # 建立資料庫連接
    try:
        yield conn  # 返回連接對象供使用
    finally:
        conn.close()  # 確保連接在結束後關閉

def clean_and_store_excel(excel_data: pd.ExcelFile, file_path: str, conn: sqlite3.Connection) -> int:
    """
    清理並存儲 Excel 資料到 SQLite 資料庫。
    :param excel_data: Excel 文件對象
    :param file_path: Excel 文件路徑
    :param conn: 資料庫連接對象
    :return: 總處理的行數
    """
    total_processed_rows = 0  # 初始化總處理行數

    # 遍歷 Excel 文件中的每個工作表
    for sheet_name in excel_data.sheet_names:
        # 檢查文件是否存在
        if not Path(file_path).exists():
            logging.warning(f"文件不存在，跳過處理: {file_path}")
            continue

        # 讀取 Excel 文件中的工作表，跳過前 4 行
        df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=4)
        df.columns = df.iloc[0]  # 將第一行作為欄位名稱
        df = df.drop(0).loc[:, df.columns.notna()].reset_index(drop=True)  # 刪除空欄位並重置索引

        # 清理「序號」欄位，確保其為數值型資料
        if '序號' in df.columns:
            df = df[pd.to_numeric(df['序號'], errors='coerce').notnull()]

        # 刪除全為空值的欄位
        df = df.dropna(axis=1, how='all')

        # 將「刷卡日期」和「刷卡時間」欄位轉換為字串類型
        for col in ['刷卡日期', '刷卡時間']:
            if col in df.columns:
                df[col] = df[col].astype(str)

        # 將清理後的資料存儲到 SQLite 資料庫的 `punch` 表中
        df.to_sql('punch', conn, if_exists='replace', index=False)
        total_processed_rows += len(df)  # 累計處理行數
        logging.info(f"處理工作表 '{sheet_name}'，轉換筆數: {len(df)}，累計總筆數: {total_processed_rows}")

    return total_processed_rows

def convert_date_time_format(conn: sqlite3.Connection):
    """
    轉換日期和時間格式。
    :param conn: 資料庫連接對象
    """
    cursor = conn.cursor()

    # 檢查 `punch` 表是否存在
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='punch'")
    if cursor.fetchone():
        # 將「刷卡日期」從民國年格式轉換為西元年格式
        cursor.execute(""" 
            UPDATE punch 
            SET 刷卡日期 = CASE 
                WHEN LENGTH(刷卡日期) = 7 THEN 
                    (CAST(SUBSTR(刷卡日期, 1, 3) AS INTEGER) + 1911) || '-' || 
                    SUBSTR(刷卡日期, 4, 2) || '-' || 
                    SUBSTR(刷卡日期, 6, 2)
                ELSE 刷卡日期 
            END
        """)

        # 將「刷卡時間」從 4 位數格式轉換為標準時間格式
        cursor.execute("""
            UPDATE punch 
            SET 刷卡時間 = 
                CASE 
                    WHEN LENGTH(刷卡時間) = 4 THEN 
                        SUBSTR(刷卡時間, 1, 2) || ':' || 
                        SUBSTR(刷卡時間, 3, 2) || ':00'
                    ELSE 刷卡時間 
                END
        """)
        conn.commit()  # 提交事務
        logging.info("日期和時間格式修正已完成")
    else:
        logging.warning("表 'punch' 不存在，無法進行日期和時間格式修正")

def integrate_data(conn: sqlite3.Connection):
    """
    整合資料並創建新表。
    :param conn: 資料庫連接對象
    :return: 整合後的資料
    """
    # 查詢語句，將 `punch` 表和 `shift_class` 表進行左連接
    query_integrate = """
        SELECT 
            punch.公務帳號,
            shift_class.卡號,
            shift_class.姓名,
            shift_class.班別,
            punch.刷卡日期,
            GROUP_CONCAT(punch.刷卡時間) AS 刷卡時間
        FROM
            punch
        LEFT JOIN
            shift_class ON punch.公務帳號 = shift_class.公務帳號
        GROUP BY
            punch.公務帳號, punch.刷卡日期, shift_class.班別;
    """

    # 執行查詢並將結果存儲到 DataFrame 中
    integrated_data = pd.read_sql(query_integrate, conn)

    # 將「刷卡時間」欄位中的多個時間值拆分為多個欄位
    split_times = integrated_data['刷卡時間'].str.split(',', expand=True)
    max_splits = split_times.shape[1]  # 獲取拆分後的最大列數

    # 創建新的欄位名稱（如 `刷卡時間1`, `刷卡時間2` 等）
    time_columns = [f'刷卡時間{i+1}' for i in range(max_splits)]
    new_columns = pd.DataFrame(split_times.values, columns=time_columns)
    new_columns = new_columns.dropna(axis=1, how='all')  # 刪除全為空值的欄位

    # 將拆分後的欄位與原始資料合併
    integrated_data = pd.concat([integrated_data.drop(columns=['刷卡時間']), new_columns], axis=1)

    # 檢查並確保「卡號」和「姓名」欄位存在
    if '卡號' not in integrated_data.columns:
        integrated_data['卡號'] = None
    if '姓名' not in integrated_data.columns:
        integrated_data['姓名'] = None

    # 創建 `integrated_punch` 表
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS integrated_punch (
        公務帳號 TEXT,
        卡號 TEXT,
        姓名 TEXT,
        刷卡日期 TEXT,
        班別 TEXT,
        {', '.join([f'{col} TEXT' for col in new_columns.columns])}
    );
    """
    conn.execute(create_table_query)

    # 將整合後的資料存儲到 `integrated_punch` 表中
    integrated_data.to_sql('integrated_punch', conn, if_exists='replace', index=False)
    logging.info(f"整合後的打卡資料已存儲到 integrated_punch 表，共 {len(integrated_data)} 筆資料")
    return integrated_data

def main(config: Dict[str, str]):
    """
    主函數，負責協調整個程式的執行流程。
    :param config: 配置參數
    """
    # 檢查資料庫檔案的路徑
    db_path = config['db_path']
    db_dir = Path(db_path).parent  # 獲取資料庫文件所在的目錄

    # 確保資料夾存在，若不存在則創建
    if not db_dir.exists():
        db_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"已創建資料夾: {db_dir}")

    # 檢查資料庫檔案是否存在，若存在則刪除
    if Path(db_path).exists():
        Path(db_path).unlink()  # 刪除現有的資料庫檔案
        logging.info(f"已刪除現有的資料庫檔案: {db_path}")

    # 檢查第一個 Excel 文件是否存在
    if not Path(config['file_path_1']).exists():
        logging.error(f"檔案不存在: {config['file_path_1']}")
        return  # 終止程式

    # 使用資料庫連接上下文管理器
    with get_db_connection(db_path) as conn:
        # 處理第一個 Excel 文件（刷卡資料）
        excel_data_1 = pd.ExcelFile(config['file_path_1'])
        total_processed_rows = clean_and_store_excel(excel_data_1, config['file_path_1'], conn)

        # 處理第二個 Excel 文件（班別資料）
        excel_data_2 = pd.ExcelFile(config['file_path_2'])
        for sheet_name in excel_data_2.sheet_names:
            if not Path(config['file_path_2']).exists():
                logging.warning(f"班別資料文件不存在，跳過處理: {config['file_path_2']}")
                continue
            df = pd.read_excel(config['file_path_2'], sheet_name=sheet_name)
            df.to_sql('shift_class', conn, if_exists='append', index=False)  # 直接存儲到 `shift_class` 表
            total_processed_rows += len(df)
            logging.info(f"處理工作表 '{sheet_name}'，直接存儲筆數: {len(df)}，累計總筆數: {total_processed_rows}")

        # 轉換日期和時間格式
        convert_date_time_format(conn)

        # 整合資料
        integrated_data = integrate_data(conn)

        # 顯示整合後的資料頭部
        logging.info("整合後的打卡資料頭部：")
        logging.info(integrated_data.head())

    logging.info(f"清理和整合後的資料已保存到 {config['db_path']}")

if __name__ == "__main__":
    # 解析命令行參數
    parser = argparse.ArgumentParser(description="處理和整合打卡資料")
    parser.add_argument("--db_path", default=CONFIG['db_path'], help="資料庫文件路徑")
    parser.add_argument("--file_path_1", default=CONFIG['file_path_1'], help="刷卡資料 Excel 文件路徑")
    parser.add_argument("--file_path_2", default=CONFIG['file_path_2'], help="班別資料 Excel 文件路徑")
    args = parser.parse_args()

    # 更新配置參數
    CONFIG.update(vars(args))

    # 執行主函數
    main(CONFIG)

    print("程式執行完畢，正在自動退出...")