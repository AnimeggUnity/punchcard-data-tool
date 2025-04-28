import sqlite3
import pandas as pd
import os
from datetime import datetime
import logging
from typing import Dict, List
import argparse

# 設置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connect_to_db(db_path: str) -> sqlite3.Connection:
    """連接到資料庫
    Args:
        db_path (str): 資料庫檔案路徑
    Returns:
        sqlite3.Connection: 資料庫連線物件
    Raises:
        sqlite3.Error: 資料庫連線失敗時會拋出例外
    """
    try:
        return sqlite3.connect(db_path)
    except sqlite3.Error as e:
        logging.error(f"資料庫連接錯誤: {e}")
        raise

def get_time_columns(cursor: sqlite3.Cursor) -> List[str]:
    """獲取 integrated_punch 表的所有時間列
    Args:
        cursor (sqlite3.Cursor): 資料庫游標物件
    Returns:
        List[str]: 包含所有時間欄位名稱的列表
    """
    cursor.execute("PRAGMA table_info(integrated_punch)")  # 查詢表格欄位資訊
    columns_info = cursor.fetchall()  # 取得所有欄位資訊
    return [col[1] for col in columns_info if col[1].startswith('刷卡時間')] # 篩選出刷卡時間欄位


def create_rules_dict(conn: sqlite3.Connection) -> Dict[str, Dict]:
    """從 integrated_punch 表格讀取班別資訊並轉為字典
    Args:
        conn (sqlite3.Connection): 資料庫連線物件
    Returns:
        Dict[str, Dict]: 班別規則字典，key 為班別名稱, value為班別規則
    """
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT 班別 FROM integrated_punch") # 查詢不重複的班別名稱
    rules = cursor.fetchall() # 取得所有班別名稱
    
    rules_dict = {} # 初始化班別規則字典
    for row in rules:
        class_name = row[0] # 取出班別名稱
        rules_dict[class_name] = { # 建立班別規則字典
            'class_name': class_name,
            'night_meal_threshold': parse_time('22:00:00')  # 設定預設夜點門檻時間為22:00:00
        }
    
    return rules_dict


def parse_time(time_str: str) -> datetime.time:
    """解析時間字符串為 datetime.time 對象
    Args:
        time_str (str): 時間字串,格式為 %H:%M:%S
    Returns:
        datetime.time: datetime.time 物件，如果輸入為None則回傳None
    """
    if pd.isna(time_str):
        return None
    return datetime.strptime(time_str, '%H:%M:%S').time() # 將時間字串轉為datetime.time物件

def process_night_meal_data(conn: sqlite3.Connection, class_name: str, rules: Dict, time_columns: List[str]) -> List[List]:
    """處理特定班別的夜點數據
    Args:
        conn (sqlite3.Connection): 資料庫連線物件
        class_name (str): 班別名稱
        rules (Dict): 班別規則字典，包含夜點門檻時間
        time_columns (List[str]): 所有時間欄位名稱的列表
    Returns:
        List[List]: 符合夜點資格的資料列表, 包含 `卡號`, `公務帳號`, `姓名`, `月份`, `日期`
    """
    # 獲取所有時間列的列表，並添加到查詢中
    time_columns_str = ', '.join(time_columns)
    # 使用 SQL 查詢從 integrated_punch 表格讀取需要的資料
    data_query = f"""
    SELECT 
        ip.公務帳號, 
        ip.姓名, 
        ip.卡號, 
        ip.班別, 
        ip.刷卡日期
        {',' if time_columns else ''} {time_columns_str}
    FROM 
        integrated_punch ip
    WHERE 
        ip.班別 = '{class_name}'
    ORDER BY 
        ip.卡號 ASC, 
        ip.班別 ASC, 
        ip.公務帳號 ASC, 
        ip.姓名 ASC, 
        ip.刷卡日期 ASC
    """
    data = pd.read_sql_query(data_query, conn) # 將查詢結果轉為 Pandas DataFrame
    
    night_meal_data = [] # 初始化夜點資料列表
    processed_dates = {}  # 初始化處理過的日期字典，key 為 公務帳號，value 為set

    for _, row in data.iterrows(): # 迭代每一筆資料
        account = row['公務帳號'] # 取得公務帳號
        card_no = row['卡號'] # 取得卡號
        date = row['刷卡日期'] # 取得刷卡日期
        day = date[8:10]  # 從刷卡日期字串中提取日
        month = date[5:7] # 從刷卡日期字串中提取月

        if account not in processed_dates: # 如果該公務帳號還沒有被紀錄
            processed_dates[account] = set() # 初始化一個空的集合，用來儲存處理過的日期

        night_meal_recorded = check_night_meal(row, time_columns, rules['night_meal_threshold']) # 使用 check_night_meal 函數判斷是否符合夜點資格
        if night_meal_recorded and date not in processed_dates[account]:  # 如果符合夜點資格，且該日期尚未被記錄
            night_meal_data.append([card_no, account, row['姓名'], month, day]) # 將資料新增到夜點資料列表
            processed_dates[account].add(date) # 將處理過的日期新增到處理過的日期集合中

    return night_meal_data

def check_night_meal(row: pd.Series, time_columns: List[str], night_meal_threshold: datetime.time) -> bool:
    """檢查是否有夜間餐費記錄
    Args:
        row (pd.Series): 打卡紀錄
        time_columns (List[str]): 所有時間欄位名稱的列表
        night_meal_threshold (datetime.time): 夜點門檻時間
    Returns:
        bool: True 如果符合夜點資格，否則 False
    """
    for col in time_columns[::-1]: # 反向迭代時間欄位，以取得最後一次打卡時間
        if not pd.isna(row[col]): # 如果該時間欄位有值
            last_time_str = row[col]  # 取出最後一次打卡時間字串
            if len(last_time_str) == 6:
                last_time_str = f"{last_time_str[:2]}:{last_time_str[2:4]}:{last_time_str[4:6]}"
            last_time = datetime.strptime(last_time_str, '%H:%M:%S').time() # 將最後一次打卡時間轉為 datetime.time 物件
            return last_time > night_meal_threshold # 如果最後一次打卡時間大於門檻時間則回傳True
    return False


def output_night_meal_results(output_dir: str, class_name: str, night_meal_data: List):
    """輸出夜點結果到CSV文件
    Args:
        output_dir (str): 輸出資料夾路徑
        class_name (str): 班別名稱
        night_meal_data (List): 符合夜點資格的資料列表, 包含 `卡號`, `公務帳號`, `姓名`, `月份`, `日期`
    """
    night_meal_df = pd.DataFrame(night_meal_data, columns=['卡號', '公務帳號', '姓名', '月份', '日期'])
    
    # 使用 SQL 查詢進行分組和彙總
    query = f"""
        SELECT
            卡號,
            公務帳號,
            姓名,
            月份,
            COUNT(DISTINCT 日期) AS 有記錄的總共天數,
            GROUP_CONCAT(日期, ', ') AS 日期列表
        FROM
            (SELECT * from night_meal_df ORDER BY 日期)
        GROUP BY
            卡號, 公務帳號, 姓名, 月份
        ORDER BY 卡號, 月份
    """
    
    conn = sqlite3.connect(':memory:') # 建立一個暫存的 SQLite 資料庫
    night_meal_df.to_sql('night_meal_df', conn, if_exists='replace', index=False)  # 將 night_meal_df 資料寫入暫存資料庫
    night_meal_summary = pd.read_sql_query(query, conn)  # 從暫存資料庫查詢分組和彙總的資料
    conn.close() # 關閉暫存的資料庫連線

    night_meal_summary.to_csv(os.path.join(output_dir, f'{class_name}_night_meal_records.csv'), index=False, encoding='utf-8-sig') # 將資料輸出為csv檔案

def main(db_path: str, output_dir: str):
    """主函數
    Args:
        db_path (str): 資料庫檔案路徑
        output_dir (str): 輸出資料夾路徑
    """
    os.makedirs(output_dir, exist_ok=True)  # 建立輸出資料夾，如果已存在則不產生錯誤
    
    try:
        conn = connect_to_db(db_path)  # 連接到資料庫
        cursor = conn.cursor()  # 建立資料庫游標
        
        rules_dict = create_rules_dict(conn) # 建立班別規則字典
        time_columns = get_time_columns(cursor)  # 取得所有時間欄位名稱

        for class_name, rules in rules_dict.items(): # 迭代每一個班別規則
            logging.info(f"處理班別名稱: {rules['class_name']}")
            night_meal_data = process_night_meal_data(conn, class_name, rules, time_columns) # 處理特定班別的夜點資料
            output_night_meal_results(output_dir, rules['class_name'], night_meal_data)  # 輸出夜點資料到 CSV 檔案

    except Exception as e:
        logging.error(f"處理過程中發生錯誤: {e}") # 記錄處理過程中的錯誤
    finally:
        if conn:
            conn.close() # 關閉資料庫連線

    logging.info("夜點處理程式執行完畢") # 記錄程式執行完畢訊息

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="處理夜點資料") # 建立命令列參數解析器
    parser.add_argument("--db_path", default=".\\db\\source.db", help="資料庫文件路徑") # 添加資料庫路徑參數
    parser.add_argument("--output_dir", default=".\\output", help="輸出資料夾路徑") # 添加輸出資料夾路徑參數
    args = parser.parse_args() # 解析命令列參數

    main(args.db_path, args.output_dir) # 呼叫主函式