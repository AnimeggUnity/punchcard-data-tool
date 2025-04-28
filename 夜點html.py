import sqlite3
import pandas as pd
import os
from datetime import datetime, date
import logging
from typing import Dict, List
import argparse
from calendar import monthrange

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
    data = pd.read_sql_query(data_query, conn)
    
    night_meal_data = []
    processed_dates = {}  # 用於追蹤已處理的日期

    for _, row in data.iterrows():
        account = row['公務帳號']
        card_no = row['卡號']
        date_str = row['刷卡日期']  # 格式應為 'YYYY-MM-DD'
        
        # 從日期字串中提取月份和日期
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            month = f"{date_obj.month:02}"  # 確保月份為兩位數
            day = f"{date_obj.day:02}"      # 確保日期為兩位數
        except ValueError:
            logging.error(f"無效的日期格式: {date_str}")
            continue

        if account not in processed_dates:
            processed_dates[account] = set()

        night_meal_recorded = check_night_meal(row, time_columns, rules['night_meal_threshold'])
        if night_meal_recorded and date_str not in processed_dates[account]:
            night_meal_data.append([card_no, account, row['姓名'], month, day])
            processed_dates[account].add(date_str)

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

def generate_html_table(night_meal_summary: pd.DataFrame, class_name: str) -> str:
    """產生夜點結果的 HTML 表格
    Args:
        night_meal_summary (pd.DataFrame): 夜點彙總資料
    Returns:
        str: HTML 表格字串
    """
    # CSS 樣式
    css_style = """
    <style>
        .night-meal-table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }
        .night-meal-table th, .night-meal-table td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: center;
        }
        .night-meal-table th {
            background-color: #f2f2f2;
            font-weight: bold;
        }
        .night-meal-table tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        .night-meal-table tr:hover {
            background-color: #e0e0e0;
        }
        body {
           font-family: sans-serif;
        }
        h2 {
            text-align: center;
            color:#333
        }
        .date-box {
            display: inline-block;
            width: 20px;
            height: 20px;
            line-height: 20px;
            border: 1px solid #ccc;
            margin: 2px;
            text-align: center;
        }
        .date-box.filled {
            background-color: #4CAF50;
            color: white;
            font-weight: bold;
        }
        .total-days {
            font-weight: bold;
            color: #4CAF50;
        }
        .wed-sun-col {
            background-color: #FFF3E0;  /* 周三和周日的背景色 */
        }
        .sat-col {
            background-color: #F3E5F5;  /* 周六的背景色 */
        }
        .date-box.filled.wed-sun-col {
            background-color: #4CAF50;  /* 有資料時保持綠色 */
        }
        .date-box.filled.sat-col {
            background-color: #4CAF50;  /* 有資料時保持綠色 */
        }
    </style>
    """
    
    html_table = ""
    current_month = datetime.now().month  # 取得當前月份
    current_year = datetime.now().year # 取得當前年份
    _, num_days = monthrange(current_year, current_month)  # 取得當月天數
    
    # 建立表頭
    html_header = f"""
    <html>
    <head>
        <title>{class_name} 夜點紀錄</title>
        {css_style}
    </head>
    <body>
        <h2>{class_name} 夜點紀錄</h2>
        <table class='night-meal-table'>
            <thead>
                <tr>
    """
    
    # 生成日期表頭
    html_header += "<th>卡號</th><th>公務帳號</th><th>姓名</th><th>總天數</th><th>月份</th>" # 加入基本表頭
    for day in range(1, num_days + 1): # 生成每一天表頭
         day_str = f"{day:02}"
         # 計算星期幾
         weekday = datetime(current_year, current_month, day).weekday()
         # 根據星期幾添加不同的背景色
         if weekday == 2 or weekday == 6:  # 周三(2)或周日(6)
             html_header += f"<th class='wed-sun-col'>{day_str}</th>"
         elif weekday == 5:  # 周六(5)
             html_header += f"<th class='sat-col'>{day_str}</th>"
         else:
             html_header += f"<th>{day_str}</th>"
    html_header += "</tr></thead><tbody>"

    for index, row in night_meal_summary.iterrows():  # 轉換成 HTML 表格
        html_row = "<tr>"
        
        # 加入基本欄位
        html_row += f"<td>{row['卡號']}</td>"
        html_row += f"<td>{row['公務帳號']}</td>"
        html_row += f"<td>{row['姓名']}</td>"

        # 計算並顯示日期總數
        dates = []
        if not pd.isna(row['日期列表']):
            dates = [d.strip() for d in row['日期列表'].split(",")]
        total_days = len(dates)
        html_row += f"<td class='total-days'>{total_days}</td>"
        
        html_row += f"<td>{row['月份']}</td>"

        # 生成日期格子
        if dates:  # 如果日期列表不是空的
            for day in range(1, num_days + 1):  # 迭代當月份的每一天
                day_str = f"{day:02}"  # 將日期轉換成兩位數字串
                is_filled = any(day_str == d for d in dates)  # 檢查當前日期是否在列表中
                
                # 計算星期幾
                weekday = datetime(current_year, current_month, day).weekday()
                
                # 根據星期幾和是否有資料決定樣式
                if weekday == 2 or weekday == 6:  # 周三或周日
                    td_class = "wed-sun-col"
                    date_class = "date-box"
                    if is_filled:
                        date_class += " filled"
                elif weekday == 5:  # 周六
                    td_class = "sat-col"
                    date_class = "date-box"
                    if is_filled:
                        date_class += " filled"
                else:
                    td_class = ""
                    date_class = "date-box"
                    if is_filled:
                        date_class += " filled"
                
                html_row += f'<td class="{td_class}"><div class="{date_class}">{day_str if is_filled else ""}</div></td>'
        else:
            for day in range(1, num_days + 1):  # 如果沒有資料則補上空的格子
                # 計算星期幾
                weekday = datetime(current_year, current_month, day).weekday()
                if weekday == 2 or weekday == 6:  # 周三或周日
                    html_row += f'<td class="wed-sun-col"><div class="date-box"></div></td>'
                elif weekday == 5:  # 周六
                    html_row += f'<td class="sat-col"><div class="date-box"></div></td>'
                else:
                    html_row += f'<td><div class="date-box"></div></td>'

        html_row += "</tr>"
        html_table += html_row

    html_footer = """
            </tbody>
        </table>
    </body>
    </html>
    """
    return html_header + html_table + html_footer


def output_night_meal_results(output_dir: str, class_name: str, night_meal_data: List):
    """輸出夜點結果到 HTML 文件
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

    html_content = generate_html_table(night_meal_summary, class_name) # 產生 HTML 內容
    
    with open(os.path.join(output_dir, f'{class_name}_night_meal_records.html'), 'w', encoding='utf-8') as f:
        f.write(html_content) # 將 HTML 內容寫入檔案
    


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
            output_night_meal_results(output_dir, rules['class_name'], night_meal_data)  # 輸出夜點資料到 HTML 檔案

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