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


def process_all_classes_data(conn: sqlite3.Connection, rules_dict: Dict, time_columns: List[str]) -> pd.DataFrame:
    """處理所有班別數據並返回合併的DataFrame"""
    all_data = []
    
    for class_name, rules in rules_dict.items():
        logging.info(f"正在處理班別: {class_name}")
        class_data = process_night_meal_data(conn, class_name, rules, time_columns)
        # 添加班別名稱到每筆資料
        class_df = pd.DataFrame(class_data, columns=['卡號', '公務帳號', '姓名', '月份', '日期'])
        class_df['班別'] = class_name
        all_data.append(class_df)
    
    return pd.concat(all_data, ignore_index=True)

def output_combined_html(output_dir: str, combined_df: pd.DataFrame, account_list: set):
    """輸出合併後的互動式HTML報表"""
    # 數據處理
    combined_df['月份'] = combined_df['月份'].astype(str) + '月'
    
    # 建立記憶體資料庫處理分組
    conn = sqlite3.connect(':memory:')
    combined_df.to_sql('combined_data', conn, index=False)
    
    # 優化後的SQL查詢
    query = """
    SELECT 
        班別,
        卡號,
        公務帳號,
        姓名,
        月份,
        COUNT(DISTINCT 日期) AS 夜點天數,
        GROUP_CONCAT(日期, ', ') AS 日期清單
    FROM combined_data
    GROUP BY 班別, 卡號, 公務帳號, 姓名, 月份
    ORDER BY 班別, 卡號, 月份
    """
    summary_df = pd.read_sql(query, conn)
    conn.close()

    # 標註清單中的人名為紅色
    summary_df['姓名'] = summary_df.apply(
        lambda row: f'<span style="color: red;">{row["姓名"]}</span>' 
        if row['公務帳號'] in account_list 
        else row['姓名'], 
        axis=1
    )

    # 按班別分組
    grouped_data = summary_df.groupby('班別')

    # 生成每個班別的表格
    tables_html = ""
    for class_name, group_df in grouped_data:
        tables_html += f"""
        <div class="class-section mb-5">
            <h3 class="class-title p-2 bg-primary text-white rounded">{class_name}</h3>
            {group_df.to_html(
                index=False,
                classes='table table-hover class-table',
                escape=False,
                border=0,
                columns=['卡號', '公務帳號', '姓名', '月份', '夜點天數', '日期清單']
            )}
        </div>
        """

    # HTML模板
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>全班別夜點津貼彙總表</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <link href="https://cdn.datatables.net/1.13.6/css/dataTables.bootstrap5.min.css" rel="stylesheet">
        <style>
            .class-section {{
                margin-bottom: 2rem;
            }}
            .class-title {{
                font-size: 1.25rem;
                margin-bottom: 1rem;
            }}
            .class-table {{
                font-size: 0.9em;
                table-layout: fixed;
                width: 100% !important;
            }}
            .class-table thead th {{
                background-color: #2c3e50 !important;
                color: white !important;
            }}
            .class-table tbody tr:nth-child(odd) {{
                background-color: #f8f9fa;
            }}
            .class-table tbody tr:hover {{
                background-color: #e9ecef;
            }}
            .class-table td {{
                white-space: normal !important;  /* 允許換行 */
                word-wrap: break-word;           /* 長內容自動換行 */
            }}
            .date-list {{
                white-space: nowrap;
                overflow-x: auto;
            }}
        </style>
    </head>
    <body>
        <div class="container-fluid p-4">
            <h2 class="mb-4">夜點津貼彙總表</h2>
            
            {tables_html}

            <div class="mt-4 text-muted">
                <small>生成時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</small>
            </div>
        </div>

        <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
        <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
        <script src="https://cdn.datatables.net/1.13.6/js/dataTables.bootstrap5.min.js"></script>
        <script>
            $(document).ready(function() {{
                $('.class-table').DataTable({{
                    language: {{
                        url: '//cdn.datatables.net/plug-ins/1.13.6/i18n/zh-HANT.json'
                    }},
                    columnDefs: [
                        {{ targets: 0, width: '20px' }},   // 卡號
                        {{ targets: 1, width: '20px' }},  // 公務帳號
                        {{ targets: 2, width: '20px' }},  // 姓名
                        {{ targets: 3, width: '10px' }},   // 月份
                        {{ targets: 4, width: '10px' }},   // 夜點天數
                        {{ targets: 5, width: '300px' }}   // 日期清單
                    ],
                    paging: false,
                    searching: true,
                    ordering: true,
                    info: false,
                    scrollX: true
                }});
            }});
        </script>
    </body>
    </html>
    """

    output_path = os.path.join(output_dir, 'combined_night_meal_report.html')
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)


def main(db_path: str, output_dir: str, list_path: str):
    """主函數
    Args:
        db_path (str): 資料庫檔案路徑
        output_dir (str): 輸出資料夾路徑
        list_path (str): 清單資料檔案路徑
    """
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # 讀取比對清單
        try:
            list_df = pd.read_csv(list_path)
            account_list = set(list_df['公務帳號'])  # 將清單資料的公務帳號讀取出來轉為 set
            logging.info(f"已讀取清單資料，共 {len(account_list)} 筆資料")
        except Exception as e:
            logging.error(f"讀取清單資料錯誤: {e}")
            account_list = set()  # 如果清單資料讀取失敗則使用空 set

        # 處理資料庫資料
        conn = connect_to_db(db_path)
        cursor = conn.cursor()
        
        rules_dict = create_rules_dict(conn)
        time_columns = get_time_columns(cursor)

        # 處理所有班別數據
        combined_df = process_all_classes_data(conn, rules_dict, time_columns)
        
        # 輸出合併報表
        output_combined_html(output_dir, combined_df, account_list)

    except Exception as e:
        logging.error(f"處理錯誤: {e}")
    finally:
        if conn:
            conn.close()

    logging.info("報表生成完成")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="處理夜點資料")
    parser.add_argument("--db_path", default=".\\db\\source.db", help="資料庫文件路徑")
    parser.add_argument("--output_dir", default=".\\output", help="輸出資料夾路徑")
    parser.add_argument("--list_path", default=".\\data\\司機名單.csv", help="比對清單檔案路徑")  # 添加清單檔案路徑參數
    args = parser.parse_args()

    main(args.db_path, args.output_dir, args.list_path)