import sqlite3
import pandas as pd
import os
import logging
from datetime import datetime
from typing import List
import argparse
from calendar import monthrange

def setup_logging():
    """設置日誌系統"""
    # 創建必要的資料夾
    os.makedirs('logs', exist_ok=True)
    os.makedirs(os.path.join('.', 'output'), exist_ok=True)

    # 檢查是否已經設置了日誌處理器
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/punch_record.log', encoding='utf-8', mode='a'),
                logging.StreamHandler()
            ]
        )
    return logging.getLogger(__name__)

# 設置資料庫路徑和輸出資料夾
db_path = os.path.join('.', 'db', 'source.db')
output_dir = os.path.join('.', 'output')

# 初始化日誌
logger = setup_logging()

def get_db_connection():
    """連接到資料庫"""
    try:
        return sqlite3.connect(db_path)
    except Exception as e:
        logger.error(f"數據庫連接錯誤: {str(e)}")
        return None

def get_time_columns():
    """獲取所有刷卡時間欄位"""
    try:
        conn = get_db_connection()
        if conn is None:
            return []
        
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(integrated_punch)")
        columns_info = cursor.fetchall()
        time_columns = [col[1] for col in columns_info if col[1].startswith('刷卡時間')]
        conn.close()
        return time_columns
    except Exception as e:
        logger.error(f"獲取時間列時發生錯誤: {str(e)}")
        return []

def format_timestamp(ts):
    """格式化時間戳記字串"""
    if pd.isna(ts):
        return None
    ts = str(ts).strip()
    if not ts:
        return None
    if ':' in ts:
        return ts
    if len(ts) == 6:
        return f"{ts[:2]}:{ts[2:4]}:{ts[4:6]}"
    return ts

def generate_html_table(df: pd.DataFrame, date_str: str, class_name: str) -> str:
    """
    產生打卡記錄的 HTML 表格
    Args:
        df (pd.DataFrame): 包含打卡記錄的 DataFrame
        date_str (str): 日期字串，用於標題
        class_name (str): 班別名稱，用於標題
    Returns:
        str: HTML 表格字串
    """
    # CSS 樣式
    css_style = """
    <style>
        .punch-record-table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }
        .punch-record-table th, .punch-record-table td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: center;
        }
        .punch-record-table th {
            background-color: #f2f2f2;
            font-weight: bold;
        }
        .punch-record-table tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        .punch-record-table tr:hover {
            background-color: #e0e0e0;
        }
        .punch-record-table td.timestamp-col {
            text-align: left;
        }
        .timestamp-odd {
            color: blue;
        }
        .timestamp-even {
            color: purple;
        }
        body {
           font-family: sans-serif;
        }
        h2 {
            text-align: center;
            color:#333
        }
    </style>
    """
    
    html_header = f"""
    <html>
    <head>
        <title>{class_name} 打卡記錄 - {date_str}</title>
        {css_style}
    </head>
    <body>
        <h2>{class_name} - {date_str} 打卡記錄</h2>
        <table class='punch-record-table'>
            <thead>
                <tr>
                    <th>卡號</th>
                    <th>公務帳號</th>
                    <th>姓名</th>
                    <th>打卡次數</th>
                    <th>時間戳記</th>
                </tr>
            </thead>
            <tbody>
    """
    
    html_table = ""
    for index, row in df.iterrows():
        html_row = "<tr>"
        html_row += f"<td>{row['卡號']}</td>"
        html_row += f"<td>{row['公務帳號']}</td>"
        html_row += f"<td>{row['姓名']}</td>"
        html_row += f"<td>{row['打卡次數']}</td>"
        
        # 將時間戳記文字依照奇數或偶數改變顏色
        timestamp_str = row['所有時間戳記']
        formatted_timestamps = []
        
        for i, ts in enumerate(timestamp_str.split(', ')):
            if not ts:
               continue
            if (i + 1) % 2 == 0:
                formatted_timestamps.append(f"<span class='timestamp-even'>{ts}</span>")
            else:
                formatted_timestamps.append(f"<span class='timestamp-odd'>{ts}</span>")
        
        html_row += f"<td class='timestamp-col'>{' '.join(formatted_timestamps)}</td>"
        html_row += "</tr>"
        html_table += html_row

    html_footer = """
                </tbody>
            </table>
        </body>
    </html>
    """
    return html_header + html_table + html_footer
    
def export_punch_record(date_str=None, gui_mode=False):
    """
    匯出打卡記錄到HTML檔案，並依班別分割
    :param date_str: 日期字串，格式為 'MM-DD'，如果為 None 則使用當前日期
    :param gui_mode: 是否在GUI模式下運行
    :return: 如果在GUI模式下，返回處理結果訊息
    """
    try:
        conn = get_db_connection()
        if conn is None:
            msg = "無法連接到數據庫"
            logger.error(msg)
            return msg if gui_mode else None

        # 如果沒有提供日期，使用當前日期
        if date_str is None:
            date_str = datetime.now().strftime('%m-%d')

        time_columns = get_time_columns()
        if not time_columns:
            msg = "無法獲取時間欄位"
            logger.error(msg)
            return msg if gui_mode else None
            
        # 查詢數據
        data_query = f"""
        SELECT 
            ip.班別,
            ip.卡號,
            ip.公務帳號,
            ip.姓名,
            ip.刷卡日期,
            {', '.join(time_columns)}
        FROM integrated_punch ip
        WHERE strftime('%m-%d', ip.刷卡日期) = ?
        ORDER BY ip.班別 ASC, ip.卡號 ASC
        """
        
        try:
            df = pd.read_sql_query(data_query, conn, params=(date_str,))
            
            # 格式化時間戳記
            for col in time_columns:
                df[col] = df[col].map(format_timestamp)
            
            # 合併所有時間戳記
            df['所有時間戳記'] = df[time_columns].apply(
                lambda row: ', '.join([t for t in row if pd.notna(t)]), axis=1
            )
            
            # 計算打卡次數
            df['打卡次數'] = df[time_columns].apply(
                lambda row: sum(1 for t in row if pd.notna(t)), axis=1
            )
            
            # 輸出到HTML
            output_file = os.path.join(output_dir, f'punch_record_{date_str}.html')
            html_content = ""
            for class_name, group in df.groupby('班別'):  # 按照班別分組，然後迭代
                result_df = group[['卡號', '公務帳號', '姓名', '打卡次數', '所有時間戳記']]  # 只保留需要的欄位
                html_content += generate_html_table(result_df, date_str, class_name)  # 為每個班別生成一個表格

            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(html_content)  # 將所有表格寫入同一個HTML檔案
            
            # 準備返回訊息
            record_count = len(df)
            msg = f"已將打卡記錄保存到: {output_file}\n共處理 {record_count} 筆打卡記錄"
            logger.info(msg)
            return msg if gui_mode else None
            
        except Exception as e:
            msg = f"查詢數據時發生錯誤: {str(e)}"
            logger.error(msg)
            return msg if gui_mode else None
        finally:
            conn.close()
            
    except Exception as e:
        msg = f"處理請求時發生錯誤: {str(e)}"
        logger.error(msg)
        return msg if gui_mode else None

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="處理打卡資料")
    parser.add_argument("--date", help="查詢日期 (格式: MM-DD, 預設為今天)")
    args = parser.parse_args()
    
    # 直接執行時的處理
    if args.date:
        export_punch_record(args.date, gui_mode=False)
    else:
        export_punch_record(gui_mode=False)