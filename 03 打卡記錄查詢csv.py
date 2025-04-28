import sqlite3
import pandas as pd
import os
import logging
from datetime import datetime

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

def export_punch_record(date_str=None, gui_mode=False):
    """
    匯出打卡記錄到CSV檔案
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
            def format_timestamp(ts):
                if pd.isna(ts):
                    return None
                ts = str(ts).strip()
                if not ts:
                    return None
                # 如果已經是 HH:MM:SS 格式就直接返回
                if ':' in ts:
                    return ts
                # 如果是 HHMMSS 格式則轉換
                if len(ts) == 6:
                    return f"{ts[:2]}:{ts[2:4]}:{ts[4:6]}"
                return ts

            # 處理所有時間欄位
            for col in time_columns:
                df[col] = df[col].map(format_timestamp)
            
            # 合併所有時間戳記
            df['所有時間戳記'] = df[time_columns].apply(
                lambda row: ', '.join([t for t in row if pd.notna(t)]), axis=1
            )
            
            # 只保留需要的欄位
            result_df = df[['班別', '卡號', '姓名', '所有時間戳記']]
            
            # 輸出到CSV
            output_file = os.path.join(output_dir, f'punch_record_{date_str}.csv')
            result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            # 準備返回訊息
            record_count = len(result_df)
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
    # 直接執行時的處理
    date_input = input("請輸入查詢日期 (格式: MM-DD，直接按Enter使用今天日期): ").strip()
    if date_input:
        export_punch_record(date_input, gui_mode=False)
    else:
        export_punch_record(gui_mode=False) 