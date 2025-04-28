import sqlite3
import os
import logging
from datetime import datetime
import argparse

# 檢查並安裝必要套件
try:
    import pandas as pd
except ImportError:
    import subprocess
    import sys
    print("正在安裝 pandas...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas"])
    import pandas as pd

def setup_logging():
    """設置日誌系統"""
    os.makedirs('logs', exist_ok=True)
    os.makedirs(os.path.join('.', 'output'), exist_ok=True)
    
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

db_path = os.path.join('.', 'db', 'source.db')
output_dir = os.path.join('.', 'output')
logger = setup_logging()

def get_db_connection():
    try:
        return sqlite3.connect(db_path)
    except Exception as e:
        logger.error(f"資料庫連接錯誤: {str(e)}")
        return None

def get_time_columns():
    """獲取所有刷卡時間欄位，兼容簡繁體"""
    try:
        conn = get_db_connection()
        if conn is None: return []
        
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(integrated_punch)")
        columns_info = cursor.fetchall()
        
        # 同時匹配簡體和繁體欄位名
        time_columns = [
            col[1] for col in columns_info 
            if col[1].startswith('刷卡時間') or col[1].startswith('刷卡时间')
        ]
        conn.close()
        return time_columns
    except Exception as e:
        logger.error(f"獲取時間列時發生錯誤: {str(e)}")
        return []

def format_timestamp(ts):
    if pd.isna(ts): return None
    ts = str(ts).strip()
    if not ts: return None
    if len(ts) == 6: return f"{ts[:2]}:{ts[2:4]}:{ts[4:6]}"
    return ts if ':' in ts else ts

def generate_html_table(df: pd.DataFrame) -> str:
    """生成帶分組結構的HTML表格(以卡號分組，固定欄位寬度，對齊優化)"""
    css_style = """
    <style>
        .account-group {
            border: 2px solid #007bff;
            border-radius: 5px;
            margin: 15px 0;
            background: #f8f9fa;
        }
        .account-header {
            padding: 12px;
            background: #007bff;
            color: white;
            font-weight: bold;
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 10px;
        }
        .header-item {
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .punch-table {
            width: 100%;
            border-collapse: collapse;
            table-layout: fixed;  /* 固定表格佈局 */
        }
        .punch-table th, .punch-table td {
            padding: 8px;
            border: 1px solid #dee2e6;
        }
        .punch-table th {
            background: #e9ecef;
            text-align: center;  /* 表頭置中 */
        }
        .punch-table td {
            word-wrap: break-word;  /* 允許文字換行 */
        }
        .punch-table td:not(.timestamp-col) {
            text-align: center;  /* 非時間戳記欄位置中 */
        }
        .timestamp-col {
            text-align: left;  /* 時間戳記靠左 */
        }
        .timestamp-odd { color: #1e88e5; }
        .timestamp-even { color: #d81b60; }
        .search-box {
            margin: 20px;
            text-align: center;
        }
        #searchInput {
            padding: 8px 15px;
            width: 300px;
            border-radius: 20px;
            border: 2px solid #007bff;
        }
    </style>
    """

    # 定義搜尋框
    search_box = """
    <div class="search-box">
        <input type="text" id="searchInput" placeholder="輸入卡號/公務帳號/姓名/班別..." onkeyup="filterGroups()">
    </div>
    """

    # JavaScript 搜尋函數
    js_script = """
    <script>
    function filterGroups() {
        const searchTerm = document.getElementById('searchInput').value.toUpperCase();
        const groups = document.getElementsByClassName('account-group');
        
        Array.from(groups).forEach(group => {
            const headerText = group.querySelector('.account-header').innerText.toUpperCase();
            group.style.display = headerText.includes(searchTerm) ? "block" : "none";
        });
    }
    </script>
    """

    html = f"""
    <html>
    <head>
        <title>打卡記錄總表（以卡號分組）</title>
        {css_style}
    </head>
    <body>
        <h2 style="text-align:center; color:#2c3e50;">打卡記錄總表</h2>
        {search_box}
        <div id="content">
    """

    # 以卡號分組處理
    grouped = df.groupby('卡號')
    for card_number, group in grouped:
        # 獲取唯一值
        unique_accounts = group['公務帳號'].unique()
        unique_names = group['姓名'].unique()
        unique_classes = group['班別'].unique()
        
        # 轉換為易讀格式
        accounts_str = '、'.join(map(str, unique_accounts))
        names_str = '、'.join(map(str, unique_names))
        classes_str = '、'.join(map(str, unique_classes))

        html += f"""
        <div class="account-group">
            <div class="account-header">
                <div class="header-item">卡號：{card_number}</div>
                <div class="header-item">公務帳號：{accounts_str}</div>
                <div class="header-item">姓名：{names_str}</div>
                <div class="header-item">班別：{classes_str}</div>
            </div>
            <table class="punch-table">
                <colgroup>
                    <col style="width: 10%;">  <!-- 日期 -->
                    <col style="width: 5%;">  <!-- 星期 -->
                    <col style="width: 5%;">  <!-- 打卡次數 -->
                    <col style="width: 80%;">  <!-- 時間戳記 -->
                </colgroup>
                <thead>
                    <tr>
                        <th>日期</th>
                        <th>星期</th>
                        <th>打卡次數</th>
                        <th>時間戳記</th>
                    </tr>
                </thead>
                <tbody>
        """
        
        # 填充表格內容
        for _, row in group.iterrows():
            timestamps = []
            for i, ts in enumerate(row['所有時間戳記'].split(', ')):
                if not ts: continue
                cls = "timestamp-odd" if (i+1)%2 else "timestamp-even"
                timestamps.append(f'<span class="{cls}">{ts}</span>')
            
            html += f"""
            <tr>
                <td>{row['日期']}</td>
                <td>{row['星期']}</td>
                <td>{row['打卡次數']}</td>
                <td class="timestamp-col">{' '.join(timestamps)}</td>
            </tr>
            """
        
        html += """
                </tbody>
            </table>
        </div>
        """

    html += f"""
        </div>
        {js_script}
    </body>
    </html>
    """
    return html

def export_punch_record():
    try:
        conn = get_db_connection()
        time_columns = get_time_columns()
        
        # 動態拼接時間欄位，確保無多餘逗號
        time_fields = ', '.join(time_columns) if time_columns else ''
        time_fields = f", {time_fields}" if time_fields else ''

        # 先獲取資料庫中的日期範圍
        date_range_query = """
        SELECT 
            MIN(刷卡日期) as min_date,
            MAX(刷卡日期) as max_date
        FROM integrated_punch
        """
        date_range = pd.read_sql(date_range_query, conn)
        min_date = pd.to_datetime(date_range['min_date'].iloc[0])
        max_date = pd.to_datetime(date_range['max_date'].iloc[0])
        
        # 創建完整的日期範圍
        date_range_df = pd.DataFrame({
            '日期': pd.date_range(min_date, max_date, freq='D').strftime('%Y-%m-%d'),
            '星期': pd.date_range(min_date, max_date, freq='D').strftime('%w').map({
                '0': '日', '1': '一', '2': '二', '3': '三',
                '4': '四', '5': '五', '6': '六'
            })
        })

        # 獲取打卡資料
        query = f"""
        SELECT 
            班別, 
            卡號, 
            公務帳號, 
            姓名,
            strftime('%Y-%m-%d', 刷卡日期) as 日期,
            CASE strftime('%w', 刷卡日期)
                WHEN '0' THEN '日'
                WHEN '1' THEN '一'
                WHEN '2' THEN '二'
                WHEN '3' THEN '三'
                WHEN '4' THEN '四'
                WHEN '5' THEN '五'
                WHEN '6' THEN '六'
            END as 星期
            {time_fields}  -- 動態插入時間欄位
        FROM integrated_punch
        ORDER BY 公務帳號, 日期
        """
        
        df = pd.read_sql(query, conn)
        
        # 處理時間戳
        for col in time_columns:
            df[col] = df[col].map(format_timestamp)
        
        df['所有時間戳記'] = df[time_columns].apply(
            lambda x: ', '.join([t for t in x if pd.notna(t)]), axis=1)
        
        df['打卡次數'] = df[time_columns].apply(
            lambda x: sum(1 for t in x if pd.notna(t)), axis=1)

        # 獲取所有唯一的員工資訊
        employees = df[['卡號', '公務帳號', '姓名', '班別']].drop_duplicates()
        
        # 為每個員工創建完整的日期記錄
        complete_records = []
        for _, emp in employees.iterrows():
            # 複製日期範圍並添加員工資訊
            emp_dates = date_range_df.copy()
            for col, val in emp.items():
                emp_dates[col] = val
            
            # 合併打卡資料
            emp_dates = emp_dates.merge(
                df[df['卡號'] == emp['卡號']][['日期', '所有時間戳記', '打卡次數']],
                on='日期',
                how='left'
            )
            
            # 填充空值
            emp_dates['所有時間戳記'] = emp_dates['所有時間戳記'].fillna('')
            emp_dates['打卡次數'] = emp_dates['打卡次數'].fillna(0)
            
            complete_records.append(emp_dates)
        
        # 合併所有記錄
        final_df = pd.concat(complete_records, ignore_index=True)
        
        # 生成HTML文件
        output_file = os.path.join(output_dir, 'punch_by_account.html')
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(generate_html_table(final_df))
        
        logger.info(f"文件已生成：{output_file}")
        
    except Exception as e:
        logger.error(f"處理失敗：{str(e)}")
    finally:
        if conn: conn.close()

if __name__ == '__main__':
    export_punch_record()