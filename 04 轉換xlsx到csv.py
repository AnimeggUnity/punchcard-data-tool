import pandas as pd
import os
import tkinter as tk
from tkinter import ttk, messagebox
import logging
from typing import List, Dict
from pathlib import Path

# 設置日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ExcelConverterGUI:
    def __init__(self):
        self.data_dir = os.path.join('.', 'data')
        self.output_dir = os.path.join('.', 'data')
        
        # 確保資料夾存在
        os.makedirs(self.data_dir, exist_ok=True)
        
        # 創建主視窗
        self.window = tk.Tk()
        self.window.title("Excel 轉 CSV 工具")
        self.window.geometry("600x500")
        
        # 創建主框架
        self.main_frame = ttk.Frame(self.window, padding="10")
        self.main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 檔案列表框架
        self.list_frame = ttk.LabelFrame(self.main_frame, text="Excel 檔案列表", padding="5")
        self.list_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # 建立檔案列表
        self.file_listbox = tk.Listbox(self.list_frame, selectmode=tk.MULTIPLE)
        self.file_listbox.pack(fill=tk.BOTH, expand=True)
        
        # 儲存選項框架
        self.save_option_frame = ttk.LabelFrame(self.main_frame, text="儲存選項", padding="5")
        self.save_option_frame.pack(fill=tk.X, pady=5)
        
        # 儲存模式選擇
        self.save_mode = tk.StringVar(value="original")
        ttk.Radiobutton(
            self.save_option_frame,
            text="保持原始檔名",
            variable=self.save_mode,
            value="original"
        ).pack(anchor=tk.W)
        ttk.Radiobutton(
            self.save_option_frame,
            text="另存為司機名單.csv (用於夜點清單程式)",
            variable=self.save_mode,
            value="driver_list"
        ).pack(anchor=tk.W)
        
        # 按鈕框架
        self.button_frame = ttk.Frame(self.main_frame)
        self.button_frame.pack(fill=tk.X, pady=5)
        
        # 轉換按鈕
        self.convert_button = ttk.Button(
            self.button_frame,
            text="轉換選擇的檔案",
            command=self.convert_selected_files
        )
        self.convert_button.pack(side=tk.LEFT, padx=5)
        
        # 重新整理按鈕
        self.refresh_button = ttk.Button(
            self.button_frame,
            text="重新整理檔案列表",
            command=self.refresh_file_list
        )
        self.refresh_button.pack(side=tk.LEFT)
        
        # 初始化檔案列表
        self.refresh_file_list()
    
    def refresh_file_list(self):
        """重新整理檔案列表"""
        self.file_listbox.delete(0, tk.END)
        excel_files = self.get_excel_files()
        for file in excel_files:
            self.file_listbox.insert(tk.END, file)
    
    def get_excel_files(self) -> List[str]:
        """獲取資料夾中的 Excel 檔案"""
        excel_files = []
        for file in os.listdir(self.data_dir):
            if file.endswith(('.xlsx', '.xls')) and not file.startswith('~$'):
                excel_files.append(file)
        return sorted(excel_files)
    
    def convert_selected_files(self):
        """轉換選擇的檔案"""
        selected_indices = self.file_listbox.curselection()
        if not selected_indices:
            messagebox.showwarning("警告", "請選擇要轉換的檔案")
            return
        
        selected_files = [self.file_listbox.get(idx) for idx in selected_indices]
        success_count = 0
        error_count = 0
        save_mode = self.save_mode.get()
        
        all_data = []  # 用於收集所有數據，如果需要合併為司機名單
        
        for file in selected_files:
            try:
                excel_path = os.path.join(self.data_dir, file)
                # 讀取 Excel 檔案的所有工作表
                excel_data = pd.ExcelFile(excel_path)
                
                for sheet_name in excel_data.sheet_names:
                    try:
                        # 讀取工作表
                        df = pd.read_excel(excel_path, sheet_name=sheet_name)
                        
                        # 檢查必要的欄位
                        required_columns = ['公務帳號', '卡號', '姓名']
                        missing_columns = [col for col in required_columns if col not in df.columns]
                        
                        if missing_columns:
                            logger.warning(f"工作表 {sheet_name} 缺少必要欄位: {', '.join(missing_columns)}")
                            continue
                        
                        # 只保留必要的欄位
                        df = df[required_columns]
                        
                        if save_mode == "original":
                            # 使用原始檔名模式
                            output_name = f"{Path(file).stem}_{sheet_name}.csv"
                            output_path = os.path.join(self.output_dir, output_name)
                            df.to_csv(output_path, index=False, encoding='utf-8-sig')
                            logger.info(f"已將 {file} 的工作表 {sheet_name} 轉換為 {output_name}")
                        else:
                            # 收集數據以便後續合併
                            all_data.append(df)
                        
                        success_count += 1
                        
                    except Exception as e:
                        logger.error(f"轉換工作表 {sheet_name} 時發生錯誤: {str(e)}")
                        error_count += 1
                
            except Exception as e:
                logger.error(f"處理檔案 {file} 時發生錯誤: {str(e)}")
                error_count += 1
        
        # 如果是司機名單模式，合併所有數據並儲存
        if save_mode == "driver_list" and all_data:
            try:
                combined_df = pd.concat(all_data, ignore_index=True)
                # 移除重複的資料
                combined_df = combined_df.drop_duplicates()
                output_path = os.path.join(self.output_dir, "司機名單.csv")
                combined_df.to_csv(output_path, index=False, encoding='utf-8-sig')
                logger.info("已合併所有資料並儲存為司機名單.csv")
            except Exception as e:
                logger.error(f"合併資料時發生錯誤: {str(e)}")
                error_count += 1
        
        # 顯示結果
        message = f"轉換完成！\n成功: {success_count} 個工作表\n"
        if error_count > 0:
            message += f"失敗: {error_count} 個工作表"
        messagebox.showinfo("轉換結果", message)
    
    def run(self):
        """執行主程式"""
        self.window.mainloop()

def main():
    """主程式進入點"""
    app = ExcelConverterGUI()
    app.run()

if __name__ == "__main__":
    main() 