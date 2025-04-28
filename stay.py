import os
import subprocess
import tkinter as tk
from tkinter import ttk, messagebox
import sys
import importlib

# 先檢查並安裝 packaging
try:
    from packaging import version
except ImportError:
    print("正在安裝必要套件 packaging...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "packaging"])
        from packaging import version
        print("packaging 安裝成功！")
    except Exception as e:
        print(f"無法安裝 packaging: {e}")
        # 定義一個簡單的版本比較函數作為備用
        class SimpleVersion:
            def __init__(self, version_str):
                self.version_str = version_str
                
            def __lt__(self, other):
                return tuple(map(int, self.version_str.split('.'))) < tuple(map(int, other.version_str.split('.')))
        
        def parse(version_str):
            return SimpleVersion(version_str)
        version = type('Version', (), {'parse': staticmethod(parse)})()

# 所需套件及其最低版本
required_packages = {
    'pandas': '1.0.0',
    'openpyxl': '3.0.0',
    'flask': '2.0.0',
    'packaging': '20.0',
    'logging': None,  # logging 是 Python 標準庫的一部分
    'argparse': None,  # argparse 是 Python 標準庫的一部分
    'sqlite3': None,  # sqlite3 是 Python 標準庫的一部分
    'tkinter': None,  # tkinter 是 Python 標準庫的一部分
    'ttkthemes': None,  # ttkthemes是GUI主題套件
    'datetime': None,  # datetime 是 Python 標準庫的一部分
}

def check_and_install_packages(packages):
    for package, min_version in packages.items():
        try:
            # 嘗試導入套件
            pkg = importlib.import_module(package)
            if min_version:
                # 檢查版本
                installed_version = pkg.__version__
                if version.parse(installed_version) < version.parse(min_version):
                    print(f"警告: {package} 版本過低 (已安裝版本: {installed_version}, 最低要求: {min_version})")
                    subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", package])
                    print(f"{package} 已成功升級至最新版本")
                else:
                    print(f"{package} 已經安裝 (版本: {installed_version})")
            else:
                print(f"{package} 已經安裝 (標準庫)")
        except ImportError:
            print(f"警告: {package} 未安裝")
            try:
                # 安裝缺少的套件
                subprocess.check_call([sys.executable, "-m", "pip", "install", package])
                print(f"{package} 已成功安裝")
            except subprocess.CalledProcessError:
                print(f"錯誤: 無法安裝 {package}")
        except AttributeError:
            print(f"注意: {package} 無法檢查版本")

def list_scripts(script_dir, current_dir=None):
    """ 列出指定目錄下的所有 .py 檔案 (不包含此起始頁面本身) 
    
    Args:
        script_dir: scripts 目錄的路徑
        current_dir: 當前目錄的路徑
    """
    scripts = []
    try:
        # 確保 scripts 目錄存在
        if not os.path.exists(script_dir):
            os.makedirs(script_dir)
            print(f"已創建目錄: {script_dir}")
        
        # 先檢查當前目錄的 Python 檔案
        if current_dir:
            for filename in os.listdir(current_dir):
                if filename.endswith(".py") and filename != os.path.basename(__file__):
                    full_path = os.path.join(current_dir, filename)
                    if os.path.isfile(full_path) and os.access(full_path, os.R_OK):
                        scripts.append(("current", filename))
        
        # 再檢查 scripts 目錄的 Python 檔案
        if os.path.exists(script_dir):
            for filename in os.listdir(script_dir):
                if filename.endswith(".py"):
                    full_path = os.path.join(script_dir, filename)
                    if os.path.isfile(full_path) and os.access(full_path, os.R_OK):
                        scripts.append(("scripts", filename))
        
        if not scripts:
            print("沒有找到可執行的 Python 檔案")
            messagebox.showinfo("提示", 
                "在當前目錄和腳本目錄中都沒有找到可執行的 Python 檔案。\n"
                "請確保：\n"
                "1. 檔案副檔名為 .py\n"
                "2. 檔案具有讀取權限")
                
    except Exception as e:
        print(f"讀取目錄時發生錯誤: {e}")
        messagebox.showerror("錯誤", f"無法讀取程式目錄: {e}")
        return []
        
    return sorted(scripts, key=lambda x: x[1])  # 按照檔名排序

def run_script(script_path, output_area, status_label):
    """ 執行指定的 Python 程式並將輸出顯示在 GUI 介面上
    
    特殊處理：
    1. 對於一般的 Python 程式，使用 subprocess 執行並捕獲輸出
    2. 對於打卡記錄查詢程式，使用特殊的 GUI 介面處理
    
    參數：
        script_path: 要執行的 Python 檔案路徑
        output_area: 用於顯示輸出的文字區域
        status_label: 用於顯示狀態的標籤
    """
    try:
        # 更新狀態
        status_label.config(text=f"正在執行: {os.path.basename(script_path)}")
        
        # === 特殊處理打卡記錄查詢程式 ===
        script_name = os.path.basename(script_path)
        if script_name in ['03 打卡記錄查詢.py', '03 打卡紀錄查詢htm.py']:
            # 1. 創建日期輸入對話框
            date_dialog = tk.Toplevel()  # 創建新的對話框視窗
            date_dialog.title("輸入查詢日期")
            date_dialog.geometry("300x150")  # 設定對話框大小
            
            # 2. 設置對話框置頂和模態
            date_dialog.transient(output_area.winfo_toplevel())  # 設置對話框的父視窗
            date_dialog.grab_set()  # 使對話框成為模態(強制用戶先處理此對話框)
            
            # 3. 創建說明標籤
            ttk.Label(date_dialog, 
                     text="請輸入查詢日期\n(格式: MM-DD，直接按確定使用今天日期)",
                     wraplength=250,  # 設定文字換行寬度
                     justify='center').pack(pady=10)
            
            # 4. 創建日期輸入框
            date_var = tk.StringVar()  # 創建變數來儲存輸入值
            date_entry = ttk.Entry(date_dialog, textvariable=date_var)
            date_entry.pack(pady=10)
            
            # 5. 定義查詢執行函數
            def execute_query():
                # 獲取並清理輸入的日期
                date_str = date_var.get().strip()
                date_dialog.destroy()  # 關閉對話框
                
                # 清空輸出區域
                output_area.delete("1.0", tk.END)
                
                # 6. 使用 importlib 動態導入模組
                import importlib.util
                spec = importlib.util.spec_from_file_location("punch_record", script_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                # 7. 執行查詢並處理結果
                result = module.export_punch_record(
                    date_str if date_str else None,  # 如果沒有輸入日期，則使用 None
                    gui_mode=True  # 指定為 GUI 模式
                )
                if result:
                    output_area.insert(tk.END, result + '\n')
                
                # 8. 更新狀態
                status_label.config(text="執行完成")
            
            # 9. 創建確定按鈕
            ttk.Button(date_dialog, text="確定", command=execute_query).pack(pady=10)
            
            # 10. 設置焦點和快捷鍵
            date_entry.focus_set()  # 自動聚焦到輸入框
            date_dialog.bind('<Return>', lambda e: execute_query())  # 綁定 Enter 鍵
            
            return  # 提前返回，不執行後續的一般程式處理邏輯
        
        # === 一般程式的處理邏輯 ===
        # 設定環境變數，確保使用 UTF-8 編碼
        my_env = os.environ.copy()
        my_env["PYTHONIOENCODING"] = "utf-8"
        
        # 使用子進程執行程式
        process = subprocess.Popen(["python", script_path],
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE,
                                  text=True,
                                  encoding='utf-8',
                                  env=my_env)
        
        # 清空輸出區域
        output_area.delete("1.0", tk.END)

        while True:
            # 讀取輸出
            stdout = process.stdout.readline()
            stderr = process.stderr.readline()

            # 沒有東西可讀取就檢查子程序是否結束
            if not stdout and not stderr:
                if process.poll() is not None:
                    break
                else:
                    continue

            # 更新輸出區域
            if stdout:
                output_area.insert(tk.END, stdout)
            if stderr:
                output_area.insert(tk.END, stderr)

            # 確保輸出可以即時顯示在 GUI 上
            output_area.see(tk.END)  # 自動滾動到最新的輸出
            output_area.update()

        # 程式執行完成，更新狀態
        status_label.config(text="執行完成")

    except FileNotFoundError:
        status_label.config(text="錯誤：找不到 Python 直譯器")
        messagebox.showerror("錯誤", "找不到 Python 直譯器，請確認已安裝 Python 並將其加入環境變數。")
    except subprocess.CalledProcessError as e:
        status_label.config(text="錯誤：執行失敗")
        messagebox.showerror("錯誤", f"執行程式時發生錯誤: {e}")
    except Exception as e:
        status_label.config(text="錯誤：未知錯誤")
        messagebox.showerror("錯誤", f"發生未知錯誤: {e}")

def create_buttons(script_dir, button_frame, output_area, status_label, current_dir=None):
    """ 根據程式列表創建按鈕 """
    scripts = list_scripts(script_dir, current_dir)
    buttons = []
    
    # 設定按鈕樣式
    style = ttk.Style()
    style.configure('Custom.TButton',
                   padding=(5, 5),
                   anchor='center')
    
    # 創建一個框架來容納所有按鈕
    container_frame = ttk.Frame(button_frame)
    container_frame.pack(fill=tk.BOTH, expand=True)
    
    for dir_type, script in scripts:
        # 根據檔案位置決定完整路徑
        if dir_type == "current":
            script_path = os.path.join(current_dir, script)
        else:
            script_path = os.path.join(script_dir, script)
            
        # 創建按鈕容器框架
        btn_container = ttk.Frame(container_frame)
        btn_container.pack(fill=tk.X, padx=20, pady=2)
        
        # 創建按鈕
        button = ttk.Button(btn_container, 
                          text=f"{script} ({dir_type})",  # 顯示檔案位置
                          style='Custom.TButton',
                          command=lambda path=script_path: run_script(path, output_area, status_label))
        
        button.pack(fill=tk.X)
        buttons.append(button)

    if not scripts:
        messagebox.showinfo("訊息", "目前沒有可執行的 Python 程式。")
    
    return len(scripts)

def main():
    """ 主程式邏輯 """
    try:
        # 獲取程式執行檔案的絕對路徑
        if getattr(sys, 'frozen', False):
            current_dir = os.path.dirname(sys.executable)
        else:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            
        # 切換到程式所在目錄
        os.chdir(current_dir)
        
        # 設定 scripts 目錄路徑
        scripts_dir = os.path.join(current_dir, "scripts")
        
        # 顯示目前的工作目錄
        print(f"目前工作目錄: {os.getcwd()}")
        print(f"腳本目錄: {scripts_dir}")
        
        # 在視窗標題顯示目錄資訊
        window = tk.Tk()
        window.title(f"Python 程式啟動器 - 腳本目錄: {scripts_dir}")
        
        # === 視窗風格設定 ===
        style = ttk.Style(window)
        style.theme_use('xpnative')  # 使用系統原生主題，讓應用程式看起來更專業
                                # 可選用的主題：'alt'(替代), 'default'(預設), 
                                # 'classic'(經典), 'vista', 'xpnative'(Windows原生)

        # === 主分割區域設定 ===
        # 設定主視窗的權重，使分割區域可以正確調整大小
        window.grid_rowconfigure(0, weight=1)
        window.grid_columnconfigure(0, weight=1)
        
        # 創建可調整的分割視窗
        paned = ttk.PanedWindow(window, orient=tk.HORIZONTAL)
        paned.grid(row=0, column=0, sticky='nsew', padx=5, pady=5)

        # === 左側區域設定 ===
        left_frame = ttk.Frame(paned)
        left_frame.pack_propagate(False)  # 防止 frame 自動縮放
        left_frame.configure(width=200, height=600)  # 設定初始大小
        
        # === 右側區域設定 ===
        right_frame = ttk.Frame(paned)
        right_frame.pack_propagate(False)  # 防止 frame 自動縮放
        right_frame.configure(width=800, height=600)  # 設定初始大小
        
        # 添加左右兩側到分割視窗，設定最小寬度
        paned.add(left_frame, weight=0)  # weight=0 表示不會隨視窗縮放而改變比例
        paned.add(right_frame, weight=1) # weight=1 表示會隨視窗縮放而改變大小

        # === 右側框架內部佈局 ===
        # 創建一個框架來容納輸出區域和狀態列
        right_content = ttk.Frame(right_frame)
        right_content.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 輸出區域
        output_area = tk.Text(right_content, 
                             wrap=tk.WORD,
                             font=("Courier New", 10))
        output_area.pack(fill=tk.BOTH, expand=True, pady=(0, 5))  # 底部留出空間給狀態列
        
        # 狀態列框架
        status_frame = ttk.Frame(right_content)
        status_frame.pack(fill=tk.X, pady=(0, 5))
        
        # 狀態標籤
        status_label = ttk.Label(status_frame, text="就緒")
        status_label.pack(side=tk.LEFT, padx=5)

        # === 按鈕區域框架設定 ===
        button_frame = ttk.Frame(left_frame)
        button_frame.pack(fill=tk.BOTH, expand=True)

        # === 捲軸和畫布設定 ===
        scrollbar = None
        canvas = None
        
        # 創建按鈕並獲取按鈕數量
        if len(list_scripts(scripts_dir, current_dir)) > 8:  # 如果按鈕數量超過8個才顯示捲軸
            scrollbar = ttk.Scrollbar(button_frame, orient=tk.VERTICAL)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

            canvas = tk.Canvas(button_frame, 
                             yscrollcommand=scrollbar.set,
                             highlightthickness=0,  # 移除畫布邊框
                             bd=0)                  # 移除邊框
            canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            
            if scrollbar:
                scrollbar.config(command=canvas.yview)

            # === 可捲動框架設定 ===
            scrollable_frame = ttk.Frame(canvas)
            canvas_window = canvas.create_window((0, 0), 
                                              window=scrollable_frame, 
                                              anchor='nw')

            # === 自適應寬度設定 ===
            def on_frame_configure(event=None):
                canvas.configure(scrollregion=canvas.bbox('all'))
                canvas.itemconfig(canvas_window, width=canvas.winfo_width())

            scrollable_frame.bind('<Configure>', on_frame_configure)
            canvas.bind('<Configure>', on_frame_configure)

            # === 滾輪事件設定 ===
            def _on_mousewheel(event):
                canvas.yview_scroll(int(-1*(event.delta/120)), "units")
            canvas.bind_all("<MouseWheel>", _on_mousewheel)
            canvas.bind_all("<Button-4>", _on_mousewheel)
            canvas.bind_all("<Button-5>", _on_mousewheel)
            
            create_buttons(scripts_dir, scrollable_frame, output_area, status_label, current_dir)
        else:
            # 如果按鈕數量不多，直接在 button_frame 中創建按鈕
            create_buttons(scripts_dir, button_frame, output_area, status_label, current_dir)

        # 創建一個顯示目錄資訊的標籤
        dir_info = f"當前目錄: {current_dir}\n腳本目錄: {scripts_dir}"
        dir_label = ttk.Label(window, 
                            text=dir_info,
                            wraplength=800)
        dir_label.grid(row=1, column=0, sticky='w', padx=5, pady=5)

        # 修改重新整理按鈕的功能
        def refresh_scripts():
            for widget in button_frame.winfo_children():
                widget.destroy()
            create_buttons(scripts_dir, button_frame, output_area, status_label, current_dir)
            
        refresh_btn = ttk.Button(window, 
                               text="重新整理腳本列表", 
                               command=refresh_scripts)
        refresh_btn.grid(row=2, column=0, sticky='w', padx=5, pady=5)

        window.mainloop()

    except Exception as e:
        messagebox.showerror("錯誤", f"程式初始化失敗: {e}")
        return

if __name__ == "__main__":
    main()