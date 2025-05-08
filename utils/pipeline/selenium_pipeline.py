import json, csv, os, logging, glob
from datetime import datetime
from multiprocessing import Process, Manager
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO,
    filename='/app/vietstock/crawled_data/crawl.log',
    filemode='a',  # Chế độ append
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

class SeleniumPipeline:
    """ Pipeline để xử lý và lưu dữ liệu từ Selenium """


    def __init__(self, output_folder= f"vietstock/crawled_data"):
        self.output_folder = output_folder
        os.makedirs(self.output_folder, exist_ok=True)  # Tạo thư mục nếu chưa có
        self.data = {}  # Dictionary chứa dữ liệu theo từng bảng
        self._seen_keys = {}  # NEW: giữ các key duy nhất đã gặp
    
    def get_unique_key(self, item, source):
        # Tuỳ theo bảng mà tạo key khác nhau
        if source == "HNX_DSCK":
            return item.get("CK_id", "").strip()
        elif source == "HNX_KQGD":
            return f"{item.get('CK_id', '').strip()}|{item.get('date', '').strip()}"
        elif source  in ("DSCK_HNX", "DSCK_HSX", "DSCK_UpCom"):
            return item.get("Mã", "").strip()
        elif source == "HNX_KQGD_CAFEF":
            return f"{item.get('CK_id', '').strip()}|{item.get('ngay', '').strip()}"
        elif source in (f"vietstock_company_{item.get('exchange')},"): #vietstock company
            return item.get('CK_id', '').strip()
        elif source in (f"vietstock_price_{item.get('exchange')}_{item.get('ngay')}"): #vietstock price
            return f"{item.get('maCK', '').strip()}|{item.get('ngay', '').strip()}"
        elif source == "Vietstock_symbol_News":#vietstock news symbol
            return f"{item.get('title', '').strip()}|{item.get('url', '').strip()}"
        elif source == "Vietstock_News_Latest": #vietstock News_Latest
            return f"{item.get('title', '').strip()}|{item.get('url', '').strip()}"
        return json.dumps(item, sort_keys=True)  # fallback


    def process_item(self, item, source):
        if source not in self.data:
            self.data[source] = []
            self._seen_keys[source] = set()  # Đảm bảo khởi tạo luôn _seen_keys cho source này
        
        unique_key = self.get_unique_key(item, source)
        
            # Bỏ qua nếu key không hợp lệ
        if not unique_key or unique_key == "|":
            print(f"[⚠️ Bỏ qua] Dữ liệu thiếu CK_id hoặc date_value → {item}")
            logging.warning(f"[⚠️ Bỏ qua] Dữ liệu thiếu CK_id hoặc date_value → {item}")
            return

        if unique_key in self._seen_keys[source]:
            print(f"[⚠️ Duplicate] {unique_key} đã tồn tại, bỏ qua.")
            logging.warning(f"[⚠️ Duplicate] {unique_key} đã tồn tại, bỏ qua.")
            return

        self._seen_keys[source].add(unique_key)
        processed_item = getattr(self, f"process_{source}", self.default_process)(item)
        self.data[source].append(processed_item)
        print(f"✔️ Đã xử lý: {unique_key}")
        logging.info(f"✔️ Đã xử lý: {unique_key}")

    def process_HNX_DSCK(self, item):
        """ Xử lý bảng HNX_DSCK """
        return {
            "stt": item.get("stt", "").strip(),
            "Mã chứng khoán": item.get("CK_id", "").strip(),
            "Tên tổ chức": item.get("organization", "").strip(),
            "Ngành": item.get("branch", "").strip(),
            "Ngày giao dịch đầu tiên": item.get("start_trading_date", "").strip(),
            "Khối lượng niêm yết": item.get("listed_volume", "").replace(",", "").strip(),
            "Tổng giá trị niêm yết": item.get("total_listing_value", "").replace(",", "").strip()
        }
    
    def process_HNX_KQGD(self, item):
        return {
            "Số tt": (item.get("STT") or "").strip(),
            "Ngày": (item.get("date") or "").strip(),
            "Mã CK": (item.get("CK_id") or "").strip(),
            "Giá tham chiếu": (item.get("gia_tham_chieu") or "").strip(),
            "Giá trần": (item.get("gia_tran") or "").strip(),
            "Giá sàn": (item.get("gia_san") or "").strip(),
            "Giá mở cửa ": (item.get("gia_mo_cua") or "").replace(",", "").strip(),
            "Giá đóng cửa ": (item.get("gia_dong_cua") or "").replace(",", "").strip(),
            "Giá cao nhất": (item.get("gia_cao_nhat") or "").replace(",", "").strip(),
            "Giá Thấp Nhất": (item.get("gia_thap_nhat") or "").replace(",", "").strip(),
            "Thay đổi (Điểm)": (item.get("thay_doi_diem") or "").replace(",", "").strip(),
            "Thay đổi (%)": (item.get("thay_doi_phan_tram") or "").replace(",", "").strip(),
        }

    def process_HNX_KQGD_CAFEF(self, item):
        return {
            "Ngày": (item.get("ngay") or "").strip(),
            "Đóng cửa": (item.get("dong_cua") or "").strip(),
            "Điều chỉnh": (item.get("dieu_chinh") or "").strip(),
            "Thay đổi": (item.get("thay_doi") or "").strip(),
            "Khối lượng GDKL": (item.get("khoi_luong_GDKL") or "").strip(),
            "Giá trị GDKL ": (item.get("gia_tri_GDKL") or "").replace(",", "").strip(),
            "Khối lượng GDTT ": (item.get("khoi_luong_GDTT") or "").replace(",", "").strip(),
            "Giá trị GDTT": (item.get("gia_tri_GDTT") or "").replace(",", "").strip(),
            "Mở cửa": (item.get("mo_cua") or "").replace(",", "").strip(),
            "Cao nhất": (item.get("cao_nhat") or "").replace(",", "").strip(),
            "Thấp nhất": (item.get("thap_nhat") or "").replace(",", "").strip(),
        }
    

    def process_Vietstock_Symbol_News(self, item):
    # Chuẩn hóa ngày
        try:
            date_obj = datetime.strptime(item.get("date", ""), "%d/%m/%Y")
            date_str = date_obj.strftime("%Y-%m-%d")
        except:
            date_str = item.get("date", "").strip()

        return {
            "URL": item.get("url", "").strip(),
            "Mã CK": item.get("symbol", "").strip(),
            "Tiêu đề": item.get("title", "").strip(),
            "Nội dung": (item.get("content") or "").strip(),
            "Ngày": date_str,
        }
    
    def process_Vietstock_News_latest(self, item):
        try:
            date_obj = datetime.strptime(item.get("date", ""), "%d/%m/%Y")
            date_str = date_obj.strftime("%Y-%m-%d")
        except:
            date_str = item.get("date", "").strip()

        return {
            "date": date_str,
            "title": item.get("title", "").strip(),
            "content": (item.get("content") or "").strip(),
            "author" : (item.get("author") or "").strip(),
            "publish_time" : (item.get("publish_time") or "").strip(),
            "URL": item.get("url", "").strip(),
        }

    def default_process(self, item):
        """ Xử lý mặc định nếu chưa có hàm riêng """
        return item

    def save_data(self, temp=False):

        # Lưu từng bảng dữ liệu ra file CSV.
        # Nếu temp=True → ghi file tạm (_tmp.csv) 
        # Nếu temp=False → ghi file chính (.csv)
        
        for source, items in self.data.items():
            if not items:
                continue

            safe_source = source.replace("/", "-")
            suffix = "_tmp.csv" if temp else ".csv"
            file_path = os.path.join(self.output_folder, f"{safe_source}{suffix}")

            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            headers = list(items[0].keys())

            with open(file_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=headers, quoting=csv.QUOTE_ALL)
                writer.writeheader()
                writer.writerows(items)

            print(f"💾 Đã lưu file { 'tạm' if temp else 'chính' }: {file_path} ({len(items)} dòng)")
            logging.info(f"💾 Đã lưu file { 'tạm' if temp else 'chính' }: {file_path} ({len(items)} dòng)")


"""
# hàm gộp file 
def merge_csv_files(pattern, output_file, remove_temp=True):
    all_files = glob.glob(pattern)
    if not all_files:
        print("⚠️ Không tìm thấy file tạm để gộp.")
        return

    df_list = [pd.read_csv(f) for f in all_files]
    merged_df = pd.concat(df_list, ignore_index=True)
    merged_df.drop_duplicates(inplace=True)

    merged_df.to_csv(output_file, index=False, encoding="utf-8")
    print(f"✅ Đã gộp {len(all_files)} file thành {output_file} ({len(merged_df)} dòng)")

    if remove_temp:
        for f in all_files:
            os.remove(f)
            print(f"🗑️ Đã xoá file tạm: {f}")
"""

import glob
import pandas as pd
import os

def merge_csv_files(pattern, output_file):
    new_files = glob.glob(pattern)
    if not new_files:
        print("⚠️ Không tìm thấy file mới.")
        return

    # Đọc tất cả file mới crawl
    new_df = pd.concat((pd.read_csv(f) for f in new_files), ignore_index=True)

    # Nếu file output đã có ➔ gộp thêm
    if os.path.exists(output_file):
        old_df = pd.read_csv(output_file)
        merged_df = pd.concat([old_df, new_df], ignore_index=True)
        merged_df.drop_duplicates(subset=["CK_id"] , keep="first", inplace=True)
    else:
        merged_df = new_df

    merged_df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"✅ Merge thành công {len(new_files)} file vào {output_file}")

    # Xóa file tạm sau khi merge
    for f in new_files:
        try:
            os.remove(f)
            print(f"🗑️ Đã xóa file tạm: {f}")
        except Exception as e:
            print(f"❌ Lỗi khi xóa {f}: {e}")



from sqlalchemy import create_engine
import pandas as pd
import logging

from sqlalchemy import create_engine, text  # Thêm import text

"""
def save_csv_to_postgres(csv_file, db_url, table_name, if_exists="replace"):
    try:
        df = pd.read_csv(csv_file)
        logging.info(f"Đọc {len(df)} dòng từ CSV thành công.")
    except Exception as e:
        logging.error(f"Không thể đọc CSV file {csv_file}: {str(e)}")
        return
    
    logging.info(f"Các cột trong DataFrame: {df.columns.tolist()}")
    logging.info(f"Dòng đầu tiên: {df.iloc[0].to_dict()}")

    df = df.rename(columns={
        'CK_id': 'symbol',
        'company': 'company_name',
        'branch': 'industry',
        'exchange': 'exchange',
        'listed_volume': 'listed_volume'
    })
    df = df.drop(columns=['stt'], errors='ignore')
    df['listed_volume'] = df['listed_volume'].astype(str).str.replace('"', '').str.replace(',', '').astype(int)
    df = df.drop_duplicates(subset=['symbol', 'exchange'])
    
    logging.info(f"Số dòng sau khi làm sạch và loại trùng: {len(df)}")
    logging.info(f"Các cột sau khi ánh xạ: {df.columns.tolist()}")

    try:
        engine = create_engine(db_url, pool_size=10, max_overflow=20, pool_timeout=30, pool_recycle=1800)
        logging.info("Đã kết nối đến DB")
    except Exception as e:
        logging.error(f"Không thể kết nối tới cơ sở dữ liệu PostgreSQL: {str(e)}")
        return

    if if_exists == "replace":
        try:
            with engine.connect() as conn:
                # Sử dụng text() để thực thi lệnh TRUNCATE
                conn.execute(text(f"TRUNCATE TABLE {table_name}"))
                conn.commit()  # Đảm bảo commit lệnh TRUNCATE
                logging.info("Đã xóa dữ liệu cũ trong bảng.")
        except Exception as e:
            logging.error(f"Không thể TRUNCATE bảng {table_name}: {str(e)}")
            return

    try:
        df.to_sql(
            table_name,
            engine,
            index=False,
            if_exists="append",
            method="multi",
            chunksize=1000
        )
        logging.info(f"✅ Đã lưu dữ liệu {len(df)} dòng vào bảng {table_name}.")
    except Exception as e:
        logging.error(f"Không thể lưu dữ liệu vào bảng {table_name}: {str(e)}")
        raise
"""

TABLE_MAPPINGS = {
    "crawler_company": {
        "column_mapping": {
            'CK_id': 'symbol',
            'company': 'company_name',
            'branch': 'industry',
            'exchange': 'exchange',
            'listed_volume': 'listed_volume'
        },
        "drop_columns": ['stt'],
        "unique_columns": ['symbol', 'exchange'],
        "data_transforms": {
            'listed_volume': lambda x: int(str(x).replace('"', '').replace(',', ''))
        },
        "dependent_tables": None
    },
    "crawler_price": {
        "column_mapping": {
            'ngay' : 'date',
            'exchange': 'exchange',
            'maCK': 'stock',
            'tham_chieu' : 'basic',
            'mo_cua' : 'open',
            'dong_cua' : 'close',
            'cao_nhat' : 'high',
            'thap_nhat' : 'low',
            'trung_binh' : 'average',
            'thay_doi_tang_giam' : 'change_abs',
            'thay_doi_phan_tram' : 'change_percent',
            'kl_gdkl' : 'order_matching_vol',
            'gt_gdkl' : 'order_matching_val',
            'kl_gdtt' : 'put_through_vol',
            'gt_gdtt' : 'put_through_val',
            'kl_tgd' : 'total_transaction_vol',
            'gt_tgd' : 'total_transaction_val',
            'von_hoa' : 'market_cap'
        },
        "drop_columns": ['stt'],
        "unique_columns": ['stock', 'date'],
        "data_transforms": {
            'date': lambda x: pd.to_datetime(x, format= '%d/%m/%Y', errors='coerce').date() if pd.notnull(x) else None,
            'basic': lambda x: float(str(x).replace(',', '').replace('"', '')) if pd.notnull(x) and str(x).replace(',', '').replace('"', '').strip() and str(x).replace(',', '').replace('"', '').strip() != '-'  else None,
            'open': lambda x: float(str(x).replace(',', '').replace('"', '')) if pd.notnull(x) and str(x).replace(',', '').replace('"', '').strip() and str(x).replace(',', '').replace('"', '').strip() != '-'  else None,
            'close': lambda x: float(str(x).replace(',', '').replace('"', '')) if pd.notnull(x) and str(x).replace(',', '').replace('"', '').strip() and str(x).replace(',', '').replace('"', '').strip() != '-'  else None,
            'high': lambda x: float(str(x).replace(',', '').replace('"', '')) if pd.notnull(x) and str(x).replace(',', '').replace('"', '').strip() and str(x).replace(',', '').replace('"', '').strip() != '-'  else None,
            'low': lambda x: float(str(x).replace(',', '').replace('"', '')) if pd.notnull(x) and str(x).replace(',', '').replace('"', '').strip() and str(x).replace(',', '').replace('"', '').strip() != '-'  else None,
            'average': lambda x: float(str(x).replace(',', '').replace('"', '')) if pd.notnull(x) and str(x).replace(',', '').replace('"', '').strip() and str(x).replace(',', '').replace('"', '').strip() != '-' else None,
            'change_abs': lambda x: float(str(x).replace(',', '').replace('"', '')) if pd.notnull(x) and str(x).replace(',', '').replace('"', '').strip() and str(x).replace(',', '').replace('"', '').strip() != '-'  else None,
            'change_percent': lambda x: float(str(x).replace('%', '').replace(',', '').replace('"', '')) if pd.notnull(x) and str(x).replace(',', '').replace('"', '').strip() and str(x).replace(',', '').replace('"', '').strip() != '-'  else None,
            'order_matching_vol': lambda x: int(str(x).replace(',', '').replace('"', '')) if pd.notnull(x) and str(x).replace(',', '').replace('"', '').strip() and str(x).replace(',', '').replace('"', '').strip() != '-'  else None,
            'order_matching_val': lambda x: float(str(x).replace(',', '').replace('"', '')) if pd.notnull(x) and str(x).replace(',', '').replace('"', '').strip() and str(x).replace(',', '').replace('"', '').strip() != '-'  else None,
            'put_through_vol': lambda x: int(str(x).replace(',', '').replace('"', '')) if pd.notnull(x) and str(x).replace(',', '').replace('"', '').strip() and str(x).replace(',', '').replace('"', '').strip() != '-'  else None,
            'put_through_val': lambda x: float(str(x).replace(',', '').replace('"', '')) if pd.notnull(x) and str(x).replace(',', '').replace('"', '').strip() and str(x).replace(',', '').replace('"', '').strip() != '-'  else None,
            'total_transaction_vol': lambda x: int(str(x).replace(',', '').replace('"', '')) if pd.notnull(x) and str(x).replace(',', '').replace('"', '').strip() and str(x).replace(',', '').replace('"', '').strip() != '-'  else None,
            'total_transaction_val': lambda x: float(str(x).replace(',', '').replace('"', '')) if pd.notnull(x) and str(x).replace(',', '').replace('"', '').strip() and str(x).replace(',', '').replace('"', '').strip() != '-'  else None,
            'market_cap': lambda x: float(str(x).replace(',', '').replace('"', '')) if pd.notnull(x) and str(x).replace(',', '').replace('"', '').strip() and str(x).replace(',', '').replace('"', '').strip() != '-'  else None,
        },
        "dependent_tables": None
    },
    "crawler_news_latest": {
        "column_mapping": {
            'date': 'date',
            'title': 'title',
            'url': 'url',
            'content': 'content',
            'author': 'author',
            'publish_time': 'publish_time'
        },
        "drop_columns": None,
        "unique_columns": ['title', 'url'],
        "data_transforms": {
            'date': lambda x: pd.to_datetime(x,  format= '%d/%m/%Y', errors='coerce').date() if pd.notnull(x) else None,
            'publish_time': lambda x: pd.to_datetime(x, format='%H:%M %d/%m/%Y', errors='coerce') if pd.notnull(x) else None
        }
    },
    "crawler_news_stock":{
        "column_mapping" :{
            'symbol': 'stock',
            'date': 'date',
            'title': 'title',
            'url': 'url',
            'content': 'content',
            'author': 'author',
        },
        "drop_columns": ['publish_time'],
        "unique_columns": ['title', 'url'],
        "data_transforms": {
            'date': lambda x: pd.to_datetime(x,  format= '%d/%m/%Y', errors='coerce').date() if pd.notnull(x) else None,
        }
    }
}

def save_csv_to_postgres(csv_file, db_url, table_name, if_exists="replace"):
    """
    Lưu dữ liệu từ file CSV vào bảng PostgreSQL.

    Parameters:
    - csv_file (str): Đường dẫn đến file CSV.
    - db_url (str): URL kết nối PostgreSQL.
    - table_name (str): Tên bảng đích.
    - if_exists (str): "replace" hoặc "append".
    """
    # Kiểm tra xem table_name có trong TABLE_MAPPINGS không
    if table_name not in TABLE_MAPPINGS:
        logging.error(f"Không tìm thấy ánh xạ cho bảng {table_name}. Vui lòng thêm vào TABLE_MAPPINGS.")
        return

    # Lấy thông tin ánh xạ
    mapping_info = TABLE_MAPPINGS[table_name]
    column_mapping = mapping_info.get("column_mapping")
    drop_columns = mapping_info.get("drop_columns")
    unique_columns = mapping_info.get("unique_columns")
    data_transforms = mapping_info.get("data_transforms")
    dependent_tables = mapping_info.get("dependent_tables")

    # Đọc file CSV
    try:
        df = pd.read_csv(csv_file)
        logging.info(f"Đọc {len(df)} dòng từ CSV thành công.")
    except Exception as e:
        logging.error(f"Không thể đọc CSV file {csv_file}: {str(e)}")
        return
    
    logging.info(f"Các cột trong DataFrame: {df.columns.tolist()}")
    logging.info(f"Dòng đầu tiên: {df.iloc[0].to_dict()}")

    # Ánh xạ cột
    if column_mapping:
        df = df.rename(columns=column_mapping)
        logging.info(f"Các cột sau khi ánh xạ: {df.columns.tolist()}")

    # Xóa các cột không cần thiết
    if drop_columns:
        df = df.drop(columns=drop_columns, errors='ignore')

    # Xử lý dữ liệu tùy chỉnh
    try:
        if data_transforms:
            for column, transform in data_transforms.items():
                if column in df.columns:
                    try:
                        df[column] = df[column].apply(transform)
                        logging.info(f"Đã xử lý cột {column}")
                    except Exception as e:
                        logging.error(f"Lỗi khi xử lý cột {column}: {str(e)}")
                        return False
    except Exception as e:
        logging.error(f"Lỗi khi xử lý dữ liệu: {str(e)}")
        return False

    # Loại bỏ trùng lặp
    if unique_columns:
        df = df.drop_duplicates(subset=unique_columns)
        logging.info(f"Số dòng sau khi loại trùng: {len(df)}")

    # Kết nối tới PostgreSQL
    try:
        engine = create_engine(db_url, pool_size=10, max_overflow=20, pool_timeout=30, pool_recycle=1800)
        logging.info("Đã kết nối đến DB")
    except Exception as e:
        logging.error(f"Không thể kết nối tới cơ sở dữ liệu PostgreSQL: {str(e)}")
        return


    # Xóa dữ liệu cũ nếu if_exists="replace"

    if if_exists == "replace":
        try:
            with engine.connect() as conn:
                # Xóa dữ liệu trong các bảng con (nếu có)
                if dependent_tables:
                    for dep_table in dependent_tables:
                        conn.execute(text(f"TRUNCATE TABLE {dep_table} CASCADE"))
                        logging.info(f"Đã xóa dữ liệu trong bảng con {dep_table}.")

                # Xóa dữ liệu trong bảng chính
                conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
                conn.commit()
                logging.info(f"Đã xóa dữ liệu cũ trong bảng {table_name}.")
        except Exception as e:
            logging.error(f"Không thể TRUNCATE bảng {table_name}: {str(e)}")
            return


    """

    # Xóa dữ liệu cũ nếu if_exists="replace"
    if if_exists == "replace":
        try:
            with engine.begin() as conn:  # 🔧 SỬA
                # Xóa dữ liệu trong các bảng con (nếu có)
                if dependent_tables:
                    for dep_table in dependent_tables:
                        conn.execute(text(f"TRUNCATE TABLE {dep_table} CASCADE"))
                        logging.info(f"Đã xóa dữ liệu trong bảng con {dep_table}.")

                # Xóa dữ liệu trong bảng chính
                conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
                logging.info(f"Đã xóa dữ liệu cũ trong bảng {table_name}.")
        except Exception as e:
            logging.error(f"Không thể TRUNCATE bảng {table_name}: {str(e)}")
            return
    """

    # Ghi dữ liệu vào bảng
    try:
        df.to_sql(
            table_name,
            engine,
            index=False,
            if_exists="append",
            method="multi",
            chunksize=1000
        )
        logging.info(f"✅ Đã lưu dữ liệu {len(df)} dòng vào bảng {table_name}.")

        # Xóa file CSV sau khi lưu thành công
        if os.path.exists(csv_file):
            os.remove(csv_file)
            logging.info(f"Đã xóa file CSV: {csv_file}")
            logging.info("================================================================")
        else:
            logging.warning(f"Không tìm thấy file CSV để xóa: {csv_file}")

    except Exception as e:
        logging.error(f"Không thể lưu dữ liệu vào bảng {table_name}: {str(e)}")
        raise    


