import json, csv, os, logging, glob, time
from datetime import datetime
from multiprocessing import Process, Manager
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    filename='/app/vietstock/crawled_data/crawl.log',
    filemode='a',  # Chế độ append
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

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
            'listed_volume': lambda x: int(str(x).replace('"', '').replace(',', '')) if pd.notnull(x) and str(x).strip() and str(x).strip() != '-' else None
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
            'date': lambda x:  pd.to_datetime(x, errors='coerce').date()
            if pd.notnull(x) and str(x).strip() else datetime.now().date()
        }
    }
}


class SeleniumPipeline:
    """ Pipeline để xử lý và lưu dữ liệu từ Selenium """

    def __init__(self, output_folder= f"vietstock/crawled_data", batch_size=100):
        self.output_folder = output_folder
        os.makedirs(self.output_folder, exist_ok=True)  # Tạo thư mục nếu chưa có
        self.data = {}  # Dictionary chứa dữ liệu theo từng bảng
        self._seen_keys = {}  # NEW: giữ các key duy nhất đã gặp
        self.batch_size = batch_size  # THÊM: Biến batch_size để lưu dữ liệu theo lô
        self.db_url = os.getenv("DB_URL", "postgresql://postgres:652003@db:5432/vnstock")  # THÊM: URL kết nối database
        self.engine = create_engine(self.db_url, pool_size=10, max_overflow=20, pool_timeout=30, pool_recycle=1800)  # THÊM: Kết nối database
        self.cleared_exchanges = set()  # THÊM: Theo dõi các sàn đã xóa dữ liệu
    
    def get_unique_key(self, item, source):
        # Tuỳ theo bảng mà tạo key khác nhau

        if source.startswith("vietstock_company_"):
            return item.get('CK_id', '').strip()
        elif source.startswith("vietstock_price_"):
            return f"{item.get('maCK', '').strip()}|{item.get('ngay', '').strip()}"
        elif source == "crawler_news_stock": #vietstock news symbol
            return f"{item.get('title', '').strip()}|{item.get('url', '').strip()}"
        elif source == "crawler_news_latest": #vietstock News_Latest
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

        # THÊM: Lưu theo batch khi đạt batch_size
        if len(self.data[source]) >= self.batch_size:
            self._save_to_db(source)

    """
    def process_crawler_news_stock(self, item):
    # Chuẩn hóa ngày
        try:
            date_obj = datetime.strptime(item.get("date", ""), "%d/%m/%Y")
            date_str = date_obj.strftime("%Y-%m-%d")
        except:
            date_str = item.get("date", "").strip()

        return {
            "symbol": item.get("symbol", "").strip(),
            "date": date_str,
            "title": item.get("title", "").strip(),
            "url": item.get("url", "").strip(),
            "content": (item.get("content") or "").strip(),
            "author": (item.get("author") or "").strip(),
            "publish_time": (item.get("publish_time") or "").strip()  # Đảm bảo khớp với column_mapping
        }
    """    
    def process_crawler_news_stock(self, item):
        date_str = item.get("date", "").strip()
        if date_str:
            try:
                date_obj = datetime.strptime(date_str, "%d/%m/%Y")
                date_str = date_obj.strftime("%Y-%m-%d")
            except:
                try:
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                    date_str = date_obj.strftime("%Y-%m-%d")
                except:
                    print(f"⚠️ Không thể phân tích ngày {date_str} cho bài viết {item.get('url', '')}, gán ngày hiện tại")
                    logging.info(f"⚠️ Không thể phân tích ngày {date_str} cho bài viết {item.get('url', '')}, gán ngày hiện tại")
                    date_str = time.strftime("%Y-%m-%d")
        else:
            print(f"⚠️ Trường date trống cho bài viết {item.get('url', '')}, gán ngày hiện tại")
            logging.info(f"⚠️ Trường date trống cho bài viết {item.get('url', '')}, gán ngày hiện tại")
            date_str = time.strftime("%Y-%m-%d")

        return {
            "symbol": item.get("symbol", "").strip(),
            "date": date_str,  # CHẮC CHẮN KHÔNG NULL
            "title": item.get("title", "").strip(),
            "url": item.get("url", "").strip(),
            "content": (item.get("content") or "").strip(),
            "author": (item.get("author") or "").strip(),
            "publish_time": (item.get("publish_time") or "").strip()
        }

    def process_vietstock_news_latest(self, item):
        try:
            date_obj = datetime.strptime(item.get("date", ""), "%d/%m/%Y")
            date_str = date_obj.strftime("%Y-%m-%d")
        except:
            date_str = item.get("date", "").strip()

        return {
            "date": date_str,
            "title": item.get("title", "").strip(),
            "content": (item.get("content") or "").strip(),
            "author": (item.get("author") or "").strip(),
            "publish_time": (item.get("publish_time") or "").strip(),
            "url": item.get("url", "").strip(),
        }

    def default_process(self, item):
        """ Xử lý mặc định nếu chưa có hàm riêng """
        return item

    """
    def _save_to_db(self, source):
        if source not in self.data or not self.data[source]:
            return

        table_name = self._map_source_to_table(source)
        if table_name not in TABLE_MAPPINGS:
            logging.error(f"Không tìm thấy ánh xạ cho nguồn {source}")
            return

        mapping_info = TABLE_MAPPINGS[table_name]
        column_mapping = mapping_info.get("column_mapping", {})
        drop_columns = mapping_info.get("drop_columns", [])
        unique_columns = mapping_info.get("unique_columns", [])
        data_transforms = mapping_info.get("data_transforms", {})
        dependent_tables = mapping_info.get("dependent_tables", [])

        df = pd.DataFrame(self.data[source])

        if column_mapping:
            df = df.rename(columns=column_mapping)

        if drop_columns:
            df = df.drop(columns=drop_columns, errors='ignore')

        for column, transform in data_transforms.items():
            if column in df.columns:
                df[column] = df[column].apply(transform)

        if unique_columns:
            initial_rows = len(df)
            df = df.drop_duplicates(subset=unique_columns, keep='first')
            logging.info(f"Loại bỏ {initial_rows - len(df)} dòng trùng lặp dựa trên {unique_columns}")

        current_exchange = source.replace("vietstock_company_", "") if source.startswith("vietstock_company_") else None

        with self.engine.connect() as conn:
            if table_name == "crawler_company" and current_exchange and current_exchange not in self.cleared_exchanges:
                conn.execute(text(f"DELETE FROM {table_name} WHERE exchange = :exchange"), {"exchange": current_exchange})
                logging.info(f"Đã xóa dữ liệu cũ của sàn {current_exchange} trong bảng {table_name}")
                self.cleared_exchanges.add(current_exchange)
            conn.commit()

            df.to_sql(
                table_name,
                self.engine,
                index=False,
                if_exists="append",
                method="multi",
                chunksize=1000
            )
            logging.info(f"💾 Đã lưu {len(df)} dòng vào bảng {table_name} cho {current_exchange or 'nguồn'}")

        self.data[source] = []
        self._seen_keys[source].clear()
    """

    def _save_to_db(self, source):
        if source not in self.data or not self.data[source]:
            logging.info(f"Không có dữ liệu để lưu cho source {source}")
            return

        table_name = self._map_source_to_table(source)
        if table_name not in TABLE_MAPPINGS:
            logging.error(f"Không tìm thấy ánh xạ cho nguồn {source}")
            return

        mapping_info = TABLE_MAPPINGS[table_name]
        column_mapping = mapping_info.get("column_mapping", {})
        drop_columns = mapping_info.get("drop_columns", [])
        unique_columns = mapping_info.get("unique_columns", [])
        data_transforms = mapping_info.get("data_transforms", {})
        dependent_tables = mapping_info.get("dependent_tables", [])

        df = pd.DataFrame(self.data[source])
        logging.info(f"Dữ liệu ban đầu cho {table_name}: {len(df)} dòng")

        if column_mapping:
            df = df.rename(columns=column_mapping)

        if drop_columns:
            df = df.drop(columns=drop_columns, errors='ignore')

        for column, transform in data_transforms.items():
            if column in df.columns:
                df[column] = df[column].apply(transform)

        if unique_columns:
            # Loại trùng lặp trong batch hiện tại
            initial_rows = len(df)
            df = df.drop_duplicates(subset=unique_columns, keep='first')
            logging.info(f"Loại bỏ {initial_rows - len(df)} dòng trùng lặp trong batch hiện tại dựa trên {unique_columns}")

        if df.empty:
            logging.info(f"Không có dữ liệu mới để lưu vào bảng {table_name} sau khi loại trùng lặp trong batch")
            self.data[source] = []
            self._seen_keys[source].clear()
            return

        # Chuẩn hóa dữ liệu trước khi kiểm tra
        for col in unique_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        # Kiểm tra trùng lặp với database
        with self.engine.connect() as conn:
            existing_data = pd.read_sql(
                f"SELECT {', '.join(unique_columns)} FROM {table_name}",
                conn
            )
            if not existing_data.empty:
                # Chuẩn hóa dữ liệu trong existing_data
                for col in unique_columns:
                    if col in existing_data.columns:
                        existing_data[col] = existing_data[col].astype(str).str.strip()

                initial_rows = len(df)
                df = df.merge(
                    existing_data,
                    on=unique_columns,
                    how='left',
                    indicator=True
                )
                # Lọc các bản ghi chỉ có trong dữ liệu mới (không trùng với database)
                new_records = df[df['_merge'] == 'left_only'].drop(columns=['_merge'])
                duplicate_records = df[df['_merge'] == 'both'].drop(columns=['_merge'])

                # Log các bản ghi trùng lặp với từ "double"
                if not duplicate_records.empty:
                    for _, row in duplicate_records.iterrows():
                        logging.info(f"double: {dict(row)} - Bỏ qua vì đã tồn tại trong database")

                df = new_records
                logging.info(f"Loại bỏ {initial_rows - len(df)} dòng trùng lặp với dữ liệu trong database dựa trên {unique_columns}")

        if df.empty:
            logging.info(f"Không còn dữ liệu mới để lưu vào {table_name} sau khi kiểm tra trùng lặp với database")
            self.data[source] = []
            self._seen_keys[source].clear()
            return

        # Log dữ liệu sẽ lưu
        logging.info(f"Dữ liệu sẽ lưu vào {table_name}: {df.to_dict('records')}")

        current_exchange = source.replace("vietstock_company_", "") if source.startswith("vietstock_company_") else None

        # Retry và xử lý lỗi
        MAX_RETRIES = 3
        RETRY_DELAY = 5
        retry_count = 0
        while retry_count < MAX_RETRIES:
            try:
                with self.engine.connect() as conn:
                    # if table_name == "crawler_company" and current_exchange and current_exchange not in self.cleared_exchanges:
                    #     conn.execute(text(f"DELETE FROM {table_name} WHERE exchange = :exchange"), {"exchange": current_exchange})
                    #     logging.info(f"Đã xóa dữ liệu cũ của sàn {current_exchange} trong bảng {table_name}")
                    #     self.cleared_exchanges.add(current_exchange)
                    if table_name == "crawler_company":
                        with self.engine.begin() as conn:
                            for _, row in df.iterrows():
                                row_dict = row.to_dict()
                                insert_stmt = text("""
                                    INSERT INTO crawler_company (
                                        symbol, exchange, company_name, industry, listed_volume
                                    ) VALUES (
                                        :symbol, :exchange, :company_name, :industry, :listed_volume
                                    )
                                    ON CONFLICT (symbol, exchange)
                                    DO UPDATE SET
                                        company_name = EXCLUDED.company_name,
                                        industry = EXCLUDED.industry,
                                        listed_volume = EXCLUDED.listed_volume
                                """)
                                conn.execute(insert_stmt, row_dict)
                        logging.info(f"💾 Đã upsert {len(df)} dòng vào bảng {table_name} theo exchange = {current_exchange}")

                    conn.commit()

                    if table_name == "crawler_company":
                        with self.engine.begin() as conn:
                            for _, row in df.iterrows():
                                row_dict = row.to_dict()
                                insert_stmt = text("""
                                    INSERT INTO crawler_company (
                                        symbol, exchange, company_name, industry, listed_volume
                                    ) VALUES (
                                        :symbol, :exchange, :company_name, :industry, :listed_volume
                                    )
                                    ON CONFLICT (symbol, exchange)
                                    DO UPDATE SET
                                        company_name = EXCLUDED.company_name,
                                        industry = EXCLUDED.industry,
                                        listed_volume = EXCLUDED.listed_volume
                                """)
                                conn.execute(insert_stmt, row_dict)
                        logging.info(f"💾 Đã upsert {len(df)} dòng vào bảng {table_name} theo exchange = {current_exchange}")
                    else:
                        df.to_sql(
                            table_name,
                            self.engine,
                            index=False,
                            if_exists="append",
                            method="multi",
                            chunksize=1000
                        )
                    logging.info(f"💾 Đã lưu {len(df)} dòng vào bảng {table_name} cho {current_exchange or 'nguồn'}")
                    break
            except Exception as e:
                retry_count += 1
                if retry_count == MAX_RETRIES:
                    logging.error(f"❌ Lỗi lưu dữ liệu vào bảng {table_name} sau {MAX_RETRIES} lần thử: {e}")
                    raise
                if "IntegrityError" in str(e) and "duplicate key value violates unique constraint" in str(e):
                    logging.warning(f"⚠️ Lỗi IntegrityError (lần {retry_count}/{MAX_RETRIES}): {e}. Loại bỏ các bản ghi trùng lặp...")
                    initial_rows = len(df)
                    df = df.drop_duplicates(subset=unique_columns, keep=False)
                    logging.info(f"Loại bỏ {initial_rows - len(df)} dòng trùng lặp sau IntegrityError")
                    if df.empty:
                        logging.info(f"Không còn dữ liệu mới để lưu vào {table_name} sau khi xử lý IntegrityError")
                        self.data[source] = []
                        self._seen_keys[source].clear()
                        return
                    continue
                logging.warning(f"⚠️ Lỗi lưu dữ liệu vào bảng {table_name} (lần {retry_count}/{MAX_RETRIES}): {e}. Thử lại sau {RETRY_DELAY} giây...")
                time.sleep(RETRY_DELAY)

        self.data[source] = []
        self._seen_keys[source].clear()

    # THÊM: Phương thức ánh xạ source với tên bảng
    def _map_source_to_table(self, source):
        if source.startswith("vietstock_company_"):
            return "crawler_company"
        elif source.startswith("vietstock_price_"):
            return "crawler_price"
        elif source == "crawler_news_latest":
            return "crawler_news_latest"
        elif source == "crawler_news_stock":
            return "crawler_news_stock"
        return source

    # THÊM: Phương thức close để lưu dữ liệu còn lại
    def close(self):
        for source in self.data.keys():
            if self.data[source]:
                self._save_to_db(source)
        logging.info("💾 Đã lưu tất cả dữ liệu còn lại vào database")
        self.cleared_exchanges.clear()

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


##############################################################################
"""
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

def save_csv_to_postgres(csv_file, db_url, table_name, if_exists="append"):

    # Lưu dữ liệu từ file CSV vào bảng PostgreSQL.

    # Parameters:
    # - csv_file (str): Đường dẫn đến file CSV.
    # - db_url (str): URL kết nối PostgreSQL.
    # - table_name (str): Tên bảng đích.
    # - if_exists (str): "replace" hoặc "append".

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
"""
