import sys, os, re
sys.path.append('/app')

import logging
from utils.config.driver_config import create_driver
from utils.pipeline.selenium_pipeline import SeleniumPipeline
from utils.pipeline.selenium_pipeline import merge_csv_files, save_csv_to_postgres
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from collections import Counter
import time
import random
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    filename='/app/vietstock/crawled_data/crawl.log',
    filemode='a',  # Chế độ append
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# Khởi tạo pipeline
pipeline = SeleniumPipeline()

def login_vietstock(driver, USERNAME, PASSWORD):
    url = "https://finance.vietstock.vn/"
    driver.get(url)
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div#login-form div.tab-login"))
        )
        username_field = driver.find_element(By.CSS_SELECTOR, "div.input-group input#txtEmailLogin")
        password_field = driver.find_element(By.CSS_SELECTOR, "div.input-group input#passwordLogin")
        username_field.send_keys(USERNAME)
        password_field.send_keys(PASSWORD)
        login_button = driver.find_element(By.ID, "btnLoginAccount")
        login_button.click()
        time.sleep(3)
        if "logout" in driver.page_source.lower():
            print("Đăng nhập thành công!")
            logging.info(f"Đăng nhập thành công! bằng | {USERNAME}")
            return True
        else:
            logging.error("Đăng nhập thất bại! Kiểm tra lại email/mật khẩu.")
            print("Đăng nhập thất bại! Kiểm tra lại email/mật khẩu.")
            return False
    except Exception as e:
        logging.error(f"Không tìm thấy login: {e}")
        print(f"Không tìm thấy login: {e}")
        return False

def vietstock_company(USERNAME, PASSWORD, exchange="0"):
    driver, user_agent, proxy = create_driver(headless=True, use_proxy=False)
    if not login_vietstock(driver, USERNAME, PASSWORD):
        driver.quit()
        return

    url = "https://finance.vietstock.vn/doanh-nghiep-a-z?page=1"
    driver.get(url)
    EXCHANGE_MAP = {
        "0": "ALL",
        "1": "HOSE",
        "2": "HNX",
        "5": "UpCom",
        "3": "OTC",
        "4": "Khác"
    }

    try:
        if exchange != "0":
            select_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "exchange"))
            )
            select = Select(select_element)
            select.select_by_value(str(exchange))

        page_size = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "pageSize"))
        )
        select = Select(page_size)
        select.select_by_value("50")
        logging.info("đã chọn số lượng 50 dòng")

        try:
            search_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.al-top"))
            )
            search_btn.click()
            logging.info("✅ đã nhấn tìm kiếm")
        except Exception as e:
            logging.error(f"lỗi nhấn tìm kiếm {e}")

        print(f"✅ Đã chọn sàn {EXCHANGE_MAP[exchange]} và nhấn tìm kiếm.")
        logging.info(f"✅ Đã chọn sàn {EXCHANGE_MAP[exchange]} và nhấn tìm kiếm.")
        time.sleep(5)
    except Exception as e:
        print(f"❌ Không thể chọn sàn hoặc nhấn tìm kiếm: {e}")

    while True:
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-striped tbody"))
            )
        except Exception as e:
            print(f"Lỗi: {e} Không tìm thấy bảng dữ liệu")
            break

        rows = driver.find_elements(By.CSS_SELECTOR, "table.table-striped tbody tr")
        for row in rows:
            try:
                columns = row.find_elements(By.CSS_SELECTOR, "td")
                stt = columns[0].text
                CK_id = columns[1].text
                company = columns[2].text
                branch = columns[3].text
                stock_exchange = columns[4].text
                listed_volume = columns[5].text

                if stock_exchange.strip().upper() != EXCHANGE_MAP[exchange].upper():
                    print(f"⚠️ Bỏ qua mã {CK_id} vì không thuộc sàn {EXCHANGE_MAP[exchange]}")
                    logging.info(f"⚠️ Bỏ qua mã {CK_id} vì không thuộc sàn {EXCHANGE_MAP[exchange]}")
                    continue

                pipeline.process_item({
                    "stt": stt,
                    "CK_id": CK_id,
                    "company": company,
                    "branch": branch,
                    "exchange": EXCHANGE_MAP[exchange],
                    "listed_volume": listed_volume,
                }, f"vietstock_company_{EXCHANGE_MAP[exchange]}")
            except Exception as e:
                print(f"Lỗi: {e}")
                continue

        try:
            next_button = driver.find_element(By.ID, "btn-page-next")
            if next_button.get_attribute("disabled"):
                print("✅ Đã đến trang cuối cùng! Dừng crawl.")
                logging.info("✅ Đã đến trang cuối cùng! Dừng crawl.")
                break
            driver.execute_script("arguments[0].click();", next_button)
            time.sleep(random.uniform(3, 7))
        except:
            print("Không tìm thấy hoặc đã hết trang!")
            break

    pipeline.save_data(temp=True)
    driver.quit()
# --------------------------------------------------------------------
if __name__ == "__main__":
    import os
    import sys
    from multiprocessing import Process, Pool
    from functools import partial
    secret_key = os.getenv('SECRET_KEY')
    DEBUG= os.getenv('DEBUG')
    PASSWORD = os.getenv('PASSWORD')

    ### random lây tai khoan
    # Lấy các USERNAME có số
    usernames = [value.strip() for key, value in os.environ.items() if re.match(r'USERNAME\d+', key)]

    # Biến Counter lưu số lần sử dụng tài khoản
    username_usage = Counter()

    # Hàm random tài khoản
    def get_random_username():
        # Lọc tài khoản chưa đủ 2 lần chọn
        available_usernames = [username for username in usernames if username_usage[username] < 2]
        
        if not available_usernames:
            return None  # Không còn tài khoản để chọn
        
        random_username = random.choice(available_usernames)
        username_usage[random_username] += 1
        return random_username

    random_username = get_random_username()


    # Lấy exchange từ đối số dòng lệnh, mặc định "0" nếu không có
    # exchange = sys.argv[1] if len(sys.argv) > 1 else "0"
    # vietstock_company(random_username, PASSWORD, exchange)

    def craw_company(exchange_code, USERNAME, PASSWORD):
        time.sleep(random.uniform(0.5, 1.5))
        vietstock_company(USERNAME, PASSWORD, exchange=exchange_code)
    exchanges_company = ["1", "2", "5"]
    with Pool(processes=2) as pool:
        pool.map(partial(craw_company, USERNAME=random_username, PASSWORD=PASSWORD), exchanges_company)


#### 👉 Gộp file sau crawl
    merge_csv_files(
        pattern="/app/vietstock/crawled_data/vietstock_company_*_tmp.csv",
        output_file="/app/vietstock/crawled_data/vst_company_final.csv"
    )

    logging.info("✅ Đã gộp các file *_tmp.csv thành vst_company_final.csv")


### save to db ###
    # db_url = "postgresql+psycopg2://postgres:652003@localhost:5432/vnstock"
    # db_url = "postgresql+psycopg2://postgres:652003@host.docker.internal:5432/vnstock"
    # db_url = "postgresql://postgres:652003@localhost:5432/vnstock"
    db_url = os.getenv('DB_URL') 
    csv_file = "/app/vietstock/crawled_data/vst_company_final.csv"
    table_name = "crawler_company"

    # Lưu dữ liệu vào PostgreSQL
    save_csv_to_postgres(csv_file=csv_file, db_url=db_url, table_name=table_name, if_exists="replace")




