import logging, time, random, re
from utils.pipeline.selenium_pipeline import SeleniumPipeline
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from collections import Counter
from utils.config.driver_config import create_driver
from scripts.vietstock_company import login_vietstock
from dotenv import load_dotenv
pipeline = SeleniumPipeline()
load_dotenv()


def vietstock_price(USERNAME, PASSWORD, exchange, last_date=None):
    exch = {
        "1": "HOSE",
        "2": "HNX",
        "3": "Upcom",
        "4": "VN30",
        "5": "HNX30",
        "otc": "OTC",
    }

    logging.info(f"Bắt đầu thu thập dữ liệu cho sàn {exch[exchange]}")
    driver, user_agent, proxy = create_driver(headless=True, use_proxy=False)
    login_vietstock(driver, USERNAME, PASSWORD)
    url = f"https://finance.vietstock.vn/ket-qua-giao-dich?exchange={exchange}"

    try:
        driver.get(url)
        time.sleep(5)  # chờ trang render
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div#statistic-price table.table tbody tr"))
        )
        logging.info(f"Đã tải trang: {url}")
    except Exception as e:
        logging.error(f"Lỗi khi tải trang: {e}")
        with open("debug_page.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        driver.quit()
        return

    try:
        # Chờ bảng được tải
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div#statistic-price table.table tbody tr"))
        )
        logging.info(f"Đã tải trang: {url}")

        # Chọn số dòng mỗi trang
        page_size = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.NAME, "limit"))
        )
        select = Select(page_size)
        select.select_by_value("30")
        logging.info("Đã chọn số dòng mỗi trang: 30")

        # Chờ bảng cập nhật sau khi đổi số dòng
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div#statistic-price table.table tbody tr"))
        )
        logging.info("Bảng đã cập nhật sau khi chọn số dòng")

    except Exception as e:
        logging.error(f"Không tìm thấy hoặc không click được tab: {exch[exchange]} – {e}")
        driver.quit()
        return

    # Bắt đầu crawl
    while True:
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div#statistic-price table.table tbody tr"))
            )
            rows = driver.find_elements(By.CSS_SELECTOR, "div#statistic-price table.table tbody tr")
            logging.info(f"Tìm thấy {len(rows)} dòng dữ liệu tại trang hiện tại")

        except Exception as e:
            logging.error(f"Không tìm thấy bảng dữ liệu: {e}")
            break

        for row in rows:
            try:
                columns = row.find_elements(By.CSS_SELECTOR, "td")
                if len(columns) < 18:
                    logging.warning(f"Dòng không đủ cột: {len(columns)} cột, bỏ qua")
                    continue

                data = {
                    "stt": columns[0].text,
                    "ngay": columns[1].text,
                    "exchange": exch[exchange],
                    "maCK": columns[2].text,
                    "tham_chieu": columns[3].text,
                    "mo_cua": columns[4].text,
                    "dong_cua": columns[5].text,
                    "cao_nhat": columns[6].text,
                    "thap_nhat": columns[7].text,
                    "trung_binh": columns[8].text,
                    "thay_doi_tang_giam": columns[9].text,
                    "thay_doi_phan_tram": columns[10].text,
                    "kl_gdkl": columns[11].text,
                    "gt_gdkl": columns[12].text,
                    "kl_gdtt": columns[13].text,
                    "gt_gdtt": columns[14].text,
                    "kl_tgd": columns[15].text,
                    "gt_tgd": columns[16].text,
                    "von_hoa": columns[17].text,
                }

                pipeline.process_item(data, f"vietstock_price_{exch[exchange]}_{columns[1].text}")
                # pipeline.process_item(data, f"vietstock_price_{exch[exchange]}")
                logging.info(f"Đã thu thập: {data['maCK']}-{exch[exchange]}- Ngày {data['ngay']}")

            except Exception as e:
                logging.error(f"Lỗi khi xử lý dòng: {e}")
                continue

        # Sang trang tiếp theo
        try:
            next_button = WebDriverWait(driver, 4).until(
                EC.presence_of_element_located((By.ID, "btn-page-next"))
            )
            if next_button.get_attribute("disabled"):
                logging.info("Đã đến trang cuối cùng! Dừng crawl.")
                break

            driver.execute_script("arguments[0].click();", next_button)
            logging.info("Đã chuyển sang trang tiếp theo")

            # Chờ bảng tải lại sau khi chuyển trang
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div#statistic-price table.table tbody tr"))
            )
            logging.info("Bảng đã tải lại sau khi chuyển trang")

        except Exception as e:
            logging.error(f"Không tìm thấy hoặc đã hết trang: {e}")
            break

    pipeline.close()
    driver.quit()
    logging.info(f"Hoàn thành thu thập dữ liệu cho sàn {exch[exchange]}!")



# --------------------------------------------------------------------
if __name__ == "__main__":
    from multiprocessing import Process, Pool
    from functools import partial
    import os
    import sys
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


    def crawl_price(exchange_code, USERNAME, PASSWORD):
        time.sleep(random.uniform(0.5, 1.5))
        vietstock_price(USERNAME, PASSWORD, exchange=exchange_code)

    exchanges_price = ["1", "2", "3"]
    with Pool(processes=2) as pool:
        pool.map(partial(crawl_price, USERNAME=random_username, PASSWORD=PASSWORD), exchanges_price)


#     #### 👉 Gộp file sau crawl
#     merge_csv_files(
#         pattern="/app/vietstock/crawled_data/vietstock_price_*_tmp.csv",
#         output_file="/app/vietstock/crawled_data/vst_price_final.csv"
#     )
#     logging.info("✅ Đã gộp các file *_tmp.csv thành vst_price_final.csv")



# ### save to db ###
#     db_url = "postgresql://postgres:652003@host.docker.internal:5432/vnstock"
#     csv_file = "/app/vietstock/crawled_data/vst_price_final.csv"
#     table_name = "crawler_price"

#     # Lưu dữ liệu vào PostgreSQL
#     save_csv_to_postgres(csv_file=csv_file, db_url=db_url, table_name=table_name, if_exists="append")

#     # save_csv_to_postgres(csv_file=csv_file, db_url=db_url, table_name=table_name, if_exists="append") # chỉnh thành append để ghi vào kh bị xóa cái cũ 
