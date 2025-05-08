import sys, os, time, random, aiohttp, aiohttp_retry, asyncio, pandas as pd, logging
sys.path.append('/app')
from bs4 import BeautifulSoup
from aiohttp_socks import ProxyConnector
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from sqlalchemy import create_engine
from utils.config.driver_config import create_driver
from utils.pipeline.selenium_pipeline import save_csv_to_postgres, merge_csv_files
from scripts.vietstock_news_latest import fetch_all_article_contents

from utils.pipeline.selenium_pipeline import SeleniumPipeline
pipeline = SeleniumPipeline()


# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    filename='/app/vietstock/crawled_data/crawl.log',
    filemode='a',  # Chế độ append
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

def process_news(symbol, news_rows):
    news_data = []
    for row in news_rows:
        try:
            date_elem = row.select_one("td.col-date")
            title_elem = row.select_one("a.text-link.news-link")
            if not title_elem or not date_elem:
                continue

            title = title_elem.get_text(strip=True)
            href = title_elem["href"]
            url = "https:" + href if href.startswith("//") else href
            date = date_elem.get_text(strip=True)

            news_data.append({
                "symbol": symbol,
                "title": title,
                "url": url,
                "date": date,
                "content": "",
            })
        except Exception as e:
            print(f"⚠️ Lỗi khi phân tích dòng tin tức: {e}")
            logging.info(f"⚠️ Lỗi khi phân tích dòng tin tức: {e}")
            continue
    return news_data

def vietstock_news_symbol():
    start_time = time.time()

    #
    db_url = os.getenv('DB_URL')
    # kết nối đến db lấy danh sách mã ck
    try:
        engine = create_engine(db_url, pool_size=10, max_overflow=20, pool_timeout=30, pool_recycle=1800)
        query = "SELECT symbol FROM crawler_company"  # Giả sử cột chứa mã chứng khoán là 'symbol'
        df = pd.read_sql(query, engine)
        symbols = df['symbol'].tolist()
        logging.info(f"Lấy được {len(symbols)} mã chứng khoán từ bảng company")
    except Exception as e:
        logging.error(f"Không thể lấy mã chứng khoán từ database: {str(e)}")
        return

    driver, user_agent, proxy = create_driver(headless=True, use_proxy=False)

    for symbol in symbols:
        glo_symbol = symbol
        base_url = f"https://finance.vietstock.vn/{symbol}/tin-moi-nhat.htm"
        driver.get(base_url)
        time.sleep(random.uniform(3, 5))
        all_news_data = []

        while True:
            soup = BeautifulSoup(driver.page_source, "html.parser")
            try:
                news_table = soup.find("div", id="latest-news").find("table")
                news_rows = news_table.find("tbody").find_all("tr")
                print(f"[{symbol}] Tìm thấy {len(news_rows)} bài viết")
                logging.info(f"[{symbol}] Tìm thấy {len(news_rows)} bài viết")
            except Exception as e:
                print(f"[{symbol}] Không tìm thấy bảng tin tức: {e}")
                logging.info(f"[{symbol}] Không tìm thấy bảng tin tức: {e}")
                break

            news_data = process_news(symbol, news_rows)
            all_news_data.extend(news_data)

            try:
                next_li = driver.find_element(By.CSS_SELECTOR, 'li[id*="new-page-next"]')
                if "disabled" in next_li.get_attribute("class"):
                    print(f"[{symbol}] ✅ Hết trang.")
                    logging.info(f"[{symbol}] ✅ Hết trang.")
                    break
                next_button = next_li.find_element(By.TAG_NAME, "a")
                driver.execute_script("arguments[0].click();", next_button)
                time.sleep(random.uniform(4, 6))
            except NoSuchElementException:
                print(f"[{symbol}] ❌ Không tìm thấy nút Next hoặc đã hết trang.")
                logging.info(f"[{symbol}] ❌ Không tìm thấy nút Next hoặc đã hết trang.")
                break

        if all_news_data:
            all_news_data = asyncio.run(fetch_all_article_contents(all_news_data, user_agent, proxy, max_concurrent=3))

        for item in all_news_data:
            pipeline.process_item(item, source=f"vst_news_{symbol}")
        pipeline.save_data(temp=True)

    driver.quit()
    print(f"⏳ Hoàn thành trong {time.time() - start_time:.2f} giây")
    logging.info(f"⏳ Hoàn thành trong {time.time() - start_time:.2f} giây")

if __name__ == "__main__":
    vietstock_news_symbol()


    ### 👉 Gộp file sau crawl
    merge_csv_files(
        pattern="/app/vietstock/crawled_data/vst_news_*_tmp.csv",
        output_file="/app/vietstock/crawled_data/vst_news_stock_final.csv"
    )

    logging.info("✅ Đã gộp các file *_tmp.csv thành vst_news_stock_final.csv")

    ### save to db ###
    db_url = os.getenv("DB_URL")
    csv_file = "/app/vietstock/crawled_data/vst_news_stock_final.csv"
    table_name = "crawler_news_stock"

    # Lưu dữ liệu vào PostgreSQL
    save_csv_to_postgres(csv_file=csv_file, db_url=db_url, table_name=table_name, if_exists="append")
