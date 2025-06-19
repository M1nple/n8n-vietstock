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

"""
# Hàm lấy nội dung bài viết bất đồng bộ với độ trễ
async def get_article_content(session, url, user_agent, max_retries=5):
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://vietstock.vn/"
    }
    
    retry_options = aiohttp_retry.ExponentialRetry(attempts=max_retries, exceptions={aiohttp.ClientError})
    retry_client = aiohttp_retry.RetryClient(client_session=session, retry_options=retry_options)

    try:
        async with retry_client.get(url, timeout=15, headers=headers) as response:
            if response.status == 200:
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")

                article_body = soup.select_one('div[itemprop="articleBody"][id="vst_detail"]')
                if not article_body:
                    print(f"❌ Không tìm thấy nội dung tại {url}")
                    logging.info(f"❌ Không tìm thấy nội dung tại {url}")
                    return None

                content_parts = []
                for element in article_body.find_all(['p', 'div']):
                    if element.name == 'p' and element.get('class') in [['pTitle'], ['pHead'], ['pBody']]:
                        text = element.get_text(strip=True)
                        if text:
                            content_parts.append(text)
                    elif element.name == 'div' and element.get('class') == ['pCaption']:
                        text = element.get_text(strip=True)
                        if text:
                            content_parts.append(f"[Chú thích ảnh]: {text}")

                content = "\n".join(content_parts) if content_parts else "Không tìm thấy nội dung"

                author = article_body.select_one('p.pAuthor a')
                # publish_time = article_body.select_one('p.pPublishTimeSource')  # lấy 1 
                publish_time = article_body.select('p.pPublishTimeSource') # lấy tất cả 

                result = {
                    "content": content,
                    "author": author.get_text(strip=True) if author else "",
                    "publish_time": publish_time.get_text(strip=True).replace('- ', '') if publish_time else ""
                }

                print(f"✅ Đã lấy nội dung bài viết: {url}")
                logging.info(f"✅ Đã lấy nội dung bài viết: {url}")
                await asyncio.sleep(random.uniform(1, 2))
                return result
            else:
                print(f"❌ Lỗi HTTP {response.status} tại {url}")
                logging.info(f"❌ Lỗi HTTP {response.status} tại {url}")
                return None
    except Exception as e:
        print(f"❌ Lỗi lấy nội dung từ {url}: {e}")
        logging.info(f"❌ Lỗi lấy nội dung từ {url}: {e}")
        return None

async def fetch_all_article_contents(articles_data, user_agent, proxy=None, max_concurrent=3):
    print("Đang chạy fetch_all_article_contents với max_concurrent =", max_concurrent)
    logging.info("Đang chạy fetch_all_article_contents với max_concurrent =", max_concurrent)
    connector = ProxyConnector.from_url(f"http://{proxy}") if proxy else None
    async with aiohttp.ClientSession(connector=connector) as session:
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def limited_get_article_content(article):
            async with semaphore:
                result = await get_article_content(session, article["url"], user_agent)
                if result:
                    article["content"] = result["content"]
                    article["author"] = result["author"]
                    article["publish_time"] = result["publish_time"]
                else:
                    article["content"] = "Lỗi khi lấy nội dung"
                    article["author"] = ""
                    article["publish_time"] = ""
        
        tasks = [limited_get_article_content(article) for article in articles_data]
        await asyncio.gather(*tasks, return_exceptions=True)
    return articles_data

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
                "author": "",  
                "publish_time": ""  
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
            pipeline.process_item(item, source=f"crawler_news_stock")
        pipeline.close()

    driver.quit()
    print(f"⏳ Hoàn thành trong {time.time() - start_time:.2f} giây")
    logging.info(f"⏳ Hoàn thành trong {time.time() - start_time:.2f} giây")

if __name__ == "__main__":
    vietstock_news_symbol()

"""

# Hàm lấy nội dung bài viết bất đồng bộ với độ trễ
async def get_article_content(session, url, user_agent, max_retries=5):
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://vietstock.vn/"
    }
    
    retry_options = aiohttp_retry.ExponentialRetry(attempts=max_retries, exceptions={aiohttp.ClientError})
    retry_client = aiohttp_retry.RetryClient(client_session=session, retry_options=retry_options)

    try:
        async with retry_client.get(url, timeout=15, headers=headers) as response:
            if response.status == 200:
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")

                article_body = soup.select_one('div[itemprop="articleBody"][id="vst_detail"]')
                if not article_body:
                    print(f"❌ Không tìm thấy nội dung tại {url}")
                    logging.info(f"❌ Không tìm thấy nội dung tại {url}")
                    return None

                content_parts = []
                for element in article_body.find_all(['p', 'div']):
                    if element.name == 'p' and element.get('class') in [['pTitle'], ['pHead'], ['pBody']]:
                        text = element.get_text(strip=True)
                        if text:
                            content_parts.append(text)
                    elif element.name == 'div' and element.get('class') == ['pCaption']:
                        text = element.get_text(strip=True)
                        if text:
                            content_parts.append(f"[Chú thích ảnh]: {text}")

                content = "\n".join(content_parts) if content_parts else "Không tìm thấy nội dung"

                author = article_body.select_one('p.pAuthor a')
                publish_time_elements = article_body.select('p.pPublishTimeSource')  # Lấy tất cả phần tử

                # Xử lý publish_time từ danh sách phần tử
                publish_time = ""
                if publish_time_elements:
                    # Lấy phần tử đầu tiên và làm sạch dữ liệu
                    publish_time = publish_time_elements[0].get_text(strip=True).replace('- ', '')

                result = {
                    "content": content,
                    "author": author.get_text(strip=True) if author else "",
                    "publish_time": publish_time
                }

                print(f"✅ Đã lấy nội dung bài viết: {url}")
                logging.info(f"✅ Đã lấy nội dung bài viết: {url}")
                await asyncio.sleep(random.uniform(1, 2))
                return result
            else:
                print(f"❌ Lỗi HTTP {response.status} tại {url}")
                logging.info(f"❌ Lỗi HTTP {response.status} tại {url}")
                return None
    except Exception as e:
        print(f"❌ Lỗi lấy nội dung từ {url}: {e}")
        logging.info(f"❌ Lỗi lấy nội dung từ {url}: {e}")
        return None

async def fetch_all_article_contents(articles_data, user_agent, proxy=None, max_concurrent=3):
    print("Đang chạy fetch_all_article_contents với max_concurrent =", max_concurrent)
    logging.info("Đang chạy fetch_all_article_contents với max_concurrent =", max_concurrent)
    connector = ProxyConnector.from_url(f"http://{proxy}") if proxy else None
    async with aiohttp.ClientSession(connector=connector) as session:
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def limited_get_article_content(article):
            async with semaphore:
                result = await get_article_content(session, article["url"], user_agent)
                if result:
                    article["content"] = result["content"]
                    article["author"] = result["author"]
                    article["publish_time"] = result["publish_time"]
                    # SỬA: Cập nhật date nếu trống, sử dụng publish_time hoặc ngày hiện tại
                    if not article.get("date") or article["date"] is None or article["date"] == "":
                        article["date"] = result["publish_time"] or time.strftime("%Y-%m-%d")
                else:
                    article["content"] = "Lỗi khi lấy nội dung"
                    article["author"] = ""
                    article["publish_time"] = ""
                    # SỬA: Gán date mặc định nếu None
                    if not article.get("date") or article["date"] is None or article["date"] == "":
                        article["date"] = time.strftime("%Y-%m-%d")
        
        tasks = [limited_get_article_content(article) for article in articles_data]
        await asyncio.gather(*tasks, return_exceptions=True)
    return articles_data

def process_news(symbol, news_rows):
    news_data = []
    from datetime import datetime  # SỬA: Thêm import datetime
    for row in news_rows:
        try:
            date_elem = row.select_one("td.col-date")
            title_elem = row.select_one("a.text-link.news-link")
            if not title_elem or not date_elem:
                continue

            title = title_elem.get_text(strip=True)
            href = title_elem["href"]
            url = "https:" + href if href.startswith("//") else href
            date_str = date_elem.get_text(strip=True)
            # SỬA: Chuyển đổi date_str thành định dạng hợp lệ (ví dụ: dd/mm/yyyy thành yyyy-mm-dd)
            try:
                date = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
            except ValueError:
                print(f"⚠️ Không thể phân tích ngày {date_str}, sử dụng ngày hiện tại")
                logging.info(f"⚠️ Không thể phân tích ngày {date_str}, sử dụng ngày hiện tại")
                date = time.strftime("%Y-%m-%d")  # Gán ngày hiện tại nếu không parse được

            news_data.append({
                "symbol": symbol,
                "title": title,
                "url": url,
                "date": date,  # SỬA: Sử dụng date đã được chuẩn hóa
                "content": "",
                "author": "",  
                "publish_time": ""  
            })
        except Exception as e:
            print(f"⚠️ Lỗi khi phân tích dòng tin tức: {e}")
            logging.info(f"⚠️ Lỗi khi phân tích dòng tin tức: {e}")
            continue
    return news_data

def vietstock_news_symbol():
    start_time = time.time()

    # Kết nối đến db lấy danh sách mã ck
    db_url = os.getenv('DB_URL')
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
            # SỬA: Đảm bảo date không phải None trước khi lưu
            if not item.get("date") or item["date"] is None or item["date"] == "":
                print(f"⚠️ Trường date trống cho bài viết {item['url']}, gán ngày hiện tại")
                logging.info(f"⚠️ Trường date trống cho bài viết {item['url']}, gán ngày hiện tại")
                item["date"] = time.strftime("%Y-%m-%d")
            pipeline.process_item(item, source=f"crawler_news_stock")
        pipeline.close()

    driver.quit()
    print(f"⏳ Hoàn thành trong {time.time() - start_time:.2f} giây")
    logging.info(f"⏳ Hoàn thành trong {time.time() - start_time:.2f} giây")

if __name__ == "__main__":
    vietstock_news_symbol()