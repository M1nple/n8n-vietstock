import sys, os, time, random, aiohttp, aiohttp_retry, asyncio, pandas as pd, logging
sys.path.append('/app')
from bs4 import BeautifulSoup
from aiohttp_socks import ProxyConnector
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from utils.config.driver_config import create_driver
from utils.pipeline.selenium_pipeline import save_csv_to_postgres

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
                publish_time = article_body.select_one('p.pPublishTimeSource')

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

# Hàm lấy nội dung tất cả bài viết với giới hạn số yêu cầu đồng thời
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

# Hàm chính cào tin tức mới nhất
def vietstock_news_latest(pages_limit=2):
    start_time = time.time()
    driver, user_agent, proxy = create_driver(headless=True, use_proxy=False)
    url = "https://vietstock.vn/chu-de/1-2/moi-cap-nhat.htm"
    driver.get(url)

    page_count = 0
    articles_data = []
    seen_urls = set()

    while page_count < pages_limit:
        page_count += 1
        try:
            articles = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.single_post.post_type12"))
            )
            print(f"✅ Tìm thấy {len(articles)} bài viết trên trang {page_count}")
            logging.info(f"✅ Tìm thấy {len(articles)} bài viết trên trang {page_count}")

            for article in articles:
                try:
                    title_el = article.find_element(By.CSS_SELECTOR, "div.single_post_text h4 a")
                    url = title_el.get_attribute("href")
                    if not url.startswith("http"):
                        url = "https://vietstock.vn" + url

                    if url in seen_urls:
                        continue
                    seen_urls.add(url)

                    title = title_el.text.strip()
                    date_el = article.find_element(By.CSS_SELECTOR, "div.img_wrap p")
                    date = date_el.text.strip()

                    articles_data.append({
                        # "symbol": "LATEST",
                        "date": date,
                        "title": title,
                        "url": url,
                        "content": "",
                        "author": "",
                        "publish_time": ""
                    })

                except Exception as e:
                    print(f"❌ Bỏ qua 1 bài: {e}")
                    logging.info(f"❌ Bỏ qua 1 bài: {e}")
                    continue

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(3, 5))

            try:
                next_button = driver.find_element(By.XPATH, '//li[contains(@id, "page-next")]/a')
                driver.execute_script("arguments[0].click();", next_button)
                time.sleep(random.uniform(4, 6))
            except Exception as e:
                print(f"❌ Không tìm thấy hoặc đã hết trang! Lỗi: {e}")
                logging.info(f"❌ Không tìm thấy hoặc đã hết trang! Lỗi: {e}")
                break

        except Exception as e:
            print(f"❌ Lỗi khi load bài viết: {e}")
            logging.info(f"❌ Lỗi khi load bài viết: {e}")
            break

    driver.quit()

    if articles_data:
        articles_data = asyncio.run(fetch_all_article_contents(articles_data, user_agent, proxy, max_concurrent=3))

    for item in articles_data:
        pipeline.process_item(item, source="vst_news_latest_final")
    pipeline.save_data()

    print(f"⏳ Hoàn thành trong {time.time() - start_time:.2f} giây")
    logging.info(f"⏳ Hoàn thành trong {time.time() - start_time:.2f} giây")
    return articles_data


if __name__ == "__main__":
    pages_limit = 5
    vietstock_news_latest()

    ### save to db ###
    db_url = "postgresql://postgres:652003@host.docker.internal:5432/vnstock"
    csv_file = "/app/vietstock/crawled_data/vst_news_latest_final.csv"
    table_name = "crawler_news_latest"

    # Lưu dữ liệu vào PostgreSQL
    save_csv_to_postgres(csv_file=csv_file, db_url=db_url, table_name=table_name, if_exists="append")

    # save_csv_to_postgres(csv_file=csv_file, db_url=db_url, table_name=table_name, if_exists="append") # chỉnh thành append để ghi vào kh bị xóa cái cũ 