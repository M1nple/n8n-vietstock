
# import sys, os, time, random, aiohttp, aiohttp_retry, asyncio, pandas as pd, logging, datetime
# sys.path.append('/app')
# from bs4 import BeautifulSoup
# from aiohttp_socks import ProxyConnector
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.by import By
# from playwright.async_api import async_playwright
# from utils.config.driver_config import create_driver
# from utils.pipeline.selenium_pipeline import SeleniumPipeline
# pipeline = SeleniumPipeline()


# # Thiết lập logging
# logging.basicConfig(
#     level=logging.INFO,
#     filename='/app/vietstock/crawled_data/crawl.log',
#     filemode='a',  # Chế độ append
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     encoding='utf-8'
# )

# """
# # Hàm lấy nội dung bài viết bất đồng bộ với độ trễ
# async def get_article_content(session, url, user_agent, max_retries=5):
#     headers = {
#         "User-Agent": user_agent,
#         "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
#         "Accept-Language": "en-US,en;q=0.5",
#         "Referer": "https://vietstock.vn/"
#     }
    
#     retry_options = aiohttp_retry.ExponentialRetry(attempts=max_retries, exceptions={aiohttp.ClientError})
#     retry_client = aiohttp_retry.RetryClient(client_session=session, retry_options=retry_options)

#     try:
#         async with retry_client.get(url, timeout=15, headers=headers) as response:
#             if response.status == 200:
#                 html = await response.text()
#                 soup = BeautifulSoup(html, "html.parser")

#                 article_body = soup.select_one('div[itemprop="articleBody"][id="vst_detail"]')
#                 if not article_body:
#                     print(f"❌ Không tìm thấy nội dung tại {url}")
#                     logging.info(f"❌ Không tìm thấy nội dung tại {url}")
#                     return None

#                 content_parts = []
#                 for element in article_body.find_all(['p', 'div']):
#                     if element.name == 'p' and element.get('class') in [['pTitle'], ['pHead'], ['pBody']]:
#                         text = element.get_text(strip=True)
#                         if text:
#                             content_parts.append(text)
#                     elif element.name == 'div' and element.get('class') == ['pCaption']:
#                         text = element.get_text(strip=True)
#                         if text:
#                             content_parts.append(f"[Chú thích ảnh]: {text}")

#                 content = "\n".join(content_parts) if content_parts else "Không tìm thấy nội dung"

#                 author = article_body.select_one('p.pAuthor a')
#                 # publish_time = article_body.select_one('p.pPublishTimeSource')  # lấy 1 
#                 publish_time = article_body.select('p.pPublishTimeSource') # lấy tất cả 

#                 result = {
#                     "content": content,
#                     "author": author.get_text(strip=True) if author else "",
#                     "publish_time": publish_time.get_text(strip=True).replace('- ', '') if publish_time else ""
#                 }

#                 print(f"✅ Đã lấy nội dung bài viết: {url}")
#                 logging.info(f"✅ Đã lấy nội dung bài viết: {url}")
#                 await asyncio.sleep(random.uniform(1, 2))
#                 return result
#             else:
#                 print(f"❌ Lỗi HTTP {response.status} tại {url}")
#                 logging.info(f"❌ Lỗi HTTP {response.status} tại {url}")
#                 return None
#     except Exception as e:
#         print(f"❌ Lỗi lấy nội dung từ {url}: {e}")
#         logging.info(f"❌ Lỗi lấy nội dung từ {url}: {e}")
#         return None

# # Hàm lấy nội dung tất cả bài viết với giới hạn số yêu cầu đồng thời
# async def fetch_all_article_contents(articles_data, user_agent, proxy=None, max_concurrent=3):
#     print("Đang chạy fetch_all_article_contents với max_concurrent =", max_concurrent)
#     logging.info("Đang chạy fetch_all_article_contents với max_concurrent =", max_concurrent)
#     connector = ProxyConnector.from_url(f"http://{proxy}") if proxy else None
#     async with aiohttp.ClientSession(connector=connector) as session:
#         semaphore = asyncio.Semaphore(max_concurrent)
        
#         async def limited_get_article_content(article):
#             async with semaphore:
#                 result = await get_article_content(session, article["url"], user_agent)
#                 if result:
#                     article["content"] = result["content"]
#                     article["author"] = result["author"]
#                     article["publish_time"] = result["publish_time"]
#                 else:
#                     article["content"] = "Lỗi khi lấy nội dung"
#                     article["author"] = ""
#                     article["publish_time"] = ""
        
#         tasks = [limited_get_article_content(article) for article in articles_data]
#         await asyncio.gather(*tasks, return_exceptions=True)
#     return articles_data
        
# # Hàm lấy nội dung tất cả bài viết với giới hạn số yêu cầu đồng thời

# # async def fetch_all_article_contents(articles_data, user_agent, proxy=None, max_concurrent=3):
# #     print("Đang chạy fetch_all_article_contents với max_concurrent =", max_concurrent)
# #     logging.info("Đang chạy fetch_all_article_contents với max_concurrent =", max_concurrent)
# #     connector = ProxyConnector.from_url(f"http://{proxy}") if proxy else None
# #     async with aiohttp.ClientSession(connector=connector) as session:
# #         semaphore = asyncio.Semaphore(max_concurrent)
        
# #         async def limited_get_article_content(article):
# #             async with semaphore:
# #                 result = await get_article_content(session, article["url"], user_agent)
# #                 if result:
# #                     article["content"] = result["content"]
# #                     article["author"] = result["author"]
# #                     article["publish_time"] = result["publish_time"]
# #                 else:
# #                     article["content"] = "Lỗi khi lấy nội dung"
# #                     article["author"] = ""
# #                     article["publish_time"] = ""
        
# #         tasks = [limited_get_article_content(article) for article in articles_data]
# #         await asyncio.gather(*tasks, return_exceptions=True)
# #     return articles_data

# # Hàm chính cào tin tức mới nhất
# def vietstock_news_latest(pages_limit=2):
#     start_time = time.time()
#     driver, user_agent, proxy = create_driver(headless=True, use_proxy=False)
#     url = "https://vietstock.vn/chu-de/1-2/moi-cap-nhat.htm"
#     driver.get(url)

#     page_count = 0
#     articles_data = []
#     seen_urls = set()

#     while page_count < pages_limit:
#         page_count += 1
#         try:
#             articles = WebDriverWait(driver, 10).until(
#                 EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.single_post.post_type12"))
#             )
#             print(f"✅ Tìm thấy {len(articles)} bài viết trên trang {page_count}")
#             logging.info(f"✅ Tìm thấy {len(articles)} bài viết trên trang {page_count}")

#             for article in articles:
#                 try:
#                     title_el = article.find_element(By.CSS_SELECTOR, "div.single_post_text h4 a")
#                     url = title_el.get_attribute("href")
#                     if not url.startswith("http"):
#                         url = "https://vietstock.vn" + url

#                     if url in seen_urls:
#                         continue
#                     seen_urls.add(url)

#                     title = title_el.text.strip()
#                     date_el = article.find_element(By.CSS_SELECTOR, "div.img_wrap p")
#                     date = date_el.text.strip()

#                     articles_data.append({
#                         # "symbol": "LATEST",
#                         "date": date,
#                         "title": title,
#                         "url": url,
#                         "content": "",
#                         "author": "",
#                         "publish_time": ""
#                     })

#                 except Exception as e:
#                     print(f"❌ Bỏ qua 1 bài: {e}")
#                     logging.info(f"❌ Bỏ qua 1 bài: {e}")
#                     continue

#             driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
#             time.sleep(random.uniform(3, 5))

#             try:
#                 next_button = driver.find_element(By.XPATH, '//li[contains(@id, "page-next")]/a')
#                 driver.execute_script("arguments[0].click();", next_button)
#                 time.sleep(random.uniform(4, 6))
#             except Exception as e:
#                 print(f"❌ Không tìm thấy hoặc đã hết trang! Lỗi: {e}")
#                 logging.info(f"❌ Không tìm thấy hoặc đã hết trang! Lỗi: {e}")
#                 break

#         except Exception as e:
#             print(f"❌ Lỗi khi load bài viết: {e}")
#             logging.info(f"❌ Lỗi khi load bài viết: {e}")
#             break

#     driver.quit()

#     if articles_data:
#         articles_data = asyncio.run(fetch_all_article_contents(articles_data, user_agent, max_concurrent=5))

#     for item in articles_data:
#         pipeline.process_item(item, source="crawler_news_latest")
#     pipeline.close()

#     print(f"⏳ Hoàn thành trong {time.time() - start_time:.2f} giây")
#     logging.info(f"⏳ Hoàn thành trong {time.time() - start_time:.2f} giây")
#     return articles_data


# if __name__ == "__main__":
#     pages_limit = 2
#     vietstock_news_latest()

# """

# # Hàm lấy nội dung bài viết bằng aiohttp (cho vietstock.vn)
# async def get_article_content_with_aiohttp(session, url, user_agent, max_retries=5):
#     headers = {
#         "User-Agent": user_agent,
#         "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
#         "Accept-Language": "en-US,en;q=0.5",
#         "Referer": "https://vietstock.vn/"
#     }
    
#     retry_options = aiohttp_retry.ExponentialRetry(attempts=max_retries, exceptions={aiohttp.ClientError})
#     retry_client = aiohttp_retry.RetryClient(client_session=session, retry_options=retry_options)

#     try:
#         async with retry_client.get(url, timeout=10, headers=headers) as response:  # SỬA: Giảm timeout từ 15 xuống 10 vì vietstock.vn tải nhanh
#             if response.status == 200:
#                 html = await response.text()
#                 soup = BeautifulSoup(html, "html.parser")
#                 article_body = soup.select_one('div[itemprop="articleBody"][id="vst_detail"]')
#                 if not article_body:
#                     print(f"❌ Không tìm thấy nội dung tại {url}")
#                     logging.info(f"❌ Không tìm thấy nội dung tại {url}")
#                     return None

#                 content_parts = []
#                 for element in article_body.find_all(['p', 'div']):
#                     if element.get('class') in [['pTitle'], ['pHead'], ['pBody']]:
#                         text = element.get_text(strip=True)
#                         if text:
#                             content_parts.append(text)
#                     elif element.get('class') == ['pCaption']:
#                         text = element.get_text(strip=True)
#                         if text:
#                             content_parts.append(f"[Chú thích ảnh]: {text}")

#                 content = "\n".join(content_parts) if content_parts else "Không tìm thấy nội dung"
#                 author = article_body.select_one('p.pAuthor a')
#                 publish_time = article_body.select_one('p.pPublishTimeSource')

#                 result = {
#                     "content": content,
#                     "author": author.get_text(strip=True) if author else "",
#                     "publish_time": publish_time.get_text(strip=True).replace('- ', '') if publish_time else None,
#                     "source": "vietstock" if "vietstock.vn" in url else "external"  # SỬA: Thêm trường source để xác định nguồn
#                 }

#                 print(f"✅ Đã lấy nội dung bằng aiohttp: {url} (publish_time: {result['publish_time'] or 'Không có'}, source: {result['source']})")  # SỬA: Thêm source vào log
#                 logging.info(f"✅ Đã lấy nội dung bằng aiohttp: {url} (publish_time: {result['publish_time'] or 'Không có'}, source: {result['source']})")  # SỬA: Thêm source vào log
#                 return result
#             else:
#                 print(f"❌ Lỗi HTTP {response.status} tại {url}")
#                 logging.info(f"❌ Lỗi HTTP {response.status} tại {url}")
#                 return None
#     except Exception as e:
#         print(f"❌ Lỗi lấy nội dung từ {url} bằng aiohttp: {e}")
#         logging.info(f"❌ Lỗi lấy nội dung từ {url} bằng aiohttp: {e}")
#         return None

# # SỬA: Thêm hàm mới để lấy nội dung bằng Playwright (cho nguồn external như fili.vn)
# async def get_article_content_with_playwright(page, url):
#     try:
#         await page.goto(url, timeout=15000)  # SỬA: Tăng timeout cho nguồn bên ngoài
#         await page.wait_for_selector('div[itemprop="articleBody"][id="vst_detail"]', timeout=5000)

#         content = await page.content()
#         soup = BeautifulSoup(content, "html.parser")
#         article_body = soup.select_one('div[itemprop="articleBody"][id="vst_detail"]')
#         if not article_body:
#             print(f"❌ Không tìm thấy nội dung tại {url}")
#             logging.info(f"❌ Không tìm thấy nội dung tại {url}")
#             return None

#         content_parts = []
#         for element in article_body.find_all(['p', 'div']):
#             if element.get('class') in [['pTitle'], ['pHead'], ['pBody']]:
#                 text = element.get_text(strip=True)
#                 if text:
#                     content_parts.append(text)
#             elif element.get('class') == ['pCaption']:
#                 text = element.get_text(strip=True)
#                 if text:
#                     content_parts.append(f"[Chú thích ảnh]: {text}")

#         content = "\n".join(content_parts) if content_parts else "Không tìm thấy nội dung"
#         author = article_body.select_one('p.pAuthor a')
#         publish_time = article_body.select_one('p.pPublishTimeSource')

#         result = {
#             "content": content,
#             "author": author.get_text(strip=True) if author else "",
#             "publish_time": publish_time.get_text(strip=True).replace('- ', '') if publish_time else "",
#             "source": "external"
#         }

#         print(f"✅ Đã lấy nội dung bằng Playwright: {url} (publish_time: {result['publish_time'] or 'Không có'}, source: {result['source']})")
#         logging.info(f"✅ Đã lấy nội dung bằng Playwright: {url} (publish_time: {result['publish_time'] or 'Không có'}, source: {result['source']})")
#         await asyncio.sleep(random.uniform(0.5, 1))
#         return result
#     except Exception as e:
#         print(f"❌ Lỗi lấy nội dung từ {url} bằng Playwright: {e}")
#         logging.info(f"❌ Lỗi lấy nội dung từ {url} bằng Playwright: {e}")
#         return None

# # SỬA: Cập nhật hàm fetch_all_article_contents để phân biệt nguồn
# async def fetch_all_article_contents(articles_data, user_agent, max_concurrent=5):
#     print(f"Đang chạy fetch_all_article_contents với max_concurrent = {max_concurrent}")
#     logging.info(f"Đang chạy fetch_all_article_contents với max_concurrent = {max_concurrent}")
#     async with async_playwright() as p:
#         browser = await p.chromium.launch(headless=True)
#         context = await browser.new_context(user_agent=user_agent)
#         page = await context.new_page()

#         connector = ProxyConnector.from_url("http://proxy:port") if "proxy" in locals() else None
#         async with aiohttp.ClientSession(connector=connector) as session:
#             semaphore = asyncio.Semaphore(max_concurrent)
            
#             async def limited_get_article_content(article):
#                 async with semaphore:
#                     if "vietstock.vn" in article["url"]:  # SỬA: Dùng aiohttp cho vietstock.vn
#                         result = await get_article_content_with_aiohttp(session, article["url"], user_agent)
#                     else:  # SỬA: Dùng Playwright cho nguồn external (fili.vn)
#                         result = await get_article_content_with_playwright(page, article["url"])
                    
#                     if result:
#                         article["content"] = result["content"]
#                         article["author"] = result["author"]
#                         article["publish_time"] = result["publish_time"]
#                     else:
#                         article["content"] = "Lỗi khi lấy nội dung"
#                         article["author"] = ""
#                         article["publish_time"] = ""

#             tasks = [limited_get_article_content(article) for article in articles_data]
#             await asyncio.gather(*tasks, return_exceptions=True)
#         await browser.close()
#     return articles_data

# # Hàm chính cào tin tức mới nhất (không sửa đổi)
# def vietstock_news_latest(pages_limit=2):
#     start_time = time.time()
#     driver, user_agent, proxy = create_driver(headless=True, use_proxy=False)
#     url = "https://vietstock.vn/chu-de/1-2/moi-cap-nhat.htm"
#     driver.get(url)

#     page_count = 0
#     articles_data = []
#     seen_urls = set()

#     while page_count < pages_limit:
#         page_count += 1
#         try:
#             articles = WebDriverWait(driver, 10).until(
#                 EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.single_post.post_type12"))
#             )
#             print(f"✅ Tìm thấy {len(articles)} bài viết trên trang {page_count}")
#             logging.info(f"✅ Tìm thấy {len(articles)} bài viết trên trang {page_count}")

#             for article in articles:
#                 try:
#                     title_el = article.find_element(By.CSS_SELECTOR, "div.single_post_text h4 a")
#                     url = title_el.get_attribute("href")
#                     if not url.startswith("http"):
#                         url = "https://vietstock.vn" + url

#                     if url in seen_urls:
#                         continue
#                     seen_urls.add(url)

#                     title = title_el.text.strip()
#                     date_el = article.find_element(By.CSS_SELECTOR, "div.img_wrap p")
#                     date = date_el.text.strip()

#                     articles_data.append({
#                         "date": date,
#                         "title": title,
#                         "url": url,
#                         "content": "",
#                         "author": "",
#                         "publish_time": ""
#                     })

#                 except Exception as e:
#                     print(f"❌ Bỏ qua 1 bài: {e}")
#                     logging.info(f"❌ Bỏ qua 1 bài: {e}")
#                     continue

#             driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
#             time.sleep(random.uniform(2, 4))

#             try:
#                 next_button = driver.find_element(By.XPATH, '//li[contains(@id, "page-next")]/a')
#                 driver.execute_script("arguments[0].click();", next_button)
#                 time.sleep(random.uniform(3, 5))
#             except Exception as e:
#                 print(f"❌ Không tìm thấy hoặc đã hết trang! Lỗi: {e}")
#                 logging.info(f"❌ Không tìm thấy hoặc đã hết trang! Lỗi: {e}")
#                 break

#         except Exception as e:
#             print(f"❌ Lỗi khi load bài viết: {e}")
#             logging.info(f"❌ Lỗi khi load bài viết: {e}")
#             break

#     if articles_data:
#         articles_data = asyncio.run(fetch_all_article_contents(articles_data, user_agent, max_concurrent=5))

#     for item in articles_data:
#         pipeline.process_item(item, source="crawler_news_latest")
#     pipeline.close()
#     driver.quit()

#     print(f"⏳ Hoàn thành trong {time.time() - start_time:.2f} giây")
#     logging.info(f"⏳ Hoàn thành trong {time.time() - start_time:.2f} giây")
#     return articles_data

# if __name__ == "__main__":
#     pages_limit = 2
#     vietstock_news_latest()




import sys, os, time, random, aiohttp, aiohttp_retry, asyncio, pandas as pd, logging, datetime
sys.path.append('/app')
from bs4 import BeautifulSoup
from aiohttp_socks import ProxyConnector
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from playwright.async_api import async_playwright
from utils.config.driver_config import create_driver
from utils.pipeline.selenium_pipeline import SeleniumPipeline
pipeline = SeleniumPipeline()

# Thiết lập logging
logging.basicConfig(
    level=logging.DEBUG,  # SỬA: Thêm level DEBUG để ghi log chi tiết
    filename='/app/vietstock/crawled_data/crawl.log',
    filemode='a',  # Chế độ append
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

# Hàm lấy nội dung bài viết bằng aiohttp (cho vietstock.vn)
async def get_article_content_with_aiohttp(session, url, user_agent, max_retries=5):
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://vietstock.vn/"
    }
    
    retry_options = aiohttp_retry.ExponentialRetry(attempts=max_retries, exceptions={aiohttp.ClientError})
    retry_client = aiohttp_retry.RetryClient(client_session=session, retry_options=retry_options)

    try:
        async with retry_client.get(url, timeout=10, headers=headers) as response:
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
                    if element.get('class') in [['pTitle'], ['pHead'], ['pBody']]:
                        text = element.get_text(strip=True)
                        if text:
                            content_parts.append(text)
                    elif element.get('class') == ['pCaption']:
                        text = element.get_text(strip=True)
                        if text:
                            content_parts.append(f"[Chú thích ảnh]: {text}")

                content = "\n".join(content_parts) if content_parts else "Không tìm thấy nội dung"
                author = article_body.select_one('p.pAuthor a')
                publish_time = article_body.select_one('p.pPublishTimeSource')

                result = {
                    "content": content,
                    "author": author.get_text(strip=True) if author else "",
                    "publish_time": publish_time.get_text(strip=True).replace('- ', '') if publish_time else None,
                    "source": "vietstock" if "vietstock.vn" in url else "external"
                }

                print(f"✅ Đã lấy nội dung bằng aiohttp: {url} (publish_time: {result['publish_time'] or 'Không có'}, source: {result['source']})")
                logging.info(f"✅ Đã lấy nội dung bằng aiohttp: {url} (publish_time: {result['publish_time'] or 'Không có'}, source: {result['source']})")
                return result
            else:
                print(f"❌ Lỗi HTTP {response.status} tại {url}")
                logging.info(f"❌ Lỗi HTTP {response.status} tại {url}")
                return None
    except Exception as e:
        print(f"❌ Lỗi lấy nội dung từ {url} bằng aiohttp: {e}")
        logging.info(f"❌ Lỗi lấy nội dung từ {url} bằng aiohttp: {e}")
        return None

# Hàm lấy nội dung bài viết bằng Playwright (cho nguồn external như fili.vn)
async def get_article_content_with_playwright(page, url, max_retries=3):
    for attempt in range(max_retries):
        try:
            await page.goto(url, timeout=30000)  # SỬA: Tăng timeout lên 30 giây
            if "fili.vn" in url:
                await page.wait_for_selector('div.content', timeout=10000)  # Selector nội dung của fili.vn
            else:
                await page.wait_for_selector('div[itemprop="articleBody"][id="vst_detail"]', timeout=10000)

            content = await page.content()
            soup = BeautifulSoup(content, "html.parser")
            
            # Chọn selector phù hợp dựa trên domain
            if "fili.vn" in url:
                article_body = soup.select_one('div.content')  # Selector nội dung của fili.vn
                publish_time = soup.select_one('time')  # Selector thời gian của fili.vn
                author = soup.select_one('span.author')  # Selector tác giả của fili.vn (nếu có)
            else:
                article_body = soup.select_one('div[itemprop="articleBody"][id="vst_detail"]')
                publish_time = soup.select_one('p.pPublishTimeSource')
                author = article_body.select_one('p.pAuthor a') if article_body else None

            if not article_body:
                print(f"❌ Không tìm thấy nội dung tại {url}")
                logging.info(f"❌ Không tìm thấy nội dung tại {url}")
                return None

            content_parts = []
            for element in article_body.find_all(['p', 'div']):
                if element.get('class') in [['pTitle'], ['pHead'], ['pBody'], None]:
                    text = element.get_text(strip=True)
                    if text:
                        content_parts.append(text)
                elif element.get('class') == ['pCaption']:
                    text = element.get_text(strip=True)
                    if text:
                        content_parts.append(f"[Chú thích ảnh]: {text}")

            content = "\n".join(content_parts) if content_parts else "Không tìm thấy nội dung"

            result = {
                "content": content,
                "author": author.get_text(strip=True) if author else "",
                "publish_time": publish_time.get_text(strip=True).replace('- ', '') if publish_time else "",
                "source": "external" if "fili.vn" in url else "vietstock"
            }

            print(f"✅ Đã lấy nội dung bằng Playwright: {url} (publish_time: {result['publish_time'] or 'Không có'}, source: {result['source']})")
            logging.info(f"✅ Đã lấy nội dung bằng Playwright: {url} (publish_time: {result['publish_time'] or 'Không có'}, source: {result['source']})")
            await asyncio.sleep(random.uniform(0.5, 1))
            return result
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️ Lỗi lấy nội dung từ {url} (lần {attempt + 1}/{max_retries}): {e}. Thử lại...")
                logging.info(f"⚠️ Lỗi lấy nội dung từ {url} (lần {attempt + 1}/{max_retries}): {e}. Thử lại...")
                await asyncio.sleep(random.uniform(1, 3))
                continue
            print(f"❌ Lỗi lấy nội dung từ {url} bằng Playwright sau {max_retries} lần thử: {e}")
            logging.info(f"❌ Lỗi lấy nội dung từ {url} bằng Playwright sau {max_retries} lần thử: {e}")
            return None

# Cập nhật hàm fetch_all_article_contents để phân biệt nguồn và chuẩn hóa date
async def fetch_all_article_contents(articles_data, user_agent, max_concurrent=5):
    print(f"Đang chạy fetch_all_article_contents với max_concurrent = {max_concurrent}")
    logging.info(f"Đang chạy fetch_all_article_contents với max_concurrent = {max_concurrent}")
    
    async def try_with_aiohttp(article, user_agent):
        connector = ProxyConnector.from_url("http://proxy:port") if "proxy" in locals() else None
        async with aiohttp.ClientSession(connector=connector) as session:
            result = await get_article_content_with_aiohttp(session, article["url"], user_agent)
            if result:
                article["content"] = result["content"] or ""
                article["author"] = result["author"] or ""
                article["publish_time"] = result["publish_time"] or ""

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=user_agent)
        pages = [await context.new_page() for _ in range(max_concurrent)]  # SỬA: Tạo nhiều page để tránh xung đột
        page_index = 0

        connector = ProxyConnector.from_url("http://proxy:port") if "proxy" in locals() else None
        async with aiohttp.ClientSession(connector=connector) as session:
            semaphore = asyncio.Semaphore(max_concurrent)

            async def limited_get_article_content(article):
                nonlocal page_index
                async with semaphore:
                    if "vietstock.vn" in article["url"]:
                        result = await get_article_content_with_aiohttp(session, article["url"], user_agent)
                    else:
                        page = pages[page_index % max_concurrent]  # SỬA: Sử dụng page khác nhau cho mỗi yêu cầu
                        page_index += 1
                        result = await get_article_content_with_playwright(page, article["url"])
                        if not result:  # SỬA: Fallback với aiohttp nếu Playwright thất bại
                            print(f"⚠️ Playwright thất bại, thử lại bằng aiohttp cho {article['url']}")
                            logging.info(f"⚠️ Playwright thất bại, thử lại bằng aiohttp cho {article['url']}")
                            await try_with_aiohttp(article, user_agent)
                            result = {"content": article["content"], "author": article["author"], "publish_time": article["publish_time"]}

                    if result:
                        article["content"] = result["content"] or ""
                        article["author"] = result["author"] or ""
                        article["publish_time"] = result["publish_time"] or ""
                        logging.debug(f"Đã lấy dữ liệu cho {article['url']}: content={article['content'][:50]}..., publish_time={article['publish_time']}")
                        # Chuẩn hóa date từ publish_time nếu cần
                        if not article.get("date") or article["date"] is None or article["date"] == "":
                            if result["publish_time"]:
                                try:
                                    date_part = result["publish_time"].split()[-1]
                                    date_obj = datetime.datetime.strptime(date_part, "%d/%m/%Y")
                                    article["date"] = date_obj.strftime("%Y-%m-%d")
                                except (IndexError, ValueError):
                                    print(f"⚠️ Không thể phân tích publish_time {result['publish_time']} cho {article['url']}, giữ date gốc hoặc gán ngày hiện tại")
                                    logging.info(f"⚠️ Không thể phân tích publish_time {result['publish_time']} cho {article['url']}, giữ date gốc hoặc gán ngày hiện tại")
                                    article["date"] = article.get("date") or datetime.datetime.now().strftime("%Y-%m-%d")
                            else:
                                article["date"] = article.get("date") or datetime.datetime.now().strftime("%Y-%m-%d")
                    else:
                        print(f"⚠️ Bỏ qua bài viết do không lấy được nội dung: {article['url']}")
                        logging.info(f"⚠️ Bỏ qua bài viết do không lấy được nội dung: {article['url']}")
                        article["content"] = "Lỗi khi lấy nội dung"
                        article["author"] = ""
                        article["publish_time"] = ""
                        article["date"] = article.get("date") or datetime.datetime.now().strftime("%Y-%m-%d")

            tasks = [limited_get_article_content(article) for article in articles_data]
            await asyncio.gather(*tasks, return_exceptions=True)

        for page in pages:
            await page.close()
        await browser.close()
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

                    title = title_el.text.strip() if title_el.text.strip() else "Không có tiêu đề"
                    date_el = article.find_element(By.CSS_SELECTOR, "div.img_wrap p")
                    date = date_el.text.strip() if date_el.text.strip() else datetime.datetime.now().strftime("%d/%m/%Y")

                    articles_data.append({
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
            time.sleep(random.uniform(2, 4))

            try:
                next_button = driver.find_element(By.XPATH, '//li[contains(@id, "page-next")]/a')
                driver.execute_script("arguments[0].click();", next_button)
                time.sleep(random.uniform(3, 5))
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
        articles_data = asyncio.run(fetch_all_article_contents(articles_data, user_agent, max_concurrent=5))

    # Kiểm tra dữ liệu trước khi lưu
    for item in articles_data:
        if not item["content"] or item["content"] == "Lỗi khi lấy nội dung":
            logging.warning(f"⚠️ Bài viết không có nội dung: {item['url']}")
        if not item["publish_time"]:
            logging.warning(f"⚠️ Bài viết không có publish_time: {item['url']}")
        pipeline.process_item(item, source="crawler_news_latest")

    pipeline.close()

    print(f"⏳ Hoàn thành trong {time.time() - start_time:.2f} giây")
    logging.info(f"⏳ Hoàn thành trong {time.time() - start_time:.2f} giây")
    return articles_data

if __name__ == "__main__":
    pages_limit = 10
    vietstock_news_latest()