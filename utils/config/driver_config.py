'''
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import random
import requests
import logging
from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_fixed
from selenium.common.exceptions import WebDriverException

logging.basicConfig(
    level=logging.INFO,
    filename='/app/vietstock/crawled_data/crawl.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

def is_proxy_working(proxy):
    try:
        response = requests.get(
            "https://www.google.com",
            proxies={"http": f"http://{proxy}", "https": f"http://{proxy}"},
            timeout=5
        )
        return response.status_code == 200
    except Exception as e:
        logging.warning(f"Proxy {proxy} kiểm tra thất bại: {e}")
        return False

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def create_driver(headless=True, use_proxy=False):
    try:
        ua = UserAgent()
        random_user_agent = ua.random
    except Exception as e:
        logging.error(f"Lỗi khi tạo UserAgent: {e}")
        random_user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument(f"user-agent={random_user_agent}")

    proxy = None
    if use_proxy:
        proxy_list = [
            "123.45.67.89:8080",
            "111.222.333.444:3128",
            "101.102.103.104:8000"
        ]
        for _ in range(3):
            proxy = random.choice(proxy_list)
            if is_proxy_working(proxy):
                chrome_options.add_argument(f"--proxy-server=http://{proxy}")
                logging.info(f"Đang dùng Proxy: {proxy}")
                break
            else:
                logging.warning(f"Proxy {proxy} không hoạt động, thử proxy khác...")
                proxy_list.remove(proxy)
        else:
            logging.error("Không tìm thấy proxy hoạt động, dùng kết nối trực tiếp.")
            proxy = None

    try:
        service = Service("/usr/local/bin/chromedriver")
        driver = webdriver.Chrome(service=service, options=chrome_options)
    except WebDriverException as e:
        logging.error(f"Lỗi khi tạo WebDriver: {e}")
        raise

    logging.info(f"User-Agent đang dùng: {random_user_agent}")
    return driver, random_user_agent, proxy
'''

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import random
import requests
import logging
from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_fixed
from selenium.common.exceptions import WebDriverException

logging.basicConfig(
    level=logging.INFO,
    filename='/app/vietstock/crawled_data/crawl.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

def is_proxy_working(proxy):
    try:
        response = requests.get(
            "https://www.google.com",
            proxies={"http": f"http://{proxy}", "https": f"http://{proxy}"},
            timeout=5
        )
        return response.status_code == 200
    except Exception as e:
        logging.warning(f"Proxy {proxy} kiểm tra thất bại: {e}")
        return False

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def create_driver(headless=True, use_proxy=False):
    try:
        ua = UserAgent()
        random_user_agent = ua.random
    except Exception as e:
        logging.error(f"Lỗi tạo UserAgent: {e}")
        random_user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument(f"user-agent={random_user_agent}")

    proxy = None
    if use_proxy:
        proxy_list = [
            "123.45.67.89:8080",
            "111.222.333.444:3128",
            "101.102.103.104:8000"
        ]
        random.shuffle(proxy_list)
        for pxy in proxy_list:
            if is_proxy_working(pxy):
                chrome_options.add_argument(f"--proxy-server=http://{pxy}")
                logging.info(f"Dùng Proxy: {pxy}")
                proxy = pxy
                break
        if proxy is None:
            logging.warning("Không có proxy tốt, dùng IP thật.")

    try:
        service = Service("/usr/bin/chromedriver")  # chính xác path Docker
        driver = webdriver.Chrome(service=service, options=chrome_options)
    except WebDriverException as e:
        logging.error(f"Lỗi WebDriver: {e}")
        raise
    logging.info("===================================================================================================================================================")
    logging.info(f"User-Agent dùng: {random_user_agent}")
    return driver, random_user_agent, proxy
