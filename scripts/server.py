import os, sys, glob
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  # Thêm đường dẫn đến utils
from flask import Flask, request, jsonify
import subprocess, threading
import pandas as pd

app = Flask(__name__)

"""
@app.route('/crawl', methods=['POST'])
def crawl():
    try:
        data = request.get_json()
        exchange = data.get("exchange", "0")
        print(f"Nhận lệnh crawl sàn: {exchange}")

        # Chạy subprocess để không block request
        result = subprocess.run(
            ["python", "scripts/vietstock_company.py", exchange],
            capture_output=True,
            text=True
        )

        return jsonify({
            "code": 0 if result.returncode == 0 else 1,
            "exchange": exchange,
            "status": "success" if result.returncode == 0 else "failed",
            "stdout": result.stdout,
            "stderr": result.stderr
        })
    except Exception as e:
        return jsonify({
            "code": 2,
            "status": "error",
            "message": str(e)
        })
"""    

"""
@app.route('/merge_csv', methods=['POST'])
def merge_csv():
    try:
        # Gọi hàm merge từ pipeline_selenium.py
        result = merge_csv_files()  # Hàm này sẽ hợp nhất các file CSV tạm thời
        return jsonify({
            'status': 'success',
            'stdout': result,
            'stderr': '',
            'code': 0
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'stdout': '',
            'stderr': str(e),
            'code': 1
        }), 500
"""
"""
@app.route('/vietstock_crawl_price', methods=['POST'])
def vietstock_crawl_price():
    try:
        print("Nhận lệnh crawl giá cho các sàn HOSE, HNX, UPCOM (auto Pool).")

        # Chạy subprocess để chạy nguyên file scripts/vietstock_price.py
        result = subprocess.run(
            ["python", "scripts/vietstock_price.py"],  # Không cần truyền exchange
            capture_output=True,
            text=True
        )

        return jsonify({
            "code": 0 if result.returncode == 0 else 1,
            "status": "success" if result.returncode == 0 else "failed",
            "stdout": result.stdout,
            "stderr": result.stderr
        })
    except Exception as e:
        return jsonify({
            "code": 2,
            "status": "error",
            "message": str(e)
        }), 500
"""

# 👉 Crawl company 
@app.route('/crawl_company', methods=['POST'])
def crawl_company():
    def run_company_crawl():
        subprocess.run(["python3", "scripts/vietstock_company.py"])

    threading.Thread(target=run_company_crawl).start()
    return jsonify({
        "status": "started",
        "message": "🟢 Đã bắt đầu crawl dữ liệu công ty trong background"
    })

# 👉 Crawl price 
@app.route('/vietstock_crawl_price', methods=['POST'])
def crawl_price():
    def run_price_crawl():
        subprocess.run(["python3", "scripts/vietstock_price.py"])

    threading.Thread(target=run_price_crawl).start()
    return jsonify({
        "status": "started",
        "message": "Đã bắt đầu crawl giá cổ phiếu trong background"
    })

# 👉 Crawl news latest 
@app.route('/vietstock_crawl_news_latest', methods=['POST'])
def crawl_news_latest():
    def run_news_latest_crawl():
        subprocess.run(["python3", "scripts/vietstock_news_latest.py"])

    threading.Thread(target=run_news_latest_crawl).start()
    return jsonify({
        "status": "started",
        "message": "Đã bắt đầu crawl tin tức trong background"
    })

# 👉 Crawl news latest 
@app.route('/vietstock_crawl_news_stock', methods=['POST'])
def crawl_news_stock():
    def run_news_latest_stock():
        subprocess.run(["python3", "scripts/vietstock_news_stock.py"])

    threading.Thread(target=run_news_latest_stock).start()
    return jsonify({
        "status": "started",
        "message": "Đã bắt đầu crawl tin tức chứng khoán trong background"
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)


