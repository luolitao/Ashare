import os
import requests

HTTP_PROXY = "http://127.0.0.1:7890"  # 换成你的
HTTPS_PROXY = "http://127.0.0.1:7890"

os.environ["HTTP_PROXY"] = HTTP_PROXY
os.environ["HTTPS_PROXY"] = HTTPS_PROXY

print("当前代理：", HTTP_PROXY)
r = requests.get("https://www.baidu.com", timeout=10)
print("status:", r.status_code)