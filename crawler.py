import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# 目标 URL
base_url = "https://15445.courses.cs.cmu.edu/fall2025/schedule.html"
response = requests.get(base_url)
soup = BeautifulSoup(response.text, 'html.parser')

# 定义要下载的类型
targets = {
    "slides": "slides",
    "notes": "notes"
}

# 创建文件夹
for folder in targets.values():
    if not os.path.exists(folder):
        os.makedirs(folder)

# 查找所有链接
links = soup.find_all('a', href=True)
download_count = 0

for link in links:
    href = link['href']
    # 检查链接是否包含 slides/ 或 notes/ 且以 .pdf 结尾
    for folder_key, folder_name in targets.items():
        if f"{folder_key}/" in href and href.endswith('.pdf'):
            file_url = urljoin(base_url, href)
            file_name = os.path.join(folder_name, href.split('/')[-1])
            
            # 执行下载
            print(f"正在下载 {folder_key}: {file_name}...")
            try:
                file_data = requests.get(file_url).content
                with open(file_name, 'wb') as f:
                    f.write(file_data)
                download_count += 1
            except Exception as e:
                print(f"下载失败 {file_name}: {e}")

print(f"\n全部完成！共下载 {download_count} 个文件。")