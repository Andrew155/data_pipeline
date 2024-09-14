import requests
from bs4 import BeautifulSoup

# URL của trang web
url = 'https://www.imdb.com/chart/top/?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=470df400-70d9-4f35-bb05-8646a1195842&pf_rd_r=5V6VAGPEK222QB9E0SZ8&pf_rd_s=right-4&pf_rd_t=15506&pf_rd_i=toptv&ref_=chttvtp_ql_3'

# Thay đổi User-Agent để trông giống như yêu cầu từ trình duyệt
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Gửi yêu cầu đến trang web với header User-Agent
response = requests.get(url, headers=headers)

# Kiểm tra xem yêu cầu có thành công không
if response.status_code == 200:
    # Phân tích nội dung HTML
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Tìm tất cả các thẻ <li>
    all_li_tags = soup.find_all('li', {'class': 'ipc-metadata-list-summary-item sc-10233bc-0 TwzGn cli-parent'})
    
    # In ra tổng số thẻ <li> tìm thấy
    print(f"Total number of <li> tags found: {len(all_li_tags)}")

    # In ra một số thẻ <li> đầu tiên để kiểm tra (tuỳ chọn)
    for li in all_li_tags[:5]:  # In ra 5 thẻ đầu tiên để kiểm tra
        print(li.prettify())
else:
    print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
