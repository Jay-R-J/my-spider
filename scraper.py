#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
终极智能爬虫 - 个人百度蜘蛛（HTML邮件美化版）
功能：
- 从种子网址开始，自动提取链接并扩散
- 遵守 robots.txt
- 限制域名范围
- 智能提取正文、结构化数据、Open Graph 元数据
- 通过美观的 HTML 邮件发送详细报告（链接可点击）
- 状态持久化，每次运行接着上次
"""

import requests
from bs4 import BeautifulSoup
import os
import time
import logging
import smtplib
from email.mime.text import MIMEText
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import re
from datetime import datetime
from typing import Set, List, Dict, Any, Optional

# 第三方库导入（带错误提示）
try:
    import trafilatura
except ImportError:
    raise ImportError("请安装 trafilatura: pip install trafilatura")

try:
    from readability import Document
except ImportError:
    raise ImportError("请安装 readability-lxml: pip install readability-lxml")

try:
    import extruct
except ImportError:
    raise ImportError("请安装 extruct: pip install extruct")

# ==================== 配置区域 ====================
# 每次运行最多抓取多少页（建议 10-20，避免超时和封 IP）
MAX_PAGES_PER_RUN: int = 15
# 请求延迟（秒），降低服务器压力
REQUEST_DELAY: int = 2
# 用户代理，表明身份
USER_AGENT: str = "Mozilla/5.0 (compatible; MyPersonalSpider/1.0; +https://github.com/Jay-R-J/my-spider)"
# 允许抓取的优质内容域名组合
ALLOWED_DOMAINS: List[str] = [
    "baike.baidu.com",      # 百度百科 - 结构化知识
    "zhidao.baidu.com",     # 百度知道 - 问答
    "tieba.baidu.com",      # 百度贴吧 - 论坛讨论
    "github.com",           # GitHub - 开源项目
    "stackoverflow.com",    # Stack Overflow - 编程问答
    "wikipedia.org",        # 维基百科 - 多语言百科
    "juejin.cn",            # 掘金 - 技术文章
]
# 种子文件路径
SEEDS_FILE: str = "seeds.txt"
# 待抓取队列文件
PENDING_FILE: str = "pending.txt"
# 已访问记录文件
VISITED_FILE: str = "visited.txt"
# 数据保存目录
DATA_DIR: str = "data"
# 日志级别
LOG_LEVEL: int = logging.INFO
# 邮件正文中每个页面的正文预览最大长度（字符数）
PREVIEW_MAX_LENGTH: int = 2000
# ==================== 邮件配置（从环境变量读取）====================
MAIL_USER: Optional[str] = os.environ.get("MAIL_USER")
MAIL_PASS: Optional[str] = os.environ.get("MAIL_PASS")
MAIL_TO: Optional[str] = os.environ.get("MAIL_TO")
# ===============================================================

# 配置日志
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')

# 全局 robot 解析器缓存
robot_parsers: Dict[str, Optional[RobotFileParser]] = {}

def get_robots_parser(domain: str) -> Optional[RobotFileParser]:
    """获取 domain 的 robots 解析器（带缓存）"""
    if domain in robot_parsers:
        return robot_parsers[domain]
    parser = RobotFileParser()
    parser.set_url(f"https://{domain}/robots.txt")
    try:
        parser.read()
        robot_parsers[domain] = parser
    except Exception as e:
        logging.warning(f"读取 {domain}/robots.txt 失败: {e}")
        robot_parsers[domain] = None
    return robot_parsers[domain]

def can_fetch(url: str, user_agent: str = USER_AGENT) -> bool:
    """检查 robots.txt 是否允许抓取该 URL"""
    domain = urlparse(url).netloc
    parser = get_robots_parser(domain)
    if parser is None:
        # 无法获取 robots，默认允许（谨慎）
        return True
    return parser.can_fetch(user_agent, url)

def load_set(filename: str) -> Set[str]:
    """从文件加载集合，每行一个元素"""
    if not os.path.exists(filename):
        return set()
    with open(filename, 'r', encoding='utf-8') as f:
        return {line.strip() for line in f if line.strip()}

def save_set(filename: str, data_set: Set[str]) -> None:
    """保存集合到文件"""
    with open(filename, 'w', encoding='utf-8') as f:
        for item in sorted(data_set):
            f.write(item + '\n')

def load_list(filename: str) -> List[str]:
    """从文件加载列表（保留顺序）"""
    if not os.path.exists(filename):
        return []
    with open(filename, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

def save_list(filename: str, data_list: List[str]) -> None:
    """保存列表到文件"""
    with open(filename, 'w', encoding='utf-8') as f:
        for item in data_list:
            f.write(item + '\n')

def is_allowed_domain(url: str) -> bool:
    """检查域名是否在 ALLOWED_DOMAINS 中"""
    if not ALLOWED_DOMAINS:
        return True
    domain = urlparse(url).netloc
    for allowed in ALLOWED_DOMAINS:
        if domain == allowed or domain.endswith('.' + allowed):
            return True
    return False

def normalize_url(url: str) -> str:
    """标准化 URL：去除 fragment，保留 scheme+netloc+path"""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

def extract_links(soup: BeautifulSoup, base_url: str) -> List[str]:
    """从 BeautifulSoup 对象中提取所有链接，返回标准化后的 URL 列表"""
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        if not href:
            continue
        absolute = urljoin(base_url, href)
        if absolute.startswith(('http://', 'https://')):
            normalized = normalize_url(absolute)
            links.append(normalized)
    return links

def extract_structured_data(html: str, url: str) -> Dict[str, Any]:
    """
    使用 extruct 提取 JSON-LD、微数据等结构化数据。
    返回包含关键信息的字典。
    """
    try:
        data = extruct.extract(html, url, uniform=True)
        summary: Dict[str, Any] = {}

        if data.get('json-ld'):
            for item in data['json-ld']:
                if isinstance(item, dict):
                    if 'name' in item:
                        summary['name'] = item['name']
                    if 'description' in item and 'description' not in summary:
                        summary['description'] = item['description']
                    if 'offers' in item:
                        offers = item['offers']
                        if isinstance(offers, dict):
                            price = offers.get('price')
                            if price:
                                summary['price'] = price
                            currency = offers.get('priceCurrency')
                            if currency:
                                summary['currency'] = currency
                        elif isinstance(offers, list) and offers:
                            first_offer = offers[0]
                            if isinstance(first_offer, dict):
                                price = first_offer.get('price')
                                if price:
                                    summary['price'] = price
                                currency = first_offer.get('priceCurrency')
                                if currency:
                                    summary['currency'] = currency
                    if 'aggregateRating' in item:
                        rating = item['aggregateRating'].get('ratingValue')
                        if rating:
                            summary['rating'] = rating
                    if 'reviewCount' in item:
                        summary['review_count'] = item['reviewCount']

        if data.get('microdata'):
            for item in data['microdata']:
                props = item.get('properties', {})
                if 'price' in props:
                    summary['price'] = props['price']
                if 'name' in props and 'name' not in summary:
                    summary['name'] = props['name']

        return summary
    except Exception as e:
        logging.warning(f"结构化数据提取失败: {e}")
        return {}

def extract_opengraph(soup: BeautifulSoup) -> Dict[str, str]:
    """提取 Open Graph 元数据"""
    og = {}
    for meta in soup.find_all('meta', property=re.compile(r'^og:')):
        prop = meta.get('property')
        content = meta.get('content')
        if prop and content:
            key = prop[3:]  # 去掉 'og:'
            og[key] = content
    return og

def extract_page_data(html: str, url: str) -> Dict[str, Any]:
    """
    综合提取页面的所有重要数据。
    """
    data: Dict[str, Any] = {
        'url': url,
        'title': '',
        'meta_description': '',
        'og': {},
        'structured': {},
        'main_text': '',
        'extraction_method': 'none'
    }

    soup = BeautifulSoup(html, 'lxml')

    # 标题
    if soup.title and soup.title.string:
        data['title'] = soup.title.string.strip()

    # Meta description
    meta_desc = soup.find('meta', attrs={'name': 'description'}) or soup.find('meta', attrs={'property': 'og:description'})
    if meta_desc and meta_desc.get('content'):
        data['meta_description'] = meta_desc['content'].strip()

    # Open Graph
    data['og'] = extract_opengraph(soup)

    # 结构化数据
    data['structured'] = extract_structured_data(html, url)

    # 主要文本提取（优先用 trafilatura）
    extracted = trafilatura.extract(html, url=url, include_comments=False, include_tables=True, include_images=False)
    if extracted:
        data['main_text'] = extracted[:PREVIEW_MAX_LENGTH] + ('...' if len(extracted) > PREVIEW_MAX_LENGTH else '')
        data['extraction_method'] = 'trafilatura'
    else:
        # 回退到 readability
        doc = Document(html)
        data['main_text'] = doc.summary()
        text_soup = BeautifulSoup(data['main_text'], 'lxml')
        data['main_text'] = text_soup.get_text(separator='\n', strip=True)[:PREVIEW_MAX_LENGTH]
        data['extraction_method'] = 'readability'
        if not data['main_text']:
            # 最后备选：简单去标签取文本
            for script in soup(["script", "style"]):
                script.decompose()
            text = soup.get_text(separator=' ', strip=True)
            text = re.sub(r'\s+', ' ', text)
            data['main_text'] = text[:PREVIEW_MAX_LENGTH]
            data['extraction_method'] = 'fallback'

    return data

def send_html_email(subject: str, body_html: str) -> None:
    """发送 HTML 格式邮件"""
    if not (MAIL_USER and MAIL_PASS and MAIL_TO):
        logging.warning("邮件配置不完整，跳过发送")
        return

    msg = MIMEText(body_html, 'html', 'utf-8')
    msg['Subject'] = subject
    msg['From'] = MAIL_USER
    msg['To'] = MAIL_TO

    try:
        server = smtplib.SMTP_SSL('smtp.qq.com', 465)
        server.login(MAIL_USER, MAIL_PASS)
        server.send_message(msg)
        server.quit()
        logging.info("HTML邮件发送成功")
    except Exception as e:
        logging.error(f"邮件发送失败: {e}")

def generate_html_report(
    pages_crawled: int,
    new_links_found: int,
    failed_urls: List[str],
    unique_pending: List[str],
    visited_count: int,
    page_details: List[Dict]
) -> str:
    """生成美观的 HTML 报告"""
    html = []
    html.append('''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 900px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            background-color: white;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            padding: 25px;
        }
        .header {
            text-align: center;
            padding-bottom: 20px;
            border-bottom: 2px solid #4CAF50;
            margin-bottom: 20px;
        }
        .header h1 {
            color: #2c3e50;
            margin: 0;
            font-size: 28px;
        }
        .header p {
            color: #7f8c8d;
            margin: 5px 0 0;
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 15px;
            margin: 25px 0;
        }
        .stat-card {
            background: #f8f9fa;
            border-radius: 8px;
            padding: 15px;
            text-align: center;
            box-shadow: 0 2px 5px rgba(0,0,0,0.05);
        }
        .stat-card .value {
            font-size: 32px;
            font-weight: bold;
            color: #2c3e50;
        }
        .stat-card .label {
            font-size: 14px;
            color: #7f8c8d;
            margin-top: 5px;
        }
        .page-card {
            background: white;
            border: 1px solid #e9ecef;
            border-radius: 10px;
            padding: 20px;
            margin: 20px 0;
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
            transition: transform 0.2s;
        }
        .page-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        }
        .page-title {
            font-size: 20px;
            font-weight: 600;
            color: #2c3e50;
            margin: 0 0 10px 0;
        }
        .page-title a {
            color: #3498db;
            text-decoration: none;
        }
        .page-title a:hover {
            text-decoration: underline;
        }
        .page-url {
            font-size: 14px;
            color: #7f8c8d;
            word-break: break-all;
            margin-bottom: 15px;
            background: #f8f9fa;
            padding: 8px 12px;
            border-radius: 5px;
            border-left: 3px solid #3498db;
        }
        .page-url a {
            color: #3498db;
            text-decoration: none;
        }
        .page-url a:hover {
            text-decoration: underline;
        }
        .summary-box {
            background: #f1f9fe;
            border-left: 4px solid #3498db;
            padding: 15px;
            border-radius: 5px;
            margin: 15px 0;
            font-size: 15px;
            color: #2c3e50;
        }
        .meta-tags {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin: 15px 0;
        }
        .meta-tag {
            background: #e9ecef;
            color: #495057;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 13px;
            font-weight: 500;
        }
        .meta-tag.blue {
            background: #d4edda;
            color: #155724;
        }
        .meta-tag.green {
            background: #d1ecf1;
            color: #0c5460;
        }
        .meta-tag.orange {
            background: #fff3cd;
            color: #856404;
        }
        .structured-data {
            background: #f8f9fa;
            border-radius: 8px;
            padding: 12px;
            margin-top: 15px;
            border: 1px dashed #adb5bd;
        }
        .structured-data h4 {
            margin: 0 0 8px 0;
            font-size: 16px;
            color: #2c3e50;
        }
        .structured-data pre {
            margin: 0;
            font-family: 'Courier New', monospace;
            font-size: 13px;
            color: #2c3e50;
            white-space: pre-wrap;
            word-wrap: break-word;
        }
        hr {
            border: 0;
            border-top: 1px solid #e9ecef;
            margin: 30px 0;
        }
        .footer {
            text-align: center;
            color: #95a5a6;
            font-size: 13px;
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #e9ecef;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🕷️ 终极智能爬虫运行报告</h1>
            <p>''' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '''</p>
        </div>
''')

    # 统计卡片
    html.append('<div class="stats-grid">')
    stats = [
        ('📄 抓取页面', pages_crawled),
        ('🔗 新发现链接', new_links_found),
        ('❌ 失败链接', len(failed_urls)),
        ('⏳ 待抓取队列', len(unique_pending)),
        ('📚 累计已抓取', visited_count)
    ]
    for label, value in stats:
        html.append(f'''
        <div class="stat-card">
            <div class="value">{value}</div>
            <div class="label">{label}</div>
        </div>
        ''')
    html.append('</div>')

    if not page_details:
        html.append('<p style="text-align:center;color:#7f8c8d;">本次运行未抓取到新页面。</p>')
    else:
        html.append(f'<h2 style="color:#2c3e50;">📌 本次抓取详情（共 {len(page_details)} 页）</h2>')

        for idx, data in enumerate(page_details, 1):
            html.append(f'<div class="page-card">')
            html.append(f'<div class="page-title">📌 {idx}. {data["title"]}</div>')
            html.append(f'<div class="page-url">🔗 <a href="{data["url"]}">{data["url"]}</a></div>')

            # 内容摘要
            if data['main_text']:
                html.append(f'<div class="summary-box"><strong>📝 内容摘要：</strong><br>{data["main_text"]}</div>')
            else:
                html.append('<div class="summary-box"><strong>📝 内容摘要：</strong> 无</div>')

            # 元数据标签
            meta_tags = []
            if data['meta_description']:
                meta_tags.append(f'<span class="meta-tag blue">📄 描述：{data["meta_description"][:100]}…</span>')

            if data['og']:
                og_title = data['og'].get('title', '')
                if og_title and og_title != data['title']:
                    meta_tags.append(f'<span class="meta-tag green">🔗 OG标题：{og_title[:50]}…</span>')
                og_desc = data['og'].get('description', '')
                if og_desc:
                    meta_tags.append(f'<span class="meta-tag green">📋 OG描述：{og_desc[:50]}…</span>')

            if data['structured']:
                struct = data['structured']
                items = []
                if 'name' in struct:
                    items.append(f"产品名:{struct['name']}")
                if 'price' in struct:
                    price = struct['price']
                    if 'currency' in struct:
                        price += f" {struct['currency']}"
                    items.append(f"💰 {price}")
                if 'rating' in struct:
                    items.append(f"⭐ {struct['rating']}")
                if 'review_count' in struct:
                    items.append(f"🗣️ {struct['review_count']}评论")
                if items:
                    meta_tags.append('<span class="meta-tag orange">📊 结构化数据：' + ' | '.join(items) + '</span>')

            if meta_tags:
                html.append('<div class="meta-tags">' + ''.join(meta_tags) + '</div>')

            # 完整结构化数据显示（可选）
            if data['structured'] and len(data['structured']) > 3:  # 如果结构化数据丰富，额外展示
                html.append('<div class="structured-data">')
                html.append('<h4>📋 详细结构化数据：</h4>')
                html.append('<pre>' + str(data['structured']) + '</pre>')
                html.append('</div>')

            html.append('</div>')

    html.append('''
        <hr>
        <div class="footer">
            <p>本报告由终极智能爬虫自动生成 · 仅供个人学习研究</p>
        </div>
    </div>
</body>
</html>''')

    return '\n'.join(html)

def scrape() -> None:
    logging.info("终极智能爬虫开始运行")

    os.makedirs(DATA_DIR, exist_ok=True)

    visited = load_set(VISITED_FILE)
    pending = load_list(PENDING_FILE)

    if not pending and os.path.exists(SEEDS_FILE):
        with open(SEEDS_FILE, 'r', encoding='utf-8') as f:
            seeds = [line.strip() for line in f if line.strip()]
        seeds = [s for s in seeds if is_allowed_domain(s)]
        pending = seeds
        logging.info(f"从种子文件加载了 {len(seeds)} 个起始网址")

    if not pending:
        logging.warning("没有待抓取网址，请检查 seeds.txt 或 pending.txt")
        return

    pages_crawled = 0
    new_links_found = 0
    failed_urls = []
    new_pending = []
    page_details = []

    while pending and pages_crawled < MAX_PAGES_PER_RUN:
        url = pending.pop(0)
        norm_url = normalize_url(url)

        if norm_url in visited:
            continue
        if not is_allowed_domain(url):
            logging.debug(f"跳过不允许的域名: {url}")
            continue
        if not can_fetch(url):
            logging.info(f"robots.txt 禁止抓取: {url}")
            visited.add(norm_url)
            continue

        logging.info(f"抓取 [{pages_crawled+1}/{MAX_PAGES_PER_RUN}]: {url}")

        try:
            headers = {'User-Agent': USER_AGENT}
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            html = resp.text

            safe_fname = re.sub(r'[^\w\-_]', '_', url)[:150] + ".html"
            filepath = os.path.join(DATA_DIR, safe_fname)
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html)

            page_data = extract_page_data(html, url)
            page_details.append(page_data)

            soup = BeautifulSoup(html, 'lxml')
            links = extract_links(soup, url)
            for link in links:
                if link not in visited and link not in new_pending:
                    if is_allowed_domain(link):
                        new_pending.append(link)
                        new_links_found += 1

            visited.add(norm_url)
            pages_crawled += 1
            time.sleep(REQUEST_DELAY)

        except Exception as e:
            logging.error(f"抓取失败 {url}: {e}")
            failed_urls.append(url)

    all_pending = pending + new_pending
    seen = set()
    unique_pending = []
    for u in all_pending:
        nu = normalize_url(u)
        if nu not in seen and nu not in visited:
            seen.add(nu)
            unique_pending.append(u)

    save_set(VISITED_FILE, visited)
    save_list(PENDING_FILE, unique_pending)

    # 生成 HTML 报告
    html_report = generate_html_report(
        pages_crawled=pages_crawled,
        new_links_found=new_links_found,
        failed_urls=failed_urls,
        unique_pending=unique_pending,
        visited_count=len(visited),
        page_details=page_details
    )

    logging.info("HTML报告生成完毕，长度：%d 字符", len(html_report))

    if page_details:
        send_html_email(f"终极爬虫简报 - {pages_crawled} 页", html_report)
    else:
        logging.info("本次无新内容，不发送邮件")

if __name__ == "__main__":
    scrape()