#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
终极智能爬虫 - 个人百度蜘蛛
功能：
- 从种子网址开始，自动提取链接并扩散
- 遵守 robots.txt
- 限制域名范围
- 智能提取正文、结构化数据、Open Graph 元数据
- 通过邮件发送详细报告
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
import trafilatura
from readability import Document
import extruct
import json

# ==================== 配置区域 ====================
# 每次运行最多抓取多少页（建议 10-20，避免超时和封 IP）
MAX_PAGES_PER_RUN = 15
# 请求延迟（秒），降低服务器压力
REQUEST_DELAY = 2
# 用户代理，表明身份
USER_AGENT = "Mozilla/5.0 (compatible; MyPersonalSpider/1.0; +https://github.com/Jay-R-J/my-spider)"
# 允许抓取的优质内容域名组合
ALLOWED_DOMAINS = [
    "baike.baidu.com",      # 百度百科 - 结构化知识
    "zhidao.baidu.com",     # 百度知道 - 问答
    "tieba.baidu.com",      # 百度贴吧 - 论坛讨论
    "github.com",           # GitHub - 开源项目
    "stackoverflow.com",    # Stack Overflow - 编程问答
    "wikipedia.org",        # 维基百科 - 多语言百科
    "juejin.cn",            # 掘金 - 技术文章
]
# 种子文件路径
SEEDS_FILE = "seeds.txt"
# 待抓取队列文件
PENDING_FILE = "pending.txt"
# 已访问记录文件
VISITED_FILE = "visited.txt"
# 数据保存目录
DATA_DIR = "data"
# 日志级别
LOG_LEVEL = logging.INFO
# 邮件正文中每个页面的正文预览最大长度（字符数）
PREVIEW_MAX_LENGTH = 2000
# 结构化数据字段最大展示行数（暂未使用，但可扩展）
MAX_STRUCTURED_ITEMS = 5
# ==================== 邮件配置（从环境变量读取）====================
MAIL_USER = os.environ.get("MAIL_USER")
MAIL_PASS = os.environ.get("MAIL_PASS")
MAIL_TO = os.environ.get("MAIL_TO")
# ===============================================================

# 配置日志
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')

# 全局 robot 解析器缓存
robot_parsers = {}

def get_robots_parser(domain):
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
        parser = None
    return parser

def can_fetch(url, user_agent=USER_AGENT):
    """检查 robots.txt 是否允许抓取该 URL"""
    domain = urlparse(url).netloc
    parser = get_robots_parser(domain)
    if parser is None:
        # 无法获取 robots，默认允许（谨慎）
        return True
    return parser.can_fetch(user_agent, url)

def load_set(filename):
    """从文件加载集合，每行一个元素"""
    if not os.path.exists(filename):
        return set()
    with open(filename, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f if line.strip())

def save_set(filename, data_set):
    """保存集合到文件"""
    with open(filename, 'w', encoding='utf-8') as f:
        for item in sorted(data_set):
            f.write(item + '\n')

def load_list(filename):
    """从文件加载列表（保留顺序）"""
    if not os.path.exists(filename):
        return []
    with open(filename, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

def save_list(filename, data_list):
    """保存列表到文件"""
    with open(filename, 'w', encoding='utf-8') as f:
        for item in data_list:
            f.write(item + '\n')

def is_allowed_domain(url):
    """检查域名是否在 ALLOWED_DOMAINS 中"""
    if not ALLOWED_DOMAINS:
        return True
    domain = urlparse(url).netloc
    for allowed in ALLOWED_DOMAINS:
        if domain == allowed or domain.endswith('.' + allowed):
            return True
    return False

def normalize_url(url):
    """标准化 URL：去除 fragment，保留 scheme+netloc+path"""
    parsed = urlparse(url)
    # 只保留 scheme, netloc, path
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

def extract_links(soup, base_url):
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

def extract_structured_data(html, url):
    """
    使用 extruct 提取 JSON-LD、微数据、RDFa 等结构化数据。
    返回一个字典，包含提取到的关键信息（如名称、价格、评分等）。
    """
    try:
        data = extruct.extract(html, url, uniform=True)
        summary = {}
        
        # 提取 JSON-LD
        if data.get('json-ld'):
            for item in data['json-ld']:
                if isinstance(item, dict):
                    # 尝试提取常见的产品/文章字段
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
                            # 取第一个 offer
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
        
        # 提取微数据
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

def extract_opengraph(soup):
    """提取 Open Graph 元数据"""
    og = {}
    for meta in soup.find_all('meta', property=re.compile(r'^og:')):
        prop = meta.get('property')
        content = meta.get('content')
        if prop and content:
            key = prop[3:]  # 去掉 'og:'
            og[key] = content
    return og

def extract_page_data(html, url):
    """
    综合提取页面的所有重要数据：
    - 使用 trafilatura 提取主要文本
    - 使用 readability 作为备选
    - 提取 Open Graph 元数据
    - 提取结构化数据
    - 提取 meta description
    """
    data = {
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
        # 清理 readability 返回的 HTML 标签，变成纯文本
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

def send_email(subject, body):
    """通过 QQ 邮箱发送邮件"""
    if not (MAIL_USER and MAIL_PASS and MAIL_TO):
        logging.warning("邮件配置不完整，跳过发送")
        return

    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = subject
    msg['From'] = MAIL_USER
    msg['To'] = MAIL_TO

    try:
        # QQ 邮箱 SMTP 服务器
        server = smtplib.SMTP_SSL('smtp.qq.com', 465)
        server.login(MAIL_USER, MAIL_PASS)
        server.send_message(msg)
        server.quit()
        logging.info("邮件发送成功")
    except Exception as e:
        logging.error(f"邮件发送失败: {e}")

def scrape():
    logging.info("终极智能爬虫开始运行")

    # 创建数据目录
    os.makedirs(DATA_DIR, exist_ok=True)

    # 加载状态
    visited = load_set(VISITED_FILE)
    pending = load_list(PENDING_FILE)

    # 如果 pending 为空，从种子文件初始化
    if not pending and os.path.exists(SEEDS_FILE):
        with open(SEEDS_FILE, 'r', encoding='utf-8') as f:
            seeds = [line.strip() for line in f if line.strip()]
        # 只保留允许域名的种子
        seeds = [s for s in seeds if is_allowed_domain(s)]
        pending = seeds
        logging.info(f"从种子文件加载了 {len(seeds)} 个起始网址")

    if not pending:
        logging.warning("没有待抓取网址，请检查 seeds.txt 或 pending.txt")
        return

    pages_crawled = 0
    new_links_found = 0
    failed_urls = []
    new_pending = []          # 临时存放新发现的链接
    page_details = []          # 存储本次抓取页面的详细数据

    while pending and pages_crawled < MAX_PAGES_PER_RUN:
        url = pending.pop(0)
        norm_url = normalize_url(url)

        # 已访问检查
        if norm_url in visited:
            continue

        # 域名检查
        if not is_allowed_domain(url):
            logging.debug(f"跳过不允许的域名: {url}")
            continue

        # robots.txt 检查
        if not can_fetch(url):
            logging.info(f"robots.txt 禁止抓取: {url}")
            # 我们仍然记录为已访问，避免重复检查
            visited.add(norm_url)
            continue

        logging.info(f"抓取 [{pages_crawled+1}/{MAX_PAGES_PER_RUN}]: {url}")

        try:
            headers = {'User-Agent': USER_AGENT}
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            html = resp.text

            # 保存原始 HTML（可选）
            safe_fname = re.sub(r'[^\w\-_]', '_', url)[:150] + ".html"
            filepath = os.path.join(DATA_DIR, safe_fname)
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html)

            # 提取页面数据
            page_data = extract_page_data(html, url)
            page_details.append(page_data)

            # 提取链接
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

    # 合并剩余 pending 和新链接
    all_pending = pending + new_pending
    # 简单去重（保持顺序）
    seen = set()
    unique_pending = []
    for u in all_pending:
        nu = normalize_url(u)
        if nu not in seen and nu not in visited:
            seen.add(nu)
            unique_pending.append(u)

    # 保存状态
    save_set(VISITED_FILE, visited)
    save_list(PENDING_FILE, unique_pending)

    # 生成运行报告（丰富版）
    report_lines = []
    report_lines.append("终极智能爬虫运行报告")
    report_lines.append(f"运行时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"本次抓取页面数：{pages_crawled}")
    report_lines.append(f"新发现链接数：{new_links_found}")
    report_lines.append(f"失败链接数：{len(failed_urls)}")
    report_lines.append(f"当前待抓取队列长度：{len(unique_pending)}")
    report_lines.append(f"累计已抓取页面：{len(visited)}")
    report_lines.append("")
    report_lines.append("=" * 60)
    report_lines.append("本次抓取页面详情：")

    for idx, data in enumerate(page_details, 1):
        report_lines.append(f"\n--- 页面 {idx} ---")
        report_lines.append(f"标题：{data['title']}")
        report_lines.append(f"URL：{data['url']}")
        report_lines.append(f"描述：{data['meta_description']}")
        
        # 显示 Open Graph 关键信息
        if data['og']:
            og_items = []
            if 'title' in data['og'] and data['og']['title'] != data['title']:
                og_items.append(f"OG标题：{data['og']['title']}")
            if 'description' in data['og']:
                og_items.append(f"OG描述：{data['og']['description']}")
            if 'image' in data['og']:
                og_items.append(f"OG图片：{data['og']['image']}")
            if og_items:
                report_lines.append("社交元数据：")
                report_lines.extend(og_items)
        
        # 显示结构化数据（如价格、评分）
        if data['structured']:
            struct_items = []
            if 'name' in data['structured']:
                struct_items.append(f"产品名：{data['structured']['name']}")
            if 'price' in data['structured']:
                price_str = data['structured']['price']
                if 'currency' in data['structured']:
                    price_str += f" {data['structured']['currency']}"
                struct_items.append(f"价格：{price_str}")
            if 'rating' in data['structured']:
                struct_items.append(f"评分：{data['structured']['rating']}")
            if 'review_count' in data['structured']:
                struct_items.append(f"评论数：{data['structured']['review_count']}")
            if struct_items:
                report_lines.append("结构化数据：")
                report_lines.extend(struct_items)
        
        # 显示正文预览
        report_lines.append(f"正文预览（提取方式：{data['extraction_method']}）：")
        report_lines.append(data['main_text'])

    report = "\n".join(report_lines)
    logging.info(report)

    # 发送邮件
    if page_details:
        send_email(f"终极爬虫简报 - {pages_crawled} 页", report)
    else:
        logging.info("本次无新内容，不发送邮件")

if __name__ == "__main__":

    scrape()
