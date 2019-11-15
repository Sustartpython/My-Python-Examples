# -*- coding: utf-8 -*-
import time
import os
import concurrent.futures

import requests
import parsel


"""图片保存文件夹"""
dir_name = 'images'
if not os.path.exists(dir_name):
    os.mkdir(dir_name)
    os.chdir(dir_name)
else:
    os.chdir(dir_name)


def get_img_urls(url):
    """下载图片"""
    response = requests.get(url)
    html = response.text
    # print(html)
    """解析网页中的内容"""
    sel = parsel.Selector(html)
    divs = sel.css('.tagbqppdiv')
    for div in divs:
        link = div.css('img.ui::attr(data-original)').extract_first()
        name = div.css('a::attr(title)').extract_first()
        yield link, name


def download_img(link: str, name: str):
    """下载并保存图片"""
    try:
        suffix = link.split('.')[-1]
        response = requests.get(link)
        with open(name + '.' + suffix, mode='wb') as f:
            f.write(response.content)
    except OSError:
        print('文件名非法')


def main():
    url = 'https://fabiaoqing.com/biaoqing/lists/page/2.html'
    links = get_img_urls(url)
    for link in links:
        download_img(*link)


if __name__ == '__main__':

    executor1 = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    url = 'https://fabiaoqing.com/biaoqing/lists/page/{page}.html'
    links = get_img_urls(url)
    start_time = time.time()
    for link in links:
        executor1.submit(download_img, *link)
    executor1.shutdown()
    # main()
    print(time.time() - start_time)

