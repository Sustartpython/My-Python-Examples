import os
import sys
import re
from bs4 import BeautifulSoup
import utils
import traceback
import time


def downcover():
    url = 'https://aip.scitation.org'
    cover_dir_fullpath = os.path.dirname(os.path.abspath(__file__)) + '/cover'
    if not os.path.exists(cover_dir_fullpath):
        os.makedirs(cover_dir_fullpath)
    try:
        resp = utils.get_html(url)
    except:
        # exMsg = '* ' + traceback.format_exc()
        # print(exMsg)
        return False
    if not resp:
        return False
    if resp.text.find('</html>') < 0:
        return False
    soup = BeautifulSoup(resp.content.decode('utf8'), 'lxml')
    divList = soup.select('div.publicationCoverImage')
    # divpb = soup.select_one('div', data - widget - id='bfd39502-c303-4169-88ba-1d2b9bba85ab')
    for divtag in divList:
        coverurl = url + divtag.a.img['src']
        covername = cover_dir_fullpath + '/' + divtag.a['href'].split('/')[-1].lower() + '.jpg'
        if os.path.exists(covername):
            continue
        resp = utils.get_html(coverurl)
        if utils.Img2Jpg(resp.content, covername):
            utils.printf('下载', covername, '成功...')
            time.sleep(3)
    # apburl = 'https://aip.scitation.org/pb-assets/images/publications/apb/apl-bioeng-1483023557097.jpg'
    # apbname = cover_dir_fullpath + '/' + 'apb.jpg'
    # resp = utils.get_html(apburl)
    # if utils.Img2Jpg(resp.content, apbname):
    #     utils.printf('下载', apbname, '成功...')

    return True


def mapcover():
    nCount = 0
    provider = 'aipjournal'
    filePath = provider + '_cover.txt'
    sPath = os.path.join(os.path.abspath('.'), 'cover')
    print('path:' + sPath)

    with open(filePath, mode='w', encoding='utf-8') as f:
        for path, dirNames, fileNames in os.walk(sPath):
            for fileName in fileNames:
                journal = os.path.splitext(fileName)[0]
                line = provider + '@' + journal + '★/smartlib/' + provider + '/' + fileName + '\n'
                f.write(line)
                nCount += 1
        dic = {'phy': '/jap.jpg', 'pfl': '/phf.jpg', 'pfa': '/phf.jpg', 'pfb': '/php.jpg'}
        for key, value in dic.items():
            line = provider + '@' + key + '★/smartlib/' + provider + value + '\n'
            f.write(line)
            nCount += 1
    print('nCount:' + str(nCount))


if __name__ == '__main__':
    downcover()