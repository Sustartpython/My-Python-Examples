import toml
import os
from parsel import Selector

kong_count = 0 
count = 0
listPath = toml.load('config.toml')['listPath']
fw = open(r"E:\work\Meeting\aiaa\eisbn.txt",'a', encoding='utf-8')

for parent, dirnames, filenames in os.walk(listPath, followlinks=True):
        for filename in filenames:
            file_path = os.path.join(parent, filename)
            print(file_path)
            with open(file_path, encoding='utf8') as f:
                text = f.read()
            html = Selector(text, type='html')
            title = html.xpath("//h5[@class='teaser__group-title']/text()").getall()[0]
            eISBN = html.xpath("//div[@class='teaser__row'][2]/div[@class='teaser__item']/text()").getall()
            print(eISBN)
            if eISBN != []:
                if "eISBN" in eISBN[0]:
                    line = title + "\t" + eISBN[0] + "\n"
                    fw.write(line)
                else:
                    count += 1
            else:
                line = title + "\t" + "\n"
                fw.write(line)
                kong_count += 1

print("没有eISBN的个数："+str(count))
print("空eISBN的个数："+str(kong_count))
                