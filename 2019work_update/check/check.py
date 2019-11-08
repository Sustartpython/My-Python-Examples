from parsel import Selector
import os
import time


def GetDict(pathfile):
    print('GetDict %s ...' % pathfile)
    dic = {}
    filename = os.path.basename(pathfile)
    with open(pathfile, encoding='utf8') as f:
        text = f.read()
        html = Selector(text,'html')
        for tr in html.xpath("//div[@class='container-fluid']/span/table[2]//tr"):
            tr:Selector
            k = tr.xpath("./td[1]/text()").get('')
            v = tr.xpath("./td[2]/text()").get('')
            dic[k] = v

        for tr in html.xpath("//div[@class='container-fluid']/span/table[3]//tr"):
            tr:Selector
            k = tr.xpath("./td[1]/text()").get('')
            print('table_3 k: %s' % k)
            v = tr.xpath("./td[2]/text()").get('')
            dic[k] = v
            
    return dic


if __name__ == "__main__":
    rightDict = GetDict(r'E:\work\check\html\right.html')
    errorDict = GetDict(r'E:\work\check\html\error.html')

    r_e = rightDict.keys() - errorDict.keys()
    e_r = errorDict.keys() - rightDict.keys()
    print(r_e)
    print(e_r)
    for eKey in errorDict:
        eVal = errorDict.get(eKey, '无此项')
        rVal = rightDict.get(eKey, '无此项')
        if eVal == rVal:
            continue
        text = eKey
        text += '\n---'
        text += '\nright'
        text +=  '\n' + rVal
        text += '\n---'
        text += '\nerror'
        text +=  '\n' + eVal
        

        with open(eKey + '.txt', mode='w', encoding='utf8') as f:
            f.write(text)
