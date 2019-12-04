import requests
import json
def msg2weixin(msg):
        Headers = {
        'Accept':
            '*/*',
        'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
    }

        corpid = r'wwa7df1454d730c823'
        corpsecret = r'dDAusBg3gK7hKhLfqIRlyp84UDtII6NkMW7s8Wn2wgs'
        url = r'https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=%s&corpsecret=%s' % (corpid, corpsecret)
        count = 0
        while count < 3:
            try:
                r = requests.get(url)
                content = r.content.decode('utf8')
                dic = json.loads(content)
                accessToken = dic['access_token']
                url = r'https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token=%s' % accessToken
                form = {"touser": 'suhong', "msgtype": "text", "agentid": 1000015, "text": {"content": msg}, "safe": 0}

                r = requests.post(url=url, data=json.dumps(form), headers=Headers, timeout=30)
                break
            except:
                count += 1
                print('发送消息到企业微信失败')
msg = '123'
msg2weixin(msg)