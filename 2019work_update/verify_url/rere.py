import re


s1 = 'window.location="http://www.hanspub.org/journal/SSEM.html";'

s2 = "window.location.href='CN/volumn/home.shtml'"

s3 = " window.location.href = 'http://xb.sclib.org/CN/1003-7136/home.shtml';"

pattern = re.compile(r'window.location=(.*?);')
result1 = pattern.findall(s1)
# print(result1)

pattern = re.compile(r'window.location.href=(.*)')
result2 = pattern.findall(s2)
# print(result2)

pattern = re.compile(r'window.location.href = (.*?);')
result3 = pattern.findall(s3)
print(result3[0].replace("\'",""))
