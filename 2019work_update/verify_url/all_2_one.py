import datetime
import os
import random

output = r'E:\work\verify_url\last'
path = r'E:\work\verify_url\new_do.txt'


for root, dirs, files in os.walk(output):
    for file in files:
        filename = os.path.join(root, file)
        print(filename)
        with open(filename, mode='r', encoding="utf-8") as fp:
            text = fp.readline()
            while text:
                if text == "\n":
                    text = fp.readline()
                    continue
                with open(path, mode='a', encoding="utf-8") as f:
                    f.write(text)
                text = fp.readline()

