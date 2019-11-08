import time
import os


class all_2_one(object):
    def __init__(self):
        # 小文件存储路径
        self.pwd = r'E:\work\Meeting\asce\big_json\20190817'
        self.merge_big_jsonPath = r'E:\work\Meeting\asce\merge_big_json'

    def all_2_one_1(self):
        begin_time = time.time()
        print(begin_time)
        new_file = self.merge_big_jsonPath + '/' + "1.big_json"
        for root, dirs, files in os.walk(self.pwd):
            for file in files:
                filename = os.path.join(root, file)
                with open(filename, mode='r', encoding="utf-8") as fp:
                    text = fp.readline()
                    while text:
                        with open(new_file, mode='a', encoding="utf-8") as f:
                            f.write(text)
                        text = fp.readline()
        end_time = time.time()
        print(end_time)
        print("每次打开写入花费时间为:%s s" % (end_time-begin_time))
        

    def all_2_one_2(self):
        begin_time=time.time()
        print(begin_time)
        new_file=self.merge_big_jsonPath + '/' + "2.big_json"
        fw=open(new_file, mode='a', encoding="utf-8")
        for root, dirs, files in os.walk(self.pwd):
            for file in files:
                filename=os.path.join(root, file)
                with open(filename, mode='r', encoding="utf-8") as fp:
                    text=fp.readline()
                    while text:
                        fw.write(text)
                        text=fp.readline()
        end_time=time.time()
        print(end_time)
        print("打开一次写入花费时间为:%s s" % (end_time-begin_time))
        

if __name__ == "__main__":
    t=all_2_one()
    t.all_2_one_1()
    t.all_2_one_2()
