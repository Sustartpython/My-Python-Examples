import os
def file_list(filepath):
    """
    文件夹的遍历
    Arguments:
        filepath {string} -- 需要遍历的文件夹
    Yields:
        string,string -- 返回文件名跟文件绝对目录
    """

    for root, dirs, files in os.walk(filepath):
        for file in files:
            # yield file, os.path.join(root, file)
            fname = os.path.join(root, file)
            print(fname)

if __name__ == "__main__":
    path = r'E:\更新数据'
    file_list(path)