import os
filepath = r'E:\work\Meeting\asce\big_json'
for root, dirs, files in os.walk(filepath):
            for file in files:
                print(os.path.join(root, file))
