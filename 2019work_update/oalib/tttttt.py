import toml

TXTPATH = toml.load('config.toml')['txtpath']

with open(TXTPATH,'w',encoding='utf-8') as f:
    f.writelines("1" + "\n")
    f.writelines("2")