import json
import os

def readFile(file):
    res = None
    with open(file, 'r', encoding="utf-8") as f:
        res = json.load(f)
    return res

def mkdir(path):  
    # 去除首位空格
    path=path.strip()
    # 去除尾部 \ 符号
    path=path.rstrip("\\")
 
    # 判断路径是否存在
    # 存在     True
    # 不存在   False
    isExists=os.path.exists(path)
 
    # 判断结果
    if not isExists:
        # 如果不存在则创建目录
        # 创建目录操作函数
        os.makedirs(path) 
 
        print(path+' 创建成功')
        return True
    else:
        # 如果目录存在则不创建，并提示目录已存在
        print(path+' 目录已存在')
        return False

# 递归遍历文件夹
def listFile(path, endwith=".json"):
    res = []
    for root, dirs, files in os.walk(path):
        for file in files:
            if filter is not None:
                if(file.endswith(endwith)):
                    res.append(os.path.join(root, file))
            else:
                res.append(os.path.join(root, file))            
    return res


def writeFile(file, data):
    with open(file, 'w', encoding="utf-8") as f:
        f.write(data)      
def writeImage(file, data):
    with open(file, 'wb') as f:
        f.write(data)


