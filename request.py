from concurrent.futures import ThreadPoolExecutor
import requests
import json
import os
import urllib.parse
import asyncio
import httpx
import time

import sqlconnect

proxies = {
   'http': 'http://172.18.208.1:30000',
   'https': 'http://172.18.208.1:30000',
}

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

def writeFile(file, data):
    with open(file, 'w', encoding="utf-8") as f:
        f.write(data)      
def writeImage(file, data):
    with open(file, 'wb') as f:
        f.write(data)

async def getDetail(id,name, file, sem):
    url = 'https://api.hxsjcbs.com/index.php?s=/Protect/index/detail'
    params = "id={0}".format(id)
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8'
    }
    async with sem:
        async with httpx.AsyncClient(timeout=60) as client:
            print("request:  getDetail {0}_{1}".format(id, name))
            res = await client.post(url,data=params, headers=headers)
            res = res.json()
            if(res['code'] != 200):
                print(res.msg)
            else:
                json_str = json.dumps(res, separators=(',', ':'), ensure_ascii=False)
                writeFile(file, json_str)                
                return res['data']

async def getData(cid, mu, file, sem):    
    url = 'https://api.hxsjcbs.com/index.php?s=/Protect/index/search'
    # params = "cid=4&mu=%E8%9A%93%E8%9E%88%E7%9B%AE&protect=0&type=0".format(cid, mu)
    params = "cid={0}&mu={1}&protect=0&type=0".format(cid, urllib.parse.quote(mu))
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8'
    }


    async with sem:
        async with httpx.AsyncClient(timeout=20) as client:
            print("request:  getData {0}_{1}".format(cid, mu))
            res = await client.post(url,data=params, headers=headers)
            res = res.json()
            if(res['code'] != 200):
                print(res['msg'])
            else:
                json_str = json.dumps(res, separators=(',', ':'), ensure_ascii=False)
                writeFile(file, json_str)                
                return res['data']


    response = requests.post(url,data=params, headers=headers, verify=False, proxies=proxies)
    res = response.json()
    if(res['code'] != 200):
        print(res['msg'])
    else:
        
        json_str = json.dumps(res, separators=(',', ':'), ensure_ascii=False)
        writeFile(file, json_str)
    res['data']

async def getMu(cid, file, sem):
    url = 'https://api.hxsjcbs.com/index.php?s=/Protect/index/getMu'
    params = "cid={0}".format(cid)
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8'
    }
    async with sem:
        async with httpx.AsyncClient(timeout=20) as client:
            print("request:  getMu {0}".format(cid))
            res = await client.post(url,data=params, headers=headers)
            res = res.json()
            if(res['code'] != 200):
                print(res.msg)
            else:
                json_str = json.dumps(res, separators=(',', ':'), ensure_ascii=False)
                writeFile(file, json_str)                
                return res['data']

def resolveFileUrl(url):
    # get file name and file type from http url
    file_name = url.split('/')[-1]
    file_type = file_name.split('.')[-1]
    return file_name, file_type

async def getImage(url, file, sem):
    async with sem:
       async with httpx.AsyncClient(timeout=20) as client:
            print("request:  getImage {0} to {1}".format(url, file))
            res = await client.get(url)
            if(res):
                writeImage(file, res.content)

def getClasses(isReCapture, file):
    res = None
    isFileExists = os.path.exists(file)
    if(isReCapture or not isFileExists):
        url = 'https://api.hxsjcbs.com/index.php?s=/Protect/index/getClasses'
        params = ""
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8'
        }
        response = requests.post(url,data=params, headers=headers, verify=False, proxies=proxies)
        res = response.json()
        if(res['code'] != 200):
            print(res.msg)
        else:
            json_str = json.dumps(res, separators=(',', ':'), ensure_ascii=False)
            writeFile(file, json_str)
            res = res
    else:
        with open(file, 'r', encoding="utf-8") as f:
            res = json.load(f)

    return res['data']


# def getDBStruct(structFile):
#     struct_raw = readFile(structFile)


def preCondutSpecies(speciesList):

    for item in speciesList:
        try:
            item["json"] = json.dumps(item, separators=(',', ':'), ensure_ascii=False)
            category_chain_raw = item["category_chain"]
            category_chain_raw = [o for o in category_chain_raw if o] # remove none
            item["category_chain"] = {}
            # yu
            d = category_chain_raw is None and [] or [o['title'] for o  in category_chain_raw if o["level"] == "yu"]
            item["category_chain"]["yu"] = len(d) > 0 and d[0] or ""
            #jie
            d = category_chain_raw is None and [] or [o['title'] for o  in category_chain_raw if o["level"] == "jie"]
            item["category_chain"]["jie"] = len(d) > 0 and d[0] or ""
            # men
            d = category_chain_raw is None and [] or  [o['title'] for o  in category_chain_raw if o["level"] == "men"]
            item["category_chain"]["men"] = len(d) > 0 and d[0] or ""
            # gang
            d = category_chain_raw is None and [] or  [o['title'] for o  in category_chain_raw if o["level"] == "gang"]
            item["category_chain"]["gang"] = len(d) > 0 and d[0] or ""        
            # mu
            d = category_chain_raw is None and [] or  [o['title'] for o  in category_chain_raw if o["level"] == "mu"]
            item["category_chain"]["mu"] = len(d) > 0 and d[0] or ""        
            # ke
            d = category_chain_raw is None and [] or  [o['title'] for o  in category_chain_raw if o["level"] == "ke"]
            item["category_chain"]["ke"] = len(d) > 0 and d[0] or ""        
        except Exception as e:
            print(item)




def save2dDB(dbFile, speciesList):
    structFile = "./db.json"
    struct_raw = readFile(structFile)
    # 预处理
    preCondutSpecies(speciesList)
    db = sqlconnect.database()
    conn = db.connect(dbFile)
    # with db.connect(dbFile) as conn:
        # create table
    for struct in struct_raw:
        db.createTable(conn, struct["tbName"], struct["fields"])     
    conn.commit()

    # insert data
    # insert species
    i_spec = 0
    i_image = 0
    i_spec_image = 0
    for specItem in speciesList:
        specItem['cid'] = i_spec
        db.insertData(conn, "species", specItem, [o for o in struct_raw if o["tbName"] == "species"][0]["fields"])
        images = specItem["pictures"]
        for image in images:
            image_data = {
                "id": i_image,
                "name": image['name'],
                "file": db.convertToBinaryData(image["img_file"])
            }
            db.insertData(conn, "images", image_data, [o for o in struct_raw if o["tbName"] == "images"][0]["fields"])
            spec_image_data = {
                "id": i_spec_image,
                "species_id": i_spec,
                "image_id": i_image
            }
            db.insertData(conn, "species_images", spec_image_data, [o for o in struct_raw if o["tbName"] == "species_images"][0]["fields"])
            i_spec_image += 1
            i_image += 1
        if(i_spec % 100 == 0):
            print("insert species {0}".format(i_spec))
            conn.commit()
        i_spec += 1    

        # insert images
    conn.commit()
    conn.close()

async def main():
    isRecapture = False
    isUploadDatabase = True
    dbFile = "./result/result.db"

    sem = asyncio.Semaphore(1)

    print("start")
    start = time.time()
    # get all calss
    print("get all calss")
    classes = getClasses(isRecapture, "./result/classes.json")

    # get all mu
    print("get all mu")
    task_list = []
    for class_item in classes:
        cid = class_item['id']
        file = "./result/{0}_{1}/{0}_{1}.json".format(cid, class_item['title'])
        folder = os.path.dirname(file)
        mkdir(folder)
        image_folder = folder + "/_images"
        fileName,fileType = resolveFileUrl(class_item['url'])
        image_file = image_folder + "/{0}_{1}".format(class_item['title'], fileName)
        mkdir(image_folder)
        # download image
        if(isRecapture or not os.path.exists(image_file)):
            imageTask = getImage(class_item['url'], image_file, sem)
            task_list.append(imageTask)
        # get mu
        if(isRecapture or not os.path.exists(file)):
            task = asyncio.create_task(getMu(cid,file, sem))
            task_list.append(task)
            
    await asyncio.gather(*task_list)


    # get all data
    mu_file_list = []
    task_list = []
    print("get all data")
    for class_item in classes:
        cid = class_item['id']
        mufile = "./result/{0}_{1}/{0}_{1}.json".format(cid, class_item['title'])
        mu = readFile(mufile)
        for mu_item in mu['data']:
            file = "./result/{0}_{1}/{2}/{1}_{2}.json".format(cid, class_item['title'], mu_item)
            mu_file_list.append(file)
            folder = os.path.dirname(file)         
            mkdir(folder)          
            if(isRecapture or not os.path.exists(file)):
                task = asyncio.create_task(getData(cid, mu_item, file, sem))
                task_list.append(task)
       
    await asyncio.gather(*task_list)


    # get all detail
    task_list = []
    detail_file_list = []
    print("get all detail")
    for file in mu_file_list:
        data = readFile(file)
        for item in data['data']:
            baseFolder = os.path.dirname(file)
            detailFile = "{0}/{2}/{1}_{2}.json".format(baseFolder, item['id'], item['name'])
            folder = os.path.dirname(detailFile)
            detail_file_list.append(detailFile)
            mkdir(folder)

            image_folder = folder + "/_images"
            fileName,fileType = resolveFileUrl(item['headimg'])
            image_file = image_folder + "/{0}_{1}".format(item['name'], fileName)
            mkdir(image_folder)
            # download image
            if(isRecapture or not os.path.exists(image_file)):
                imageTask = getImage(item['headimg'], image_file, sem)
                task_list.append(imageTask)
            
            if(isRecapture or not os.path.exists(detailFile)):
                task = asyncio.create_task(getDetail(item['id'],item['name'],detailFile, sem))
                task_list.append(task)

    await asyncio.gather(*task_list)


    # download detail image
    task_list = []
    speciesList = []
    for f in detail_file_list:
        detailItem = readFile(f)['data']
        folder = os.path.dirname(f)
        image_folder = folder + "/_images"
        mkdir(image_folder)
        for picItem in detailItem['pictures']:
            fileName,fileType = resolveFileUrl(picItem["img"])
            image_file = image_folder + "/{0}_{1}".format(detailItem['name']['cn'], fileName)
            picItem['img_file'] = image_file
            picItem['name'] = "{0}_{1}".format(detailItem['name']['cn'], fileName)
            # download image            
            if(isRecapture or not os.path.exists(image_file)):
                imageTask = getImage(picItem['img'], image_file, sem)
                task_list.append(imageTask)

        speciesList.append(detailItem)

    await asyncio.gather(*task_list)


    end = time.time()
    print("time: {0}".format(end - start))

    if(isUploadDatabase):
        save2dDB(dbFile, speciesList)


if __name__ == '__main__':  
    asyncio.run(main())

    # cid = 4
    # mu = "蚓螈目"
    # file = "./result/{0}/{1}/{1}.json".format(cid, mu)
    # folder = os.path.dirname(file)
    # mkdir(folder)
    
    # getData(cid, urllib.parse.quote(mu), file)
            

