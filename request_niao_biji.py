from concurrent.futures import ThreadPoolExecutor

from pyquery import PyQuery as pq
import requests
import json
import os
import urllib.parse
import asyncio
import httpx
import time

import sqlconnect
from utils import readFile, writeFile, mkdir, listFile, writeImage

proxies = {
   'http': 'http://172.26.137.247:30000',
   'https': 'http://172.26.137.247:30000',
}

# def readFile(file):
#     res = None
#     with open(file, 'r', encoding="utf-8") as f:
#         res = json.load(f)
#     return res

# def mkdir(path):  
#     # 去除首位空格
#     path=path.strip()
#     # 去除尾部 \ 符号
#     path=path.rstrip("\\")
 
#     # 判断路径是否存在
#     # 存在     True
#     # 不存在   False
#     isExists=os.path.exists(path)
 
#     # 判断结果
#     if not isExists:
#         # 如果不存在则创建目录
#         # 创建目录操作函数
#         os.makedirs(path) 
 
#         print(path+' 创建成功')
#         return True
#     else:
#         # 如果目录存在则不创建，并提示目录已存在
#         print(path+' 目录已存在')
#         return False

# def writeFile(file, data):
#     with open(file, 'w', encoding="utf-8") as f:
#         f.write(data)      
# def writeImage(file, data):
#     with open(file, 'wb') as f:
#         f.write(data)

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


async def getUrlFromPage(url,root, sem):       
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8'
    }    
    async with sem:
        async with httpx.AsyncClient(timeout=20,verify=False) as client:
            print("request:  getPage:  {0}".format(url))
            # res = await client.get("http://www.baidu.com", headers=headers)
            res = await client.get(url, headers=headers)
            if(res.status_code != 200):
                print(res.msg)
                return []

            
            dom = pq(res.content)

            specialist = []
            propertyIndos = dom(".property-card .property-info a")
            for link_a in propertyIndos.items():
                href = root + link_a.attr("href")
                id = href[href.rfind("/")+1:href.rfind(".")]
                specialist.append(
                    {
                        "id": id,
                        "url": href,
                        "name": link_a.text()
                    }
                )
            
            
            # writeFile(file, specialist)                
            return specialist

async def getDetailFromPage(url, root,file,id, sem):       
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8'
    }    
    async with sem:
        async with httpx.AsyncClient(timeout=20,verify=False) as client:
            print("request:  getDetail:  {0}".format(url))
            # res = await client.get("http://www.baidu.com", headers=headers)
            res = await client.get(url, headers=headers)
            if(res.status_code != 200):
                print(res.msg)
                return []

            
            dom = pq(res.content)
            dom_text = dom.html()

            # get names
            name = dom(".agent-name h2").text()
            name_en = pq(dom(".agent-name")[1])("span").text()
            name_en_1 = name_en[name_en.rfind("：")+1:]
            name_en_2 = name_en[name_en.rfind(":")+1:]
            name_en = name_en_1 if name_en_1 > name_en_2 else name_en_2            
            # get alias name
            name_alia = pq(dom(".agent-name")[2])("span").text()
            name_alia_1 = name_alia[name_alia.rfind("：")+1:]
            name_alia_2 = name_alia[name_alia.rfind(":")+1:]
            name_alia = name_alia_1 if name_alia_1 > name_alia_2 else name_alia_2    

            # get 纲目科属
            name_gang = pq(dom(".agent-name")[3])("span").text()
            name_mu = pq(dom(".agent-name")[4])("span").text()
            name_ke = pq(dom(".agent-name")[5])("span").text()
            name_shu = pq(dom(".agent-name")[6])("span").text()


            # get info 
            tabPanel_0 = pq(dom(".tab-pane")[0])
            # filter with out image as child
            _p = [o for o in tabPanel_0("p") if len(pq(o)("img")) == 0]

            info_describe = _p[2].text if(len(_p) >2) else ""
            info_symbol = _p[3].text if(len(_p) >3) else ""
            info_voice = _p[4].text if(len(_p) >4) else ""
            info_area = _p[5].text if(len(_p) >5) else ""
            info_area_status = _p[6].text if(len(_p) >6) else ""
            info_habit = _p[7].text if(len(_p) >7) else ""

            info_lunaname = ""
            if(len(_p) > 8):
                for i in range(8, len(_p)):
                    info_lunaname += "\n" + _p[i].text
            # info_lunaname = _p[8].text if(len(_p) >8) else ""

            # get describes            
            describe_area = pq(dom(".tab-pane")[1])(".comment-item").text().replace("\n","").replace("\t","")
            describe_symbol = pq(dom(".tab-pane")[2])(".comment-item").text().replace("\n","").replace("\t","")
            describe_habit = pq(dom(".tab-pane")[3])(".comment-item").text().replace("\n","").replace("\t","")

            # get images
            image_head = root + dom(".agent-img img").attr("src")
            image_head = image_head[image_head.rfind("http"):]


            images = []
            dom_images = pq(dom(".tab-pane")[4])("img")
            for link_a in dom_images.items():
                url = root + link_a.attr("src")
                url = url[url.rfind("http"):]
                name = url[url.rfind("/")+1:url.rfind(".")]
                images.append({
                    "name":name,
                    "url":url
                })

            if(len(images)>0):
                pass

            data = {
                "id": id,
                "name":name,
                "name_en":name_en,
                "name_alia":name_alia,
                "name_gang":name_gang,
                "name_mu":name_mu,
                "name_ke":name_ke,
                "name_shu":name_shu,

                "info_describe":info_describe,
                "info_symbol":info_symbol,
                "info_voice":info_voice,
                "info_area":info_area,
                "info_area_status":info_area_status,
                "info_habit":info_habit,
                "info_lunaname":info_lunaname,

                "describe_area":describe_area,
                "describe_symbol":describe_symbol,
                "describe_habit":describe_habit,

                "images":images,
                "headImage":image_head,
                "html":dom_text
            }

            
            json_Str = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
            writeFile(file, json_Str)                

            return  data 

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
    structFile = "./dbStruct/db_bird_niaobiji/db.json"
    struct_raw = readFile(structFile)
    # 预处理
    # preCondutSpecies(speciesList)
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
        specItem["json"] = json.dumps(specItem, separators=(',', ':'), ensure_ascii=False)
        db.insertData(conn, "species", specItem, [o for o in struct_raw if o["tbName"] == "species"][0]["fields"])
        images = specItem["images"]
        for image in images:
            image_data = {
                "id": i_image,
                "name": image[image.rfind("/")+1:],
                "file": db.convertToBinaryData(image)
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
    resultFolder = "./result_niaobiji/"
    dbFile = os.path.join(resultFolder,"result.db") 
    allSpecialistFilePath = os.path.join(resultFolder,"itemList.json")


    root = "https://niaobiji.com/"
    catelogUrl = "https://niaobiji.com/cnniao.html?page="
    # from 1 to 34
    pageIndexes = [o for o in range(1,35)]
    

    sem = asyncio.Semaphore(1)

    print("start")
    start = time.time()    

    mkdir(resultFolder)


    task_list = []
    specialist = []
    if(isRecapture or os.path.exists(allSpecialistFilePath) is False):
        # get list from api
        print("get list from web")
        for index in pageIndexes:
            url = catelogUrl + index.__str__()
            task_list.append(getUrlFromPage(url,root,sem))
        _specialist =  await asyncio.gather(*task_list)
        for item in _specialist:
            specialist = specialist + item        

        json_str = json.dumps(specialist, separators=(',', ':'), ensure_ascii=False)
        writeFile(allSpecialistFilePath, json_str)
    else:
        # read list from file
        print("get list from file")
        specialist =  readFile(allSpecialistFilePath)

    # get detail    
    print("get details")
    task_list = []
    for item in specialist:
        id = item['id']
        name = item['name']
        url = item['url']
        file = "{0}/{1}_{2}/{1}_{2}.json".format(resultFolder,id,name)
        folder = os.path.dirname(file)
        mkdir(folder)      
        # get info
        if(isRecapture or not os.path.exists(file)):
            task = asyncio.create_task(getDetailFromPage(url,root,file,id,sem))
            task_list.append(task)
            
    await asyncio.gather(*task_list)


    # get images
    print("get images")
    task_list = []
    speciesList=[]
    dirs = os.listdir(resultFolder)
    for dir in dirs:
        folder = os.path.join(resultFolder, dir)
        listFiles = listFile(folder, ".json")
        if(listFiles is None or len(listFiles) == 0):
            print("no json file in {0}".format(folder))
            continue
            
        itemDefine = readFile(listFiles[0])
        speciesList.append(itemDefine)
        if(itemDefine is None):
            print("item define read error in {0}".format(folder))
            continue
        
        # construct need download image list
        imageList = []
        if(itemDefine['headImage'] is not None):
            imageList.append(itemDefine['headImage'])
        if(itemDefine['images'] is not None):
            for image in itemDefine['images']:
                imageList.append(image['url'])
        # solve images
        imageFiles = []
        for image in imageList:
            image_file = os.path.join(folder, image[image.rfind("/")+1:])
            imageFiles.append(image_file)
            if(isRecapture or not os.path.exists(image_file)):
                imageTask = getImage(image, image_file, sem)
                task_list.append(imageTask)
                
        itemDefine['images'] = imageFiles

    await asyncio.gather(*task_list)


    if(isUploadDatabase):
        print("save to db: {0}".format(dbFile))
        save2dDB(dbFile, speciesList)

    end = time.time()
    print("end")
    print("time: {0}".format(end - start))

if __name__ == '__main__':  
    asyncio.run(main())

    # cid = 4
    # mu = "蚓螈目"
    # file = "./result/{0}/{1}/{1}.json".format(cid, mu)
    # folder = os.path.dirname(file)
    # mkdir(folder)
    
    # getData(cid, urllib.parse.quote(mu), file)
            

