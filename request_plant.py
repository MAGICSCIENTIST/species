from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import math
import requests
import json
import os
import urllib.parse
import asyncio
import httpx
import time

import sqlconnect

cookies = {
    'PHPSESSID': 'l8qkep2roid2rifb7tjh0dtho7'
}
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

# 获取门列表
# Phylum, Order, Family, Genus and Species
# https://www.cvh.ac.cn/controller/species/tree.php?_=
def getOrderist(isReCapture, file):
    res = None
    isFileExists = os.path.exists(file)
    if(isReCapture or not isFileExists):
        url = 'https://www.cvh.ac.cn/controller/species/tree.php?_=' + math.floor(datetime.now().timestamp()*1000).__str__()
        params = ""
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8',
            'Referer': 'https://www.cvh.ac.cn/species/taxon_tree.php'
        }
        # s = requests.session()
        # s.headers.update(headers)
        response = requests.get(url,data=params, headers=headers, verify=False, cookies=cookies)
        res = response.json()
        if(res is None):
            print("get order list error")
        else:
            json_str = json.dumps(res, separators=(',', ':'), ensure_ascii=False)
            writeFile(file, json_str)
            res = res
    else:
        with open(file, 'r', encoding="utf-8") as f:
            res = json.load(f)

    return res
    
# 获取科列表
# https://www.cvh.ac.cn/controller/species/tree_lazyload.php?type=phy
async def getFamilies(param, file, sem):
    url = "https://www.cvh.ac.cn/controller/species/tree_lazyload.php?type=phy&param={0}".format(param)    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8',
        'Referer': 'https://www.cvh.ac.cn/species/taxon_tree.php'
    }
    async with sem:
        async with httpx.AsyncClient(timeout=20) as client:
            print("request:  getFamily within {0}".format(param))
            res = await client.post(url, headers=headers, cookies=cookies )
            res = res.json()
            if(res is None or len(res) == 0):
                print("get families {0} error".format(param))
            else:
                json_str = json.dumps(res, separators=(',', ':'), ensure_ascii=False)
                writeFile(file, json_str)                
                return res
# 获取属列表
# https://www.cvh.ac.cn/controller/species/tree_lazyload.php?type=fam
async def getGenuses(param, file, sem):
    url = "https://www.cvh.ac.cn/controller/species/tree_lazyload.php?type=fam&param={0}".format(param) 
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8',
        'Referer': 'https://www.cvh.ac.cn/species/taxon_tree.php'
    }
    async with sem:
        async with httpx.AsyncClient(timeout=20, verify=False) as client:
            print("request:  getGenuses within {0}".format(param))
            res = await client.post(url, headers=headers, cookies=cookies, )
            res = res.json()
            if(res is None or len(res) == 0):
                print("get getGenuses {0} error".format(param))
            else:
                json_str = json.dumps(res, separators=(',', ':'), ensure_ascii=False)
                writeFile(file, json_str)                
                return res
            
# 获取种表
# https://www.cvh.ac.cn/controller/species/tree_lazyload.php?type=gen
async def getSpecieses(param, name_zhcn, file, sem):
    url = "https://www.cvh.ac.cn/controller/species/tree_lazyload.php?type=gen&param={0}".format(param)
    params = "param={0}".format(param)
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8',
        'Referer': 'https://www.cvh.ac.cn/species/taxon_tree.php'
    }
    async with sem:
        async with httpx.AsyncClient(timeout=20, verify=False) as client:
            print("request:  Specieses within {0} {1}".format(param, name_zhcn))
            res = await client.post(url, headers=headers, cookies=cookies)
            res = res.json()
            if(res is None or len(res) == 0):
                print("get Specieses {0} {1} error".format(param, name_zhcn))
            else:
                json_str = json.dumps(res, separators=(',', ':'), ensure_ascii=False)
                writeFile(file, json_str)                
                return res
            
# 获取物种的详细信息
async def getSpecieInfo(param, file, defineObjNeedWriteForError, sem):
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8',
        'Referer': 'https://www.cvh.ac.cn/species/taxon_tree.php?type=sp&param={0}'.format(urllib.parse.quote(param['name']))
    }        
    url_info = 'https://www.cvh.ac.cn/controller/species/species_info.php?spname={0}'.format(urllib.parse.quote(param['name']))    

    url_trait = 'https://www.cvh.ac.cn/controller/species/trait_info.php'
    params_trait = 'canonicalName={0}'.format(param['name'])

    url_species = 'https://www.cvh.ac.cn/controller/species/species_spm.php'
    params_species = 'spname={0}'.format(param['name'])

    url_living_photo = 'https://www.cvh.ac.cn/controller/species/living_photo.php'
    params_living_photo = 'canonicalName={0}&chineseName={1}'.format(param['name'], param['name_zhcn'])

    async with sem:
        async with httpx.AsyncClient(timeout=20, verify=False, default_encoding='utf-8') as client:            
            print("request: detail info {0} {1}".format(param['name'], param['name_zhcn']))
            res_info = await client.get(url_info, headers=headers, cookies=cookies)
            res_info = res_info.json()

            res_trait = await client.post(url_trait,data=params_trait, headers=headers, cookies=cookies)
            res_trait = res_trait.json()

            res_species = await client.post(url_species,data=params_species, headers=headers, cookies=cookies)
            res_species = res_species.json()

            res_living_photo = await client.post(url_living_photo,data=params_living_photo, headers=headers, cookies=cookies)
            res_living_photo = res_living_photo.json()

            res = {}
            res['status'] = True
            res['info'] = res_info["info"] if 'info' in res_info else None
            res['trait'] = res_trait["info"] if isinstance(res_trait, dict) and ("info" in res_trait) else None
            res['species'] = res_species
            res['pictures'] = res_living_photo["info"] if ("info" in res_living_photo) else []

            if(res['info'] is None):
                print("error: get Specieses {0} {1} error".format(param['name'], param['name_zhcn']))
                errorMsg = {}
                errorMsg['status'] = False
                errorMsg['data'] = defineObjNeedWriteForError
                return errorMsg
                # write error species into file
            else:
                json_str = json.dumps(res, separators=(',', ':'), ensure_ascii=False)
                writeFile(file, json_str)                
                return res
            
def resolveFileUrl(url):
    # get file name and file type from http url
    file_name = url.split('/')[-1]
    file_type = file_name.split('.')[-1]
    return file_name, file_type

async def getImage(url, file, sem):
    async with sem:
       async with httpx.AsyncClient(timeout=20, verify=False) as client:
            print("request:  getImage {0} to {1}".format(url, file))
            res = await client.get(url)
            if(res):
                try:
                    writeImage(file, res.content)                     
                except Exception as e:
                    print('error: download image read file {0} error'.format(file))
                    


def preCondutSpecies(speciesList):
    pass
    # for item in speciesList:
    #     try:
    #         item["json"] = json.dumps(item, separators=(',', ':'), ensure_ascii=False)
    #         category_chain_raw = item["category_chain"]
    #         category_chain_raw = [o for o in category_chain_raw if o] # remove none
    #         item["category_chain"] = {}
    #         # yu
    #         d = category_chain_raw is None and [] or [o['title'] for o  in category_chain_raw if o["level"] == "yu"]
    #         item["category_chain"]["yu"] = len(d) > 0 and d[0] or ""
    #         #jie
    #         d = category_chain_raw is None and [] or [o['title'] for o  in category_chain_raw if o["level"] == "jie"]
    #         item["category_chain"]["jie"] = len(d) > 0 and d[0] or ""
    #         # men
    #         d = category_chain_raw is None and [] or  [o['title'] for o  in category_chain_raw if o["level"] == "men"]
    #         item["category_chain"]["men"] = len(d) > 0 and d[0] or ""
    #         # gang
    #         d = category_chain_raw is None and [] or  [o['title'] for o  in category_chain_raw if o["level"] == "gang"]
    #         item["category_chain"]["gang"] = len(d) > 0 and d[0] or ""        
    #         # mu
    #         d = category_chain_raw is None and [] or  [o['title'] for o  in category_chain_raw if o["level"] == "mu"]
    #         item["category_chain"]["mu"] = len(d) > 0 and d[0] or ""        
    #         # ke
    #         d = category_chain_raw is None and [] or  [o['title'] for o  in category_chain_raw if o["level"] == "ke"]
    #         item["category_chain"]["ke"] = len(d) > 0 and d[0] or ""        
    #     except Exception as e:
    #         print(item)

def save2dDB(dbFile, speciesList):
    structFile = "./db_plant.json"
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
            try:
                file = db.convertToBinaryData(image["img_file"])
            except Exception as e:
                print("error: convert image {0} to binary error".format(image["img_file"]))
                continue
            image_data = {
                "id": i_image,
                "name": image['name'],
                "file": file
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
    floder_base = "./result_plant"
    errorSpeciesFile = "{0}/error_species.json".format(floder_base)
    mkdir(floder_base)
    dbFile = "./{0}/result_plant.db".format(floder_base)
    
    sem = asyncio.Semaphore(1)

    print("start")
    start = time.time()
    # get all calss
    print("get all 门")
    orderList = getOrderist(isRecapture, "./{0}/orderlist.json".format(floder_base))

    # get all family
    print("get all family")
    task_list = []
    familyFileList = []
    for order in orderList:
        name = order['param']
        name_zhcn = order['text']
        file = "{0}/{2}/{2}.json".format(floder_base, name, name_zhcn)
        familyFileList.append(file)
        folder = os.path.dirname(file)
        mkdir(folder)
        if(isRecapture or not os.path.exists(file)):
            getFamilyTask = getFamilies(name, file, sem)
            task_list.append(getFamilyTask)

    await asyncio.gather(*task_list)


    # get all genus
    print("get all genus")
    genusFileList = []
    task_list = []
    for familFile in familyFileList:
        familyGroup = readFile(familFile)
        for family in familyGroup:
            name = family['param']
            name_zhcn = family['text']
            floder_relative = "{0}".format(os.path.dirname(familFile))
            file = "{0}/{2}/{2}.json".format(floder_relative, name, name_zhcn)
            genusFileList.append(file)
            folder = os.path.dirname(file)
            mkdir(folder)
            if(isRecapture or not os.path.exists(file)):
                getDataTask = getGenuses(name, file, sem)
                task_list.append(getDataTask)

    await asyncio.gather(*task_list)

    # get all species
    print("get all species")
    speciesFileList = []
    task_list = []
    for genusFile in genusFileList:
        genusGrpup = readFile(genusFile)
        for genus in genusGrpup:
            name = genus['param']
            name_zhcn = genus['text'].replace("<em>","").replace("</em>","").replace("'","").strip()
            floder_relative = "{0}".format(os.path.dirname(genusFile))
            file = "{0}/{2}/{2}.json".format(floder_relative, name, name_zhcn)
            speciesFileList.append(file)
            folder = os.path.dirname(file)
            mkdir(folder)
            if(isRecapture or not os.path.exists(file)):
                getDataTask = getSpecieses(name,name_zhcn, file, sem)
                task_list.append(getDataTask)

    await asyncio.gather(*task_list)

    # get detail and construct image url list
    print("get detail and download image")
    task_list = []    
    speciesInfoFileList = []
    for speciesFile in speciesFileList:
        speciesGroup = readFile(speciesFile)
        for species in speciesGroup:
            name = species['param']
            name_zhcn = species['text'].replace("<em>","").replace("</em>","").replace("'","").strip()
            floder_relative = "{0}".format(os.path.dirname(speciesFile))
            file = "{0}/{2}/{2}.json".format(floder_relative, name, name_zhcn)
            speciesInfoFileList.append(file)
            folder = os.path.dirname(file)
            mkdir(folder)
            # load json
            if(isRecapture or not os.path.exists(file)):
                param = {} 
                param["name"] = name
                param["name_zhcn"] = name_zhcn            
                getDataTask = getSpecieInfo(param, file, species, sem)
                task_list.append(getDataTask)

    results = await asyncio.gather(*task_list)
    new_rrorSpecies = [o['data'] for o in results if o['status'] == False]
    if(len(new_rrorSpecies) > 0):
        print("write error species")
        old_errorSpecies = readFile(errorSpeciesFile)
        errorSpecies = old_errorSpecies + new_rrorSpecies
        writeFile(errorSpeciesFile, json.dumps(errorSpecies, separators=(',', ':'), ensure_ascii=False))

    # download images
    print("download images")
    task_list = []
    speList = []
    for speciesInfoFile in speciesInfoFileList:
        # FIXME:  THIS IS AN ERROR
        try:
            speciesInfo = readFile(speciesInfoFile)
        except Exception as e:
            print('error: download image read file {0} error'.format(speciesInfoFile))
            continue
        speciesInfo = readFile(speciesInfoFile)
        folder = os.path.dirname(speciesInfoFile)
        name = speciesInfo['info']['canName']
        name_zhcn = speciesInfo['info']['chName'] if 'chName' in speciesInfo['info'] else speciesInfo['info']['accName_ch']        
        image_folder = folder + "/_images"
        mkdir(image_folder)
        for picItem in speciesInfo['pictures']:
            fileName,fileType = resolveFileUrl(picItem["reference"])
            image_file = image_folder + "/{1}_{2}".format(name,name_zhcn.replace("\\",""), fileName)            
            picItem['name'] = fileName
            picItem['img_file'] = image_file
            # download image            
            if(isRecapture or not os.path.exists(image_file)):
                imageTask = getImage(picItem['reference'], image_file, sem)
                task_list.append(imageTask)
        speList.append(speciesInfo)

    await asyncio.gather(*task_list)    


    end = time.time()
    print("time: {0}".format(end - start))
    print("total ")

    
    if(isUploadDatabase):
      save2dDB(dbFile, speList)


if __name__ == '__main__':  
    asyncio.run(main())

    # cid = 4
    # mu = "蚓螈目"
    # file = "./result/{0}/{1}/{1}.json".format(cid, mu)
    # folder = os.path.dirname(file)
    # mkdir(folder)
    
    # getData(cid, urllib.parse.quote(mu), file)
            

