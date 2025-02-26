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
from pyppeteer import launch
from utils import readFile, writeFile, mkdir, listFile, writeImage

proxies = {
   'http': 'http://172.26.137.247:30000',
   'https': 'http://172.26.137.247:30000',
}

def isAborted(request):
    abortedUrlTexts = ["unicast","qrcode.min.js","passport.baidu.com"]
    for text in abortedUrlTexts:
        if text in request.url:
            return True
    return False

            
async def pagePrepare(page):
     # 启用请求拦截
    # await page.setRequestInterception(True)

    # page.on('response', lambda res: print(
    #     f'[Response] {res.status} {res.url} '
    #     f'via {res.request.resourceType}'
    # ))

    # 定义请求拦截的回调函数
    
    async def intercept_request(request):
        try:
            # 只拦截特定的文件请求（例如，一个特定的 .js 文件）                    
            if isAborted(request):
                print(f"拦截请求: {request.url}")
                request.abort()
            else:                
                print(f"请求的 URL: {request.url}")
                request.continue_()  # 继续其他请求
        except Exception as e:
            print(e)
    page.on('request',intercept_request)
#             request.continue_()
    

async def getPageCount(url, sem, browser):    
    # 无头浏览器
    totalPage = 0
    async with sem:        
        page = await browser.newPage()
        await pagePrepare(page)
        await page.goto(url, {'waitUntil' : 'domcontentloaded', 'timeout': 60000})
        # 滚到整个page的底部
        await page.evaluate('window.scrollBy(0, document.body.scrollHeight)')
        # 从页面中提取页码div组件
        container = await page.J('.pb_footer .p_thread .l_thread_info .l_posts_num')
        # 提取页码
        element_total = await container.JJ('.l_reply_num .red')
        if(len(element_total) > 1):
            element_total = element_total[1]
        totalPage = await page.evaluate('(element) => element.innerText', element_total)

        # close page
        await page.close()

    return totalPage


async def getPageContent(url, sem, browser, outputFolder):
    async with sem:        
        page = await browser.newPage()
        await pagePrepare(page)
        try:
            await page.goto(url, {'waitUntil' : 'domcontentloaded', 'timeout': 60000})
        except Exception as e:
            print(e)
            await page.evaluate('window.stop()')
        # wait 2 seconds
        # await page.waitFor(2000)
        # wait for page load
        await page.waitForSelector('.d_post_content')

        postContents = await page.JJ('.d_post_content')

        dataList = []
        # 遍历每一个帖子
        for postContent in postContents:
            # 滚动到帖子的底部
            await page.evaluate('(element) => element.scrollIntoView(false)', postContent)
            # 获取postContent的外部html代码
            html = await page.evaluate('(element) => element.outerHTML', postContent)
            # 获取帖子id
            id = await page.evaluate('(element) => element.getAttribute("id")', postContent)           
            # 提取帖子内容
            content = await page.evaluate('(element) => element.innerText', postContent) 
            tempContentList = content.split("\n")
            locations = []
            title = ""
            isUsefull = False
            for tempContent in tempContentList:
                tempContent = tempContent.replace(" ", "").replace("：", ":")
                if "地址" in tempContent and ":" in tempContent:                    
                    locations.append(tempContent.split(":")[1])
                    isUsefull = True
                if "城市" in tempContent and ":" in tempContent:
                    title = tempContent.split(":")[1]
                    isUsefull = True
                    
            if(not isUsefull):
                continue
            # title = await postContent.J('.d_post_content')
            # 提取帖子中的图片
            images = []
            imageElements = await postContent.JJ('img')
            # with index
            for index, imageElement in enumerate(imageElements):            
                image = await page.evaluate('(element) => element.src', imageElement)
                images.append({
                    "url": image,
                    "data_id": id,
                    "fileName": image.split('/')[-1],
                    "location_index": index
                })
            data = {
                "url": url,
                "html": html,
                "id": id,                
                "locations": locations,                
                "location_desc": ";".join(locations),
                "title": title,
                "images":images,
                "root":outputFolder
            }

            # save
            file = os.path.join(outputFolder, "{0}_{1}.json".format(title,id))
            json_str = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
            dataList.append(data)
            writeFile(file, json_str)
        await page.close()
        return dataList
        

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




def save2dDB(dbFile, dataList):
    structFile = "./dbStruct/db_market/db.json"
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
    # insert market
    i_spec = 0
    i_image = 0
    i_spec_image = 0
    i_location = 0
    for dataItem in dataList:
        dataItem['cid'] = i_spec
        dataItem["json"] = json.dumps(dataItem, separators=(',', ':'), ensure_ascii=False)
        db.insertData(conn, "market", dataItem, [o for o in struct_raw if o["tbName"] == "market"][0]["fields"])
        images = dataItem["images_obj"]
        for image in images:
            image_data = {
                "id": i_image,
                "name": image["fileName"],
                "file": db.convertToBinaryData(image["file"])
            }
            db.insertData(conn, "images", image_data, [o for o in struct_raw if o["tbName"] == "images"][0]["fields"])
            spec_image_data = {
                "id": i_spec_image,
                "market_id": i_spec,
                "image_id": i_image,
                "location_index": image["location_index"],
            }
            db.insertData(conn, "market_images", spec_image_data, [o for o in struct_raw if o["tbName"] == "market_images"][0]["fields"])
            i_spec_image += 1
            i_image += 1


        # with index
        for index, location in enumerate(dataItem["locations"]):        
            location_data = {
                "id": i_location,
                "market_id": i_spec,
                "location": location,
                "location_index": index
            }
            db.insertData(conn, "market_location", location_data, [o for o in struct_raw if o["tbName"] == "market_location"][0]["fields"])
            i_location += 1

        if(i_spec % 100 == 0):
            print("insert market {0}".format(i_spec))
            conn.commit()
        i_spec += 1    

        # insert images
    conn.commit()
    conn.close()

async def main():
    isRecapture = False
    isUploadDatabase = True
    resultFolder = "./result_market/"
    dbFile = os.path.join(resultFolder,"result.db")     

    chromePath = "C:\Program Files\Google\Chrome\Application\chrome.exe"


    root = "http://tieba.baidu.com/p/5010360774"    
    

    sem = asyncio.Semaphore(1)

    print("start")
    start = time.time()    

    mkdir(resultFolder)
    # browser 打开开发者模式，并打开network面板
    browser = await launch(headless=False, executablePath=chromePath, args=['--auto-open-devtools-for-tabs'])    
    

    # get page count
    pageCountFile = os.path.join(resultFolder, "pageCount.json")
    if(isRecapture or not os.path.exists(pageCountFile)):
        print("pageCount save disappear. try to get pageCount...")
        pageCount = await getPageCount(root,sem=sem,browser=browser)
        json_str = json.dumps({
            "pageCount": int(pageCount)
        }, separators=(',', ':'), ensure_ascii=False)
        writeFile(pageCountFile, json_str)
    else:
        pageCount = readFile(pageCountFile)["pageCount"]
        print("pageCount save exist: {0}".format(pageCount))


    # get page content    
    for pageNum in range(1, pageCount+1):
        url = root + "?pn={0}".format(pageNum)
        task_list = []
        folder = os.path.join(resultFolder, "page_{0}".format(pageNum))
        if(isRecapture or not os.path.exists(folder)):
            print("get page {0}".format(pageNum))
            mkdir(folder)
            task_list.append(getPageContent(url,sem,browser,outputFolder=folder))
            dataList =  await asyncio.gather(*task_list)
        
            # [][] -> []
            dataList = [item for sublist in dataList for item in sublist]
            images = []
            for data in dataList:
                if("images" in data):                    
                    for img_obj in data["images"]:                        
                        imageFileFolder = os.path.join(folder, "_images", data["title"]+data["id"])
                        mkdir(imageFileFolder)
                        img_obj["file"] = os.path.join(imageFileFolder, img_obj["fileName"])                        
                        images.append(img_obj)                       
            json_str = json.dumps(images, separators=(',', ':'), ensure_ascii=False)
            writeFile(os.path.join(folder, "images.json"), json_str)

        else:
            print("skip {0}".format(folder))
          

    # struct data
    print("get images")
    listFiles = listFile(folder, ".json")    
    task_list = []
    imagesDefineList=[]
    totalDataObjectList = []
    dirs = os.listdir(resultFolder)
    for dir in dirs:
        folder = os.path.join(resultFolder, dir)

        listFiles = listFile(folder, ".json")        
        if(listFiles is None or len(listFiles) == 0):
            print("no json file in {0}".format(folder))
            continue

        imageFiles = listFile(folder, "images.json")
        imagesDefine =[]
        if(imageFiles is None or len(imageFiles) == 0):
            print("no images.json file in {0}".format(folder))
            continue
        else:
            imagesDefine = readFile(imageFiles[0])
            imagesDefineList.extend(imagesDefine)
                    
        for file in listFiles:
            # read images define
            if(file.endswith("images.json")):
                continue
            # read item define
            else:
                itemDefine = readFile(file)
                itemDefine["images_obj"] = imagesDefine
                totalDataObjectList.append(itemDefine) 

     # get images
    for image in imagesDefineList:
        image_file = image['file']
        url = image['url']
        # imageFiles.append(image_file)
        if(isRecapture or not os.path.exists(image_file)):
            print("get image {0}".format(url))
            imageTask = getImage(url, image_file, sem)
            task_list.append(imageTask)
        else:
            print("skip {0}".format(image_file))
                
        # itemDefine['images'] = imageFiles

    await asyncio.gather(*task_list)


    if(isUploadDatabase):
        print("save to db: {0}".format(dbFile))
        save2dDB(dbFile, totalDataObjectList)

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
    # async def fetch_paragraph_content(url):
    #     # 启动浏览器
    #     browser = await launch(headless=False, executablePath="C:\Program Files\Google\Chrome\Application\chrome.exe")
    #     page = await browser.newPage()

    #     # 启用请求拦截
    #     # await page.setRequestInterception(True)

    #     # 定义请求拦截的回调函数
    #     @page.on('request')
    #     def intercept_request(request):
    #         print(f"请求的 URL: {request.url}")
    #         # 只拦截特定的文件请求（例如，一个特定的 .js 文件）
    #         if 'example.com/some-script.js' in request.url:
    #             print(f"拦截请求: {request.url}")
    #             request.abort()  # 阻止该请求
    #         elif isAborted(request):
    #             request.abort()
    #         else:
    #             request.continue_()  # 继续其他请求

    #     # 等待页面加载完成之前，捕获所有请求
    #     await page.goto(url, {'waitUntil' : 'load'})

    #     # 等待页面加载完成，直到网络空闲
    #     await page.waitForSelector('body')  # 等待body加载完成，确保页面渲染完成
    #     await page.waitForNetworkIdle()  # 等待网络空闲，确保所有请求都发出

    #     # 从页面中提取 class 为 'asd' 的 <p> 元素内容
    #     content = await page.Jeval('p.asd', 'element => element.innerText')

    #     # 关闭浏览器
    #     await browser.close()

    #     return content

    # 示例调用
    # url = 'https://tieba.baidu.com/p/5010360774'  # 替换成你要爬取的网页
    # asyncio.get_event_loop().run_until_complete(fetch_paragraph_content(url))
            

