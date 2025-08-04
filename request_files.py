from concurrent.futures import ThreadPoolExecutor
from langchain_community.llms import Ollama
from langchain_community.chat_models import ChatOllama
from langchain.schema import SystemMessage,HumanMessage
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
from utils import readFile, readPdf, readPdfAllSpans,searchImagesInPdf, writeFile, mkdir, listFile, writeImage, extract_json_from_llm_output

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
        container = await page.J('.pagination>li span')
        # 提取页码
        # element_total = await container.JJ('.l_reply_num .red')
        # if(len(element_total) > 1):
        #     element_total = element_total[1]
        totalPage = await page.evaluate('(element) => element.innerText', container)
        totalPage = int(totalPage.split(" / ")[1])

        # close page
        await page.close()

    return totalPage
async def getPageUrls(url, sem, browser):
    urls = []
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
        await page.waitForSelector('.post-loop .list-item')

        postContents = await page.JJ('.post-loop .list-item a')
        for postContent in postContents:
            title = await page.evaluate('(element) => element.innerText', postContent)
            pageUrl = await page.evaluate('(element) => element.getAttribute("href")', postContent)
            urls.append({
                "url": pageUrl,
                "title": title
            })
        # close page
        await page.close()  
    return urls

async def getFileUrls(url, sem, browser, tags):
    urls = []
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
        await page.waitForSelector('.w3eden a[data-downloadurl]')
        # postContents = await page.JJ('.w3eden .media-body')
        postContents = await page.JJ('.w3eden .package-title a')
        for postContent in postContents:            
            title = await page.evaluate('(element) => element.innerText', postContent)
            fileUrl = await page.evaluate('(element) => element.getAttribute("href")', postContent)
            urls.append({
                "url": fileUrl,
                "title": title,
                "tags": tags
            })
        # close page
        await page.close()  
    return urls

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
        await page.waitForSelector('.post-loop .list-item')

        postContents = await page.JJ('.post-loop .list-item a')

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

async def getFile(url, file, sem):
    async with sem:
       async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            print("request:  getFile {0} to {1}".format(url, file))
            res = await client.get(url)
            if(res):
                writeImage(file, res.content)


def imageMatchFunc (textSpan, imageBlockBox, imageBlock):
    title_bbox = textSpan["bbox"]    
    yDiff = abs(imageBlockBox.y0 - title_bbox.y0)
    xDiff = abs(imageBlockBox.x0 - title_bbox.x0)
    # 判断图片是否在文本的下方
    if imageBlockBox.y1 > title_bbox.y0 and yDiff < 200 and xDiff < 50: 
        return True
    return False

# 找到目标文本前的第一个有意义的文本
def findFirstTextBeforeTargetText(textList, targetText):
    lastText = ""
    for text in textList:   
        if(text is None):
            continue
        if targetText in text:
            return lastText
        if(text.strip() != ""):
            lastText = text

    return lastText


async def tryStructPaperContent(filePath, folder):
    res = []
    pdfContent = readPdf(filePath)
    pdfSpans = readPdfAllSpans(filePath)
   
    
    # a = searchImagesInPdf(filePath, textMatchFunc, imageMatchFunc, isSingle=True)
    pdfContentSplitPages = pdfContent.split("阅读原文")    

    modelId = "gemma3:27b"  # Ollama model id
    # modelId = 'qwen:32b'

    llm = Ollama(model=modelId, temperature=1, top_p=0.95, top_k=64 )
    # llm2 = Ollama(model=modelId)

    # chat = ChatOllama(model=modelId, temperature=1, top_p=0.95, top_k=64,seed=0)
    

    promot = """
任务：提取并格式化原文信息。
要求：
#1. 格式要求为[信息标题]：[原文内容]格式的json。
#2. 内容要求完全来自原文，不要做任何总结和转述。例如：“XXX海关在BBBB\n中查获CCC4 件” 标题应为 “XXX海关在BBBB中查获CCC4 件”。
#3. 返回内容为:
{{
"title": [原文]中的标题,此项必须有,此项必须有,此项必须有,
"date": [原文]中的新闻日期,
"org": [原文]中的执法机关,
"location": [原文]中的查获地点,
"method":[原文]中的查获方式
"pieces": [原文]中的涉案物种文本内容,此项必须有,
"level":[原文]中的涉案物种的保护等级，没有就填写空,
"transport": [原文]中的运输形式,
"reson": [原文]中的案由
}}
原文如下:`{}`
    """
    for index, pageContent in enumerate(pdfContentSplitPages):
        pageContent = pageContent.replace("国内新闻", "")
        if(len(pageContent.strip()) > 0):
            print("try struct page {0}/{1}".format(index, len(pdfContentSplitPages) - 1))
            if(index==50):
                pass
            maybeATitlePieceText = findFirstTextBeforeTargetText(pageContent.split("\n"), "新闻日期")

            fullPromot = promot.replace("\n","").format(pageContent)            
            try:
                llm_output = llm(fullPromot.replace("\n",""))
                messages = [
                    SystemMessage(content="你是一个严谨的信息抽取助手，请严格遵守用户指令，保留原文，不总结。"),
                    HumanMessage(content=fullPromot)
                ]
                # llm_output2 = chat.invoke(messages)
                json_obj = extract_json_from_llm_output(llm_output)                
                # print(res)
                # json_obj = json.loads(cleaned)
                json_obj["page"] = index
                json_obj["filePath"] = filePath  
                json_obj["raw"]  = pageContent

                
                if(maybeATitlePieceText == "" or maybeATitlePieceText is None or maybeATitlePieceText.strip() == ""):
                    maybeATitlePieceText = json_obj["title"]
                def textMatchFunc (textSpan):
                    text = textSpan['text']
                    # 匹配标题
                    if maybeATitlePieceText.strip() in text:
                        return True
                    return False
                # 提取图片
                images = searchImagesInPdf(filePath, textMatchFunc, imageMatchFunc, isSingle=True)
                json_obj["__images"] = images

                res.append(json_obj)  

                # #test
                # return res    



            except Exception as e:
                print("error: {0}".format(e))
    return res

def save2dDB(dbFile, dataList):
    structFile = "./dbStruct/db_trafficchina/db.json"
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
    # insert report
    i_spec = 0
    i_image = 0
    i_spec_image = 0    
    for dataItem in dataList:
        dataItem['cid'] = i_spec
        dataItem["json"] = json.dumps(dataItem, separators=(',', ':'), ensure_ascii=False)
        db.insertData(conn, "report", dataItem, [o for o in struct_raw if o["tbName"] == "report"][0]["fields"])
        images = dataItem["__images"]
        for imageUrl in images:
            if(imageUrl is None):
                continue
            imageName = os.path.splitext(imageUrl)[0]

            image_data = {
                "id": i_image,
                "name": imageName,
                "file": db.convertToBinaryData(imageUrl)
            }
            db.insertData(conn, "images", image_data, [o for o in struct_raw if o["tbName"] == "images"][0]["fields"])
            report_images = {
                "id": i_spec_image,
                "report_id": i_spec,
                "image_id": i_image,
            }
            db.insertData(conn, "report_images", report_images, [o for o in struct_raw if o["tbName"] == "report_images"][0]["fields"])
            i_spec_image += 1
            i_image += 1

       

        if(i_spec % 100 == 0):
            print("insert report {0}".format(i_spec))
            conn.commit()
        i_spec += 1    

        # insert images
    conn.commit()
    conn.close()

async def main():
    isRecapture = False    
    isUploadDatabase = True
    resultFolder = "./result_files_trafficchina/"    
    dbFile = os.path.join(resultFolder,"result.db")  
    chromePath = "C:\Program Files\Google\Chrome\Application\chrome.exe"


    root = "https://www.trafficchina.org/category/publication/lawenforcement/"    
    

    sem = asyncio.Semaphore(3)

    print("start")
    start = time.time()    

    mkdir(resultFolder)
    # browser 打开开发者模式，并打开network面板
    # browser = await launch(headless=False, executablePath=chromePath, args=['--auto-open-devtools-for-tabs'])    
    browser = await launch(headless=True, executablePath=chromePath)    
    

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


    # get page url 
    pageList= []
    for pageNum in range(1, pageCount+1):
        task_list = [] 
        url = root + "/page/{0}/".format(pageNum)
        pageDefineFile = os.path.join(resultFolder, "recordsOnThisPage_{0}.json".format(pageNum))
        if(isRecapture or not os.path.exists(pageDefineFile)):
            print("get page {0}".format(pageNum))            
            task_list.append(getPageUrls(url,sem,browser))
            recordsOnThisPage =  await asyncio.gather(*task_list)
            pageItem = {
                "page": pageNum,                
                "records": recordsOnThisPage[0]
            }
            pageList.append(pageItem)
            json_str = json.dumps(pageItem, separators=(',', ':'), ensure_ascii=False)
            writeFile(pageDefineFile, json_str)
        else:
            print("skip page {0}".format(pageNum))
            # read from file
            pageList.append(readFile(pageDefineFile))


    # get files url
    fileInfoList = []
    for pageItem in pageList:
        task_list = []
        jsonFilePath = os.path.join(resultFolder, "filesOn_{0}.json".format(pageItem["page"]))
        if(isRecapture or not os.path.exists(jsonFilePath)): 
            for recordItem in pageItem["records"]:
                url = recordItem["url"]
                title = recordItem["title"]                        
                task_list.append(getFileUrls(url,sem,browser, recordItem))                 
            filesInfo =  await asyncio.gather(*task_list)
            # [][] to []
            json_obj = [item for sublist in filesInfo for item in sublist if item is not None]
            fileInfoList = fileInfoList + json_obj
            json_str = json.dumps(json_obj, separators=(',', ':'), ensure_ascii=False)
            writeFile(jsonFilePath, json_str)
        else:
            fileInfoList = fileInfoList + readFile(jsonFilePath)



    # download file    
    task_list = []
    for fileInfo in fileInfoList:
        folder = os.path.join(resultFolder, fileInfo["tags"]["title"])
        filePath = os.path.join(folder, fileInfo["title"]+".pdf")

        # check file if is 0kb
        if(isRecapture or not os.path.exists(filePath) or os.path.getsize(filePath) == 0):
            mkdir(folder)
            url = fileInfo["url"]
            print("get file {0} for {1}".format(url, folder))
            fileTask = getFile(url, filePath, sem)
            task_list.append(fileTask)
        else:
            print("skip {0}".format(filePath)) 

        dataObjFilePath = os.path.join(folder, fileInfo["title"]+"_data.json")
        if(isRecapture or not os.path.exists(dataObjFilePath) or os.path.getsize(dataObjFilePath) == 0):
            # read pdf content & solve data
            print("try struct paper content for {0}".format(filePath))
            dataObjects = await tryStructPaperContent(filePath, folder)
           
            # save images
            imageFolder = os.path.join(folder, "_images_" + fileInfo["title"])
            mkdir(imageFolder)
            for o in dataObjects:                
                if(o["__images"] is not None):
                    # for with index
                    for index, image in enumerate(o["__images"]):
                        imageFileName = os.path.join(imageFolder, "{0}_{1}.{2}".format(o["title"], index, image["image_ext"]))
                        try:                            
                            writeImage(imageFileName, image["image_bytes"])
                            o["__images"][index] = imageFileName
                        except Exception as e:
                            print("error when write image: {0}".format(e))
                            o["__images"][index] = None
                            continue

            # save json          
            json_str = json.dumps(dataObjects, separators=(',', ':'), ensure_ascii=False)
            writeFile(dataObjFilePath, json_str)
    
    await asyncio.gather(*task_list)
    await browser.close()

    if(isUploadDatabase):
        print("save to db: {0}".format(dbFile))
        # read data         
        # 列出文件夹内的所有第一级文件夹, 读取第一级文件夹内的json文件
        dataList = []
        listFiles = listFile(resultFolder, endwith="_data.json")
        for filePath in listFiles:
            fileData = readFile(filePath)
            fileNameWithOutExt = os.path.splitext(os.path.basename(filePath))[0]
            if(fileData is not None and len(fileData) > 0):
                for item in fileData:
                    item["paperTitle"] = fileNameWithOutExt.split("_data")[0]
                    dataList.append(item)

        save2dDB(dbFile, dataList)
        

    end = time.time()
    print("end")
    print("time: {0}".format(end - start))

if __name__ == '__main__':      
    asyncio.run(main())
    

