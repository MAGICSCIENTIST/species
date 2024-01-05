from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import math
import random
import requests
import json
import os
import urllib.parse
import asyncio
import httpx
import time




async def getCatelogs(userObj, sem):

    url ="https://weiban.mycourse.cn/pharos/usercourse/listCategory.do?timestamp={0}".format(datetime.now().timestamp())    
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
        "sec-ch-ua": "\"Chromium\";v=\"116\", \"Not)A;Brand\";v=\"24\", \"Google Chrome\";v=\"116\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "x-token": userObj["token"]
    },
    body = {
     "tenantCode" : userObj["tenantCode"],
     "userId" : userObj["userId"],
     "userProjectId" : userObj["userProjectId"],
     "chooseType":3
     
    }    
    async with sem:
        async with httpx.AsyncClient(timeout=20,  verify=False) as client:           
            res = await client.post(url, headers=headers[0],data=body)
            res = res.json()        
            return res
        
async def getList(userObj,groupId, sem):

    url ="https://weiban.mycourse.cn/pharos/usercourse/listCourse.do?timestamp={0}".format(datetime.now().timestamp())    
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
        "sec-ch-ua": "\"Chromium\";v=\"116\", \"Not)A;Brand\";v=\"24\", \"Google Chrome\";v=\"116\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "x-token": userObj["token"]
    },
    body = {
     "tenantCode" : userObj["tenantCode"],
     "userId" : userObj["userId"],
     "userProjectId" : userObj["userProjectId"],
     "categoryCode": groupId,
     "chooseType":3
     
    }    
    async with sem:
        async with httpx.AsyncClient(timeout=20,  verify=False) as client:           
            res = await client.post(url, headers=headers[0],data=body)
            res = res.json()        
            return res
        
async def startStudy(userObj,courseId, sem):


    url ="https://weiban.mycourse.cn/pharos/usercourse/study.do?timestamp={0}".format(datetime.now().timestamp()) 
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
        "sec-ch-ua": "\"Chromium\";v=\"116\", \"Not)A;Brand\";v=\"24\", \"Google Chrome\";v=\"116\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "x-token": userObj["token"]
    },
    body = {
     "tenantCode" : userObj["tenantCode"],
     "userId" : userObj["userId"],
     "userProjectId" : userObj["userProjectId"],
     "courseId": courseId,
     
    }    

    url2 ="https://weiban.mycourse.cn/pharos/usercourse/getCourseUrl.do?timestamp={0}".format(datetime.now().timestamp()) 
    headers2 = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
        "sec-ch-ua": "\"Chromium\";v=\"116\", \"Not)A;Brand\";v=\"24\", \"Google Chrome\";v=\"116\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "x-token": userObj["token"]
    },
    body2 = {
     "tenantCode" : userObj["tenantCode"],
     "userId" : userObj["userId"],
     "userProjectId" : userObj["userProjectId"],
     "courseId": courseId,
     
    }    

    
    headers_f = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
        "sec-ch-ua": "\"Chromium\";v=\"116\", \"Not)A;Brand\";v=\"24\", \"Google Chrome\";v=\"116\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "x-token": userObj["token"]
    }  

    async with sem:
        async with httpx.AsyncClient(timeout=20,  verify=False) as client:           
            res = await client.post(url, headers=headers[0],data=body)
            res = res.json()  

            res = await client.post(url2, headers=headers2[0],data=body2)
            _url = res.json()  

            # finish study
            # wait 5s -15s
            time.sleep(math.floor(10 + (15 - 5) * random.random()))
            parsed_url = urllib.parse.urlparse(_url["data"])
            methodToken = urllib.parse.parse_qs(parsed_url.query)['methodToken'][0]
            userCourseId = urllib.parse.parse_qs(parsed_url.query)['userCourseId'][0]
            url_f ="https://weiban.mycourse.cn/pharos/usercourse/v1/{1}.do?callback=jQuery34108974008757450105_1693729921183&userCourseId={2}&tenantCode={3}&_={0}".format(datetime.now().timestamp(),methodToken,userCourseId,userObj["tenantCode"]) 
            print(url_f)
            res = await client.get(url_f, headers=headers_f)            
            return res.text



async def main():
    sem = asyncio.Semaphore(1)
    userStr = '{"token":"ab8945b6-591e-4c7f-927e-83470f9e22df","userId":"f9ef70f8-2393-4313-a780-e4b90d408a60","userName":"217ec91d21b847cea53a84a3945c3b72","realName":"顾家铭","uniqueValue":"2230165","isBind":"2","tenantCode":"10000010","batchCode":"011","gender":1,"switchGoods":1,"switchDanger":1,"switchNetCase":1,"preBanner":"https://h.mycourse.cn/pharosfile/resources/images/projectbanner/pre.png","normalBanner":"https://h.mycourse.cn/pharosfile/resources/images/projectbanner/normal.png","specialBanner":"https://h.mycourse.cn/pharosfile/resources/images/projectbanner/special.png","militaryBanner":"https://h.mycourse.cn/pharosfile/resources/images/projectbanner/military.png","isLoginFromWechat":2,"tenantName":"北京林业大学","tenantType":1,"loginSide":1,"popForcedCompleted":2,"showGender":2,"showOrg":2,"nickName":"顾家铭","imageUrl":"https://resource.mycourse.cn/mercury/resources/mercury/wb/images/portrait.jpg","defensePower":60,"knowledgePower":60,"safetyIndex":99}'
    obb = json.loads(userStr)
    obb['userProjectId'] = "63f920e0-8282-4063-9b08-16f56a862fb5"
    # token = "ab8945b6-591e-4c7f-927e-83470f9e22df"
    task_list = []
    task_list.append(getCatelogs(obb,sem))
    catelogs = (await asyncio.gather(*task_list))[0]["data"]
     

    task_list = []
    for catelog in catelogs:
        task_list.append(getList(obb,catelog["categoryCode"],sem))
    coursesGroupList = (await asyncio.gather(*task_list))

    for courseList in coursesGroupList:
        task_list = []
        for course in courseList["data"]:
            if(course["finished"] == 1):
                continue
            task_list.append(startStudy(obb,course["resourceId"],sem))

        if(len(task_list) == 0):
            continue

        reses = (await asyncio.gather(*task_list))
        for (index, r) in enumerate(reses):
            print("{0};   {1}".format(courseList["data"][index]["resourceName"], r))        
     
if __name__ == '__main__':
     asyncio.run(main())