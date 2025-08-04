import json
import os
import re
import fitz
from matplotlib import pyplot as plt  # PyMuPDF

def readFile(file):
    res = None
    with open(file, 'r', encoding="utf-8") as f:
        res = json.load(f)
    return res

def readPdf(file):
    
    res = ""
    with fitz.open(file) as doc:
        for page in doc:
            res += page.get_text()
    return res

def readPdfAllSpans(file):
    res = []
    with fitz.open(file) as doc:
        for page in doc:
            blocks = page.get_text("dict")["blocks"]
            for block in blocks:
                if block["type"] != 0:  # 只处理文本块
                    continue
                for line in block["lines"]:
                    for span in line["spans"]:
                        res.append({
                            "text": span["text"].strip(),
                            "bbox": fitz.Rect(span["bbox"]),
                            "font_size": span["size"],
                            "is_bold": "Bold" in span["font"],
                        })
    return res

def previewImageBlocks(doc, imageBlock):
    # 用cv或其他库直接预览图片看看对不对
    #try catch
    try:
        xref = imageBlock[0]  # 获取图片的xref
        base_image = doc.extract_image(xref)
        image_bytes = base_image["image"]
        image_ext = base_image["ext"]  # 获取图片的扩展名
    except Exception as e:
        image_bytes = imageBlock['image']
        image_ext = imageBlock['ext']
    
    # 预览图片
    with open(f"temp_image.{image_ext}", "wb") as img_file:
        img_file.write(image_bytes)

    


def searchImagesInPdf(pdf_path, textMatchFunc=None, imageMatchFunc=None, isSingle=True):    
    doc = fitz.open(pdf_path)
    results = []

    for page_num, page in enumerate(doc, 1):
        blocks = page.get_text("dict")["blocks"]

        # 获取图片块（type == 1）
        image_blocks = [b for b in blocks if b["type"] == 1]

        # 提取文本块
        text_spans = []
        for block in blocks:
            if block["type"] != 0:
                continue
            for line in block["lines"]:
                for span in line["spans"]:
                    text_spans.append({
                        "text": span["text"].strip(),
                        "bbox": fitz.Rect(span["bbox"]),
                        "font_size": span["size"],
                        "is_bold": "Bold" in span["font"],
                    })

        # 查找标题和其下方的图片块
        for span in text_spans:
            text = span["text"]
            if textMatchFunc is not None and textMatchFunc(span) == True:
                title = text
                title_bbox = span["bbox"]

                # 匹配下方最近的图像块（纵向接近）
                matched_image = None
                for ib in image_blocks:
                    previewImageBlocks(doc, ib)  # 预览图片块
                    image_bbox = fitz.Rect(ib["bbox"])                    
                    if imageMatchFunc is not None and imageMatchFunc(span, image_bbox, ib) == True:
                    # if 0 <= image_bbox.y0 - title_bbox.y1 <= 150:  # 控制在下方150pt
                        matched_image = ib
                        break

                # 导出图片
                if matched_image:
                    # xref = ib.get("image")  # 确保有图片数据
                    # xref = matched_image["image"]
                    # base_image = doc.extract_image(xref)
                    # image_bytes = base_image["image"]
                    # ext = base_image["ext"]          

                    image_bytes  = matched_image["image"]
                    ext = matched_image["ext"]  # 获取图片的扩展名         

                    results.append({
                        "title": title,                        
                        "image_bytes": image_bytes,
                        "image_ext": ext,
                        "page_num": page_num,
                        "bbox": title_bbox,
                        "image_bbox": fitz.Rect(matched_image["bbox"]),
                        "image_info": matched_image,
                    })
                    if(isSingle):
                        return results

    return results

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

def extract_json_from_llm_output(text: str):
    """
    从 LLM 返回中提取并解析 JSON 数据。
    支持 Markdown 格式、嵌套换行、前缀说明等。
    """
    # 尝试从 markdown 代码块中提取
    code_blocks = re.findall(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if code_blocks:
        try:
            return json.loads(code_blocks[0])
        except json.JSONDecodeError as e:
            raise ValueError(f"解析 code block JSON 出错: {e}")

    # 备选方案：尝试从全文中匹配出第一个 JSON 对象（花括号配对）
    json_like = re.search(r"\{.*?\}", text, re.DOTALL)
    if json_like:
        try:
            return json.loads(json_like.group())
        except json.JSONDecodeError as e:
            raise ValueError(f"解析嵌入式 JSON 出错: {e}")

    raise ValueError("未找到有效 JSON 数据")

# 递归遍历文件夹
def listFile(path, endwith=".json"):
    res = []
    for root, dirs, files in os.walk(path):
        for file in files:
            if endwith is not None:
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


