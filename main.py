import time
import shutil
import requests
import numpy as np
import pandas as pd
from multiprocessing import Pool
from concurrent import futures
from utils import filterHtmlTag,Driver,get_html_by_selenium,get_file_names
from requests.packages.urllib3.exceptions import InsecureRequestWarning



# 初始化一个线程池，最大的同时任务数是 5
executor = futures.ThreadPoolExecutor(max_workers=5)


# 忽略requests证书警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# def getContent(url):
#     '''获取网页的纯汉字内容'''
#     url = url
#     # 发送请求获取网页内容
#     headers = {"User-Agent":'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36 Edg/92.0.902.55'}
#     try:
#         resp = requests.get(url=url, headers=headers, verify=False)
#         # print(resp.encoding)
#         if resp.encoding == "ISO-8859-1":
#             # 解决编码问题
#             html = resp.text.encode('ISO-8859-1').decode(requests.utils.get_encodings_from_content(resp.text)[0])
#             print(html)
#         else:
#             html = resp.text
#         # 过滤html中的标签
#         content = filterHtmlTag(html)
#         if content == "":
#             content = "网站无法获取中文数据"
#         print(f"url:{url}  content:{content}")
#         return content
#     # 处理网站请求失败的情况
#     except Exception as e:
#         # print(f"ERROR of {url}",e)
#         content = "网站请求失败"
#         print(f"url:{url}  content:{content}")
#         return content

# def counterToSave(counter,Data,contentsList,filepath,number=5):
#     '''运行number次数后将已经获取的结果先保存下来'''
#     if counter % number == 0:
#         # 每number条数据写入一次
#         print(f"============================================{counter}=========================================")
#         # 添加已经获得的content到相应链接后方
#         Data["content"] = pd.Series(contentsList)
#         # 将当前的Data写入到原文件
#         Data.to_csv(filepath, index=None, header=False)
#         # 将已获取到内容的数据先写入新的csv文件
#         Data[0:number].to_csv(f"data/temp/{counter}.csv", index=None, header=False)
#         # 在原文件中进行删除
#         Data.drop(Data.index[0:10], inplace=True)
#         Data.to_csv(filepath, index=None, header=False)
#         time.sleep(120)
#     return Data

# def loopGetContent(filepath):
#     '''读取csv文件中的网址进行访问，并将处理后内容存入csv文件'''
#     # 获取csv文件中的数据总条数
#     # totallines = sum(1 for line in open(filepath,encoding='utf-8'))
#     # 读取csv文件中的url
#     urlsList = readUrlsFromCsv(filepath)
#     # 获取去重后的数据
#     Data = processData(filepath)
#     # contentsList用于存储访问每个url获取的中文内容
#     contentsList = []
#     counter = 0
#     # 添加新列
#     for url in urlsList:
#         # content = getContent(url)
#         content = getContentBySelenium(url,counter)
#         contentsList.append(content)
#         counter += 1
#         number = 5
#         # 运行number次数后将已经获取的结果先保存下来
#         if counter % number == 0:
#             # 每number条数据写入一次
#             # 添加已经获得的content到相应链接后方
#             print(f"length of contentsList {len(contentsList)}",contentsList)
#             Data["content"] = pd.Series(contentsList)
#             contentsList = []
#             # 将当前的Data写入到原文件
#             Data.to_csv(filepath, index=None, header=False)
#             # 将已获取到内容的数据先写入新的csv文件
#             Data[0:number].to_csv(f"data/temp/{counter}.csv", index=None, header=False)
#             # 在原文件中进行删除
#             Data.drop(Data.index[0:number], inplace=True)
#             Data.to_csv(filepath, index=None, header=False)
#             print(f"============================================{counter}=========================================")
#             time.sleep(1)

# def processData(filepath):
#     '''对读取的数据进行去重'''
#     # 将csv文件内数据读出
#     Data = pd.read_csv(filepath, header=None)
#     # print(f"去重前总数据条数：{Data.index}")
#     # 去除出第一列中重复的网址
#     Data.drop_duplicates(subset=0,keep='first', inplace=True)
#     # print(f"去重后总数据条数：{Data.index} length:{len(Data)}")
#     Data.to_csv(filepath, index=None, header=False)
#     return Data

def getContentBySelenium(url,counter):
    '''通过selenium获取html并提取出其中的汉字'''
    counter += 1
    try:
        # 加载浏览器驱动
        # driver = Driver()
        # 获取网页html
        # html = get_html_by_selenium(driver, url)
        url = url.split('"')[0]
        html = get_html_by_selenium(url)
        # 过滤html中的标签
        content = filterHtmlTag(html)
        if content == "":
            content = "网站请求失败"

        print(f"{counter} url:{url}  content:{content}")
        # contentsList.append(content)
        # return contentsList
        return content

    except Exception as e:
        # print(f"ERROR of {url}",e)
        content = "网站请求失败"
        print(f"{counter} url:{url}  content:{content}")
        # contentsList.append(content)
        # return contentsList
        return content


def readUrlsFromCsv(filepath):
    '''从csv文件中读取出url地址'''
    # urlsList用于存储所有的url
    urlsList = []
    # 读取csv中的网址
    for line in open(filepath, encoding='utf-8'):
        try:
            # url, category= line.split(',')[0:2]
            url = line.strip('"')
            urlsList.append(f"http://" + url.strip('"'))
        except:
            continue
    # urlsList.pop(0)
    print("length of urlsList:", len(urlsList))
    print("urlsList:",urlsList)
    return urlsList

def processOneCsvFile(filename,absolute_path,result_path):
    global counter
    '''处理切分后的单个csv文件'''
    print(f"================================processing {filename}======================================")
    filepath = absolute_path + filename
    # 读取csv文件中的url
    urlsList = readUrlsFromCsv(filepath)
    # 获取去重后的数据
    Data = pd.read_csv(filepath,header=None)
    # contentsList用于存储访问每个url获取的中文内容
    contentsList = []


    p = Pool(processes=50)
    for i in range(len(urlsList)):
        # result = p.apply_async(func=getContentBySelenium, args=[urlsList[i], counter,contentsList])
        contentsList.append(p.apply_async(func=getContentBySelenium, args=[urlsList[i], counter]))
    p.close()
    p.join()

    temp = []
    for i in contentsList:
        # print(i.get())
        temp.append(i.get())

    contentsList = temp
    print(f"counterList{contentsList}")


    # # 遍历列表访问url获取内容
    # for url in urlsList:
    #     # pool = Pool(10)
    #     # pool.map(func, url_list)
    #     content = getContentBySelenium(url,counter)
    #     counter+=1
    #     contentsList.append(content)
    #     # if counter % 10==0:
    #     #     print(f"============================================{counter}=========================================")



    # 将获取到的内容添加成Data的新列
    Data["content"] = pd.Series(contentsList)
    contentsList = []
    # 删除列的表头
    Data.drop(Data.index[0:1], inplace=True)
    # 将当前的Data写入到原文件
    Data.to_csv(filepath, index=None, header=None)
    # 将处理好的文件写入result文件夹
    shutil.move(filepath,result_path)
    time.sleep(1)


def processMulCsvFile(csv_path,absolute_path,result_path):
    '''执行多次单个处理csv文件操作'''
    filenames = get_file_names(csv_path)
    for filename in filenames:
        processOneCsvFile(filename,absolute_path=absolute_path,result_path=result_path)




if __name__ == '__main__':
    # url = 'http://www.lazxsf.com'
    # content = getContentBySelenium(url)
    # 定义counter用于计数
    counter = 0
    # csv_path为分割后的csv文件所在的位置
    csv_path = "data/test"
    # absolute_path为相对路径
    absolute_path = "data/test/"
    # result_path为处理完的单个csv文件保存路径
    result_path = "data/result/"
    processMulCsvFile(csv_path,absolute_path,result_path)


    # counter = 0
    # csv_path = "data/temp"
    # absolute_path = "data/temp/"
    # result_path = "data/temp2/"
    # processMulCsvFile(csv_path, absolute_path, result_path)