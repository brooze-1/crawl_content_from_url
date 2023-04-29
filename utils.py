import os
import re
import pandas as pd
from selenium import webdriver
from fake_useragent import UserAgent
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

def Driver():
    '''获取浏览器驱动对象'''
    # # 准备好参数配置,将该参数传入Edge可以使其不弹出窗口
    # opt = Options()
    # opt.add_argument("--headless")
    # opt.add_argument('--disable-gpu')
    # opt.page_load_strategy = 'eager'
    # # 设置user-agent
    # opt.add_argument(f'user-agent={UserAgent().random}')
    # driver = webdriver.Edge(options=opt)
    dcap = dict(DesiredCapabilities.PHANTOMJS)
    # 设置user-agent请求头
    dcap["phantomjs.page.settings.userAgent"] = UserAgent().random
    # 禁止加载图片
    dcap["phantomjs.page.settings.loadImages"] = False
    driver = webdriver.PhantomJS(desired_capabilities=dcap)
    # 设置请求时间
    driver.set_page_load_timeout(10)
    return driver

def get_html_by_selenium(url):
    '''使用浏览器驱动对象获取网页内容'''
    html = driver.get(url)
    html = driver.execute_script("return document.documentElement.outerHTML")
    return html

def filterHtmlTag(htmlstr):
    '''
    过滤html中的标签
    '''
    # 兼容换行
    s = htmlstr.replace('\r\n', '\n')
    s = htmlstr.replace('\r', '\n')
    # 规则
    re_cdata = re.compile('//<!\[CDATA\[[^>]*//\]\]>', re.I)  # 匹配CDATA
    re_script = re.compile('<\s*script[^>]*>[\S\s]*?<\s*/\s*script\s*>', re.I)  # script
    re_style = re.compile('<\s*style[^>]*>[\S\s]*?<\s*/\s*style\s*>', re.I)  # style
    re_br = re.compile('<br\\s*?\/??>', re.I)  # br标签换行
    re_p = re.compile('<\/p>', re.I)  # p标签换行
    re_h = re.compile('<[\!|/]?\w+[^>]*>', re.I)  # HTML标签
    re_comment = re.compile('<!--[^>]*-->')  # HTML注释
    re_hendstr = re.compile('^\s*|\s*$')  # 头尾空白字符
    re_lineblank = re.compile('[\t\f\v ]*')  # 空白字符
    re_linenum = re.compile('\n+')  # 连续换行保留1个

    # 处理
    s = re_cdata.sub('', s)  # 去CDATA
    s = re_script.sub('', s)  # 去script
    s = re_style.sub('', s)  # 去style
    s = re_br.sub('\n', s)  # br标签换行
    s = re_p.sub('\n', s)  # p标签换行
    s = re_h.sub('', s)  # 去HTML标签
    s = re_comment.sub('', s)  # 去HTML注释
    s = re_lineblank.sub('', s)  # 去空白字符
    s = re_linenum.sub('\n', s)  # 连续换行保留1个
    s = re_hendstr.sub('', s)  # 去头尾空白字符

    # 替换实体
    s = replaceCharEntity(s)
    # 只提取出汉字的部分
    s = re.findall('[\u4e00-\u9fa5]', s, re.S)
    return "".join(s)


def replaceCharEntity(htmlStr):
    '''
      替换html中常用的字符实体
      使用正常的字符替换html中特殊的字符实体
      可以添加新的字符实体到CHAR_ENTITIES 中
      CHAR_ENTITIES是一个字典前面是特殊字符实体  后面是其对应的正常字符
      :param htmlStr:
    '''
    htmlStr = htmlStr
    CHAR_ENTITIES = {'nbsp': ' ', '160': ' ',
                     'lt': '<', '60': '<',
                     'gt': '>', '62': '>',
                     'amp': '&', '38': '&',
                     'quot': '"', '34': '"', }
    re_charEntity = re.compile(r'&#?(?P<name>\w+);')
    sz = re_charEntity.search(htmlStr)
    while sz:
        entity = sz.group()  # entity全称，如>
        key = sz.group('name')  # 去除&;后的字符如（" "--->key = "nbsp"）    去除&;后entity,如>为gt
        try:
            htmlStr = re_charEntity.sub(CHAR_ENTITIES[key], htmlStr, 1)
            sz = re_charEntity.search(htmlStr)
        except KeyError:
            # 以空串代替
            htmlStr = re_charEntity.sub('', htmlStr, 1)
            sz = re_charEntity.search(htmlStr)
    return htmlStr

# 获取指定目录下的文件名 返回文件名列表  ori_path为初始指定目录地址
def get_file_names(ori_path):
    # 获取指定目录下的文件名
    file_names = os.listdir(ori_path)
    # 打印文件名进行查看
    # print("file_names:\n",file_names)
    # 打印指定目录下的文件数目
    # print("num of files:",len(file_names))
    file_names.sort(key=number,reverse=False)
    # 将文件名列表返回
    return file_names

def number(filename):
    # print(filename)
    return int(filename.split("_")[1].split(".")[0])

class PyCSV:

    def merge_csv(self, save_name, file_dir, csv_encoding='utf-8'):
        """
        :param save_name: 合并后保存的文件名称，需要用户传入
        :param file_dir: 需要合并的csv文件所在文件夹
        :param csv_encoding: csv文件编码, 默认 utf-8
        :return: None
        """
        # 合并后保存的文件路径 = 需要合并文件所在文件夹 + 合并后的文件名称
        self.save_path = os.path.join(file_dir, save_name)
        self.__check_name()
        # 指定编码
        self.encoding = csv_encoding
        # 需要合并的csv文件所在文件夹
        self.file_dir = file_dir
        self.__check_dir_exist(self.file_dir)
        # 文件路径列表
        self.file_list = [os.path.join(self.file_dir, i) for i in os.listdir(self.file_dir)]
        self.__check_singal_dir(self.file_list)
        # 合并到指定文件中
        print("开始合并csv文件 ！")
        for file in self.file_list:
            df = pd.read_csv(file, encoding=self.encoding)
            df.to_csv(self.save_path, index=False, quoting=1, header=not os.path.exists(self.save_path), mode='a')
            print(f"{file} 已经被合并到 {self.save_path} ！")
        print("所有文件已经合并完成 ！")

    def split_csv(self, csv_path, save_dir, split_line=100000, csv_encoding='utf-8'):
        """
        切分文件并获取csv文件信息。
        :param csv_path: csv文件路径
        :param save_dir: 切分文件的保存路径
        :param split_line: 按照多少行数进行切分，默认为10万
        :param csv_encoding: csv文件的编码格式
        :return: None
        """

        # 传入csv文件路径和切分后小csv文件的保存路径
        self.csv_path = csv_path
        self.save_dir = save_dir

        # 检测csv文件路径和保存路径是否符合规范
        self.__check_dir_exist(self.save_dir)
        self.__check_file_exist(self.csv_path)

        # 设置编码格式
        self.encoding = csv_encoding

        # 按照split_line行，进行切分
        self.split_line = split_line

        print("正在切分文件... ")

        # 获取文件大小
        self.file_size = round(os.path.getsize(self.csv_path) / 1024 / 1024, 2)

        # 获取数据行数
        self.line_numbers = 0
        # 切分后文件的后缀
        i = 0
        # df生成器，每个元素是一个df，df的行数为split_line，默认100000行
        df_iter = pd.read_csv(self.csv_path,
                              chunksize=self.split_line,
                              encoding=self.encoding,header=None)
        # 每次生成一个df，直到数据全部取玩
        for df in df_iter:
            # 后缀从1开始
            i += 1
            # 统计数据总行数
            self.line_numbers += df.shape[0]
            # 设置切分后文件的保存路径
            save_filename = os.path.join(self.save_dir, self.filename + "_" + str(i) + self.extension)
            # 打印保存信息
            print(f"{save_filename} 已经生成！")
            # 保存切分后的数
            df.to_csv(save_filename, index=False, encoding='utf-8', quoting=1)

        # 获取数据列名
        self.column_names = pd.read_csv(self.csv_path, nrows=10).columns.tolist()
        print("切分完毕！")

        return None

    def __check_dir_exist(self, dirpath):
        """
        检验 save_dir 是否存在，如果不存在则创建该文件夹。
        :return: None
        """
        if not os.path.exists(dirpath):
            raise FileNotFoundError(f'{dirpath} 目录不存在，请检查！')

        if not os.path.isdir(dirpath):
            raise TypeError(f'{dirpath} 目标路径不是文件夹，请检查！')

    def __check_file_exist(self, csv_path):
        """
        检验 csv_path 是否是CSV文件。
        :return: None
        """
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f'{csv_path} 文件不存在，请检查文件路径！')

        if not os.path.isfile(csv_path):
            raise TypeError(f'{csv_path} 路径非文件格式，请检查！')

        # 文件存在路径
        self.file_path_root = os.path.split(csv_path)[0]
        # 文件名称
        self.filename = os.path.split(csv_path)[1].replace('.csv', '').replace('.CSV', '')
        # 文件后缀
        self.extension = os.path.splitext(csv_path)[1]

        if self.extension.upper() != '.CSV':
            raise TypeError(f'{csv_path} 文件类型错误，非CSV文件类型，请检查！')

    def __check_name(self):
        """
        检查文件名称是否 .csv 结尾
        :return:
        """
        if not self.save_path.upper().endswith('.CSV'):
            raise TypeError('文件名称设置错误')

    def __check_singal_dir(self, file_list):
        """
        检查需要被合并的csv文件所在文件夹是否符合要求。
        1. 不应该存在除csv文件以外的文件
        2. 不应该存在文件夹。
        :return:
        """
        for file in file_list:
            if os.path.isdir(file):
                raise EnvironmentError(f'发现文件夹 {file}, 当前文件夹中存其他文件夹，请检查！')
            if not file.upper().endswith('.CSV'):
                raise EnvironmentError(f'发现非CSV文件：{file}, 请确保当前文件夹仅存放csv文件！')

def changeColumn(filepath,savepath):
    '''批量对csv进行换列'''
    filenames = get_file_names(filepath)
    for filename in filenames:
        print(
            f"-----------------------------------------process{filename}--------------------------------------------------")
        Data = pd.read_csv(filepath+filename, names=['Url', 'Class', "Content"],encoding='ISO-8859-1')
        Data = Data.loc[:, ['Url', 'Content', 'Class']]
        print(Data)
        return
        Data.to_csv(savepath+filename, header=None, index=None)
        os.remove(filepath+filename)

def reprocess(ori_path, result_path, filename):
    '''# 对访问后没有访问获取结果的csv进行处理重新放回未处理文件夹进行重新访问（处理单个文件）'''
    print(f"-----------------------------------------process{filename}--------------------------------------------------")
    Data = pd.read_csv(ori_path + filename,index=False, names=['Url','Class',"None", "Content"])
    print(Data)
    Data = Data.loc[:, ['Url', 'Content','Class']]
    # 删除Content列再重新写到csv文件里
    Data = Data.drop(columns=['Class'])
    print(Data)
    return
    Data.to_csv(result_path + filename, header=None, index=None)
    os.remove(ori_path + filename)

def mutreprocess(ori_path,result_path):
    '''对访问后没有访问获取结果的csv进行处理重新放回未处理文件夹进行重新访问（批量处理）'''
    for num in range(26,37):
        filename = f"train1_{str(num)}.csv"
        reprocess(ori_path, result_path, filename)

global driver
driver = Driver()


if __name__ == '__main__':
    pass
    # # # 划分csv文件
    # csv_path = r'data/test(unlabeled) (1).csv'
    # save_dir = r'data/test'
    # PyCSV().split_csv(csv_path, save_dir, split_line=2000)
    # ===================================================================================================
    # # 对处理完的csv文件进行换列
    # filepath = "./data/result/"
    # savepath = "./data/finalresult/"
    # changeColumn(filepath, savepath)
    # ===================================================================================================
    # # 对访问后没有访问获取结果的csv进行处理重新放回未处理文件夹进行重新访问（处理单个文件）
    # ori_path = './data/result/'
    # result_path = "./data/train1/"
    # filename = "train1_26.csv"
    # reprocess(ori_path, result_path, filename)
    # ===================================================================================================
    # 对访问后没有访问获取结果的csv进行处理重新放回未处理文件夹进行重新访问（批量单个文件）
    # mutreprocess(ori_path,result_path)