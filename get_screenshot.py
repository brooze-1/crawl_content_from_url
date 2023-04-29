from PIL import Image
from utils_.utils import processUrl
from selenium import webdriver
from fake_useragent import UserAgent
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

def Driver():
    '''获取浏览器驱动对象'''
    dcap = dict(DesiredCapabilities.PHANTOMJS)
    # 设置user-agent请求头
    dcap["phantomjs.page.settings.userAgent"] = UserAgent().random
    service_args = []
    service_args.append('--disk-cache=yes')  ##开启缓存
    service_args.append('--ignore-ssl-errors=true')  ##忽略https错误
    driver = webdriver.PhantomJS(service_args=service_args,desired_capabilities=dcap)
    driver.set_window_size(1300, 800)
    driver.set_page_load_timeout(30)
    return driver

def get_screenshot(url,screenshot_save_path):
    '''获取网页截图'''
    try:
        url = processUrl(url)
        driver.get(url)
        # wait = WebDriverWait(browser, 10)
        # # 设置判断条件：等待id='kw'的元素加载完成
        # input = wait.until(EC.presence_of_all_elements_located)
        # print("加载完成！")
    except:
        # print("加载超时！")
        driver.execute_script('window.stop()')
    finally:
        # browser.find_element_by_id("idClose").click()
        # time.sleep(5)
        # print("正在保存截图..")
        try:
            driver.save_screenshot(screenshot_save_path+str(1)+".png")
            # print("----------------保存成功---------------------")            # print("正在处理网页截图...")
            im=Image.open(screenshot_save_path+str(1)+".png")
            left = 0
            top = 0
            right = 1300
            bottom = 800
            im=im.crop((left,top,right,bottom))
            im.save(screenshot_save_path+str(1)+".png")
            img = Image.open(screenshot_save_path+str(1)+".png")
            img.show()
            print("sceenshot:截图保存成功！！！")
        except Exception as e:
            print(f"截图保存失败,catch error:{e}")
    driver.quit()

global driver
driver = Driver()

if __name__ == '__main__':
    screenshot_save_path = "../web_page_screenshot/"
    url = "www.baidu.com"
    get_screenshot(url,screenshot_save_path)
