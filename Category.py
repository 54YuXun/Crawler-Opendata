import scrapy
import logging

from scrapy.crawler import CrawlerProcess
from Developer import *

class Category_Spider(scrapy.Spider):
    name = "Category"
    start_urls = ['https://data.gov.tw/datasets/search?qs=&order=pubdate&type=dataset']
    ## disable print log on powershell
    logging.getLogger('scrapy').propagate = False

    def parse(self, response):
        response = BeautifulSoup(response.body,"lxml") 
        
        category_list = ["服務分類", "檔案格式"]
        parse_category(response, category_list)
        gov_dic = {"地方機關":"dataset_tree_agency_21001", "中央機關":"dataset_tree_agency_21000", "法人機構":"dataset_tree_agency_21002"}
        parse_office(response, gov_dic)

## Category Case
def get_category_elements(data):
    category_name = str(data.get('data-qss'))
    category_link = str(data.get('data-qs'))
    category_count = str(data.find("", {"class": "facet-count"}).text)
    sentence = "    {0} {1}".format(category_name, category_count)
    print(sentence)
    return category_name, category_link, category_count

def parse_category(response, category_list):
    for title in category_list:
        sql_create_category(title)
        data = response.find(text=title).parent.parent.find_all("a", {"class": "facet-item"})
        for i in data:
            category_name, category_link, category_count = get_category_elements(i)
            sql_write_category(title, category_name, category_link, category_count)

def sql_create_category(title):
    sql = """ 
        declare @CID int = (select CID from Class where CName = N'iLOD')
        declare @newCID int 
        if not exists (select * from class where CName = N'政府資料開放平台')
        begin
            exec xp_insertClass  @CID, 101, N'政府資料開放平台', 'data.gov.tw', @newCID output
            if not exists (select * from class where CName = '{0}')
            begin
                exec xp_insertClass @newCID, 101, N'{0}', null, @newCID output
            end
        end
        else 
        begin
            set @newCID = (select CID from Class where CName = N'政府資料開放平台')
            exec xp_insertClass @newCID, 101, N'{0}', null, @newCID output
        end
    """.format(title)
    # print(sql)
    query2sql(sql)

## Office Case
def get_office_elements(data):
    office_name = str(data.find("a").get('data-qss'))
    office_link = str(data.find("a").get('data-qs'))
    office_count = str(data.find("a").find("", {"class": "facet-count"}).text)
    sentence = "    {0} {1}".format(office_name, office_count)
    print(sentence)
    return office_name, office_link, office_count

def parse_office(response, gov_dic):
    for title, link in gov_dic.items():
        sql_create_category(title)
        data = response.find("div", {"id": link})
        for level1 in data.find('ul'):
            office_name1, office_link1, office_count1 = get_office_elements(level1)
        ## len(i) = 1 --> 無subclass ； len(i) = 2 --> 有subclass    
            if len(level1) == 1:
                # print(office_name1)
                sql_write_category(title, office_name1, office_link1, office_count1)
            if len(level1) == 2:
                # print(office_name1)
                sql_write_category(title, office_name1, office_link1, office_count1)
        
                for level2 in level1.find_all('li'):
                    office_name2, office_link2, office_count2 = get_office_elements(level2)
                    if len(level2) ==1:               
                        # print(office_name1+','+office_name2)
                        sql_write_category(office_name1, office_name2, office_link2, office_count2)
                    if len(level2) ==2: 
                        # print(office_name1+','+office_name2)
                        sql_write_category(office_name1, office_name2, office_link2, office_count2)

                        for level3 in level2.find_all('li'):
                            office_name3, office_link3, office_count3 = get_office_elements(level3)
                            if len(level3) ==1:
                                # print(office_name1+','+office_name2+','+office_name3)
                                sql_write_category(office_name2, office_name3, office_link3, office_count3)
                            if len(level3) ==2:
                                # print(office_name1+','+office_name2+','+office_name3)
                                sql_write_category(office_name2, office_name3, office_link3, office_count3)

                                for level4 in level3.find_all('li'):
                                    office_name4, office_link4, office_count4 = get_office_elements(level4)
                                    if len(level4) ==1:
                                        # print(office_name1+','+office_name2+','+office_name3+','+office_name4)
                                        sql_write_category(office_name3, office_name4, office_link4, office_count4)
                                    if len(level4) ==2:
                                        # print(office_name1+','+office_name2+','+office_name3+','+office_name4)
                                        sql_write_category(office_name3, office_name4, office_link4, office_count4)
                    
                                        for level5 in level4.find_all('li'):
                                            office_name5, office_link5, office_count5 = get_office_elements(level5)
                                            if len(level5) ==1:
                                                # print(office_name1+','+office_name2+','+office_name3+','+office_name4+','+office_name5)
                                                sql_write_category(office_name4, office_name5, office_link5, office_count5)
                                            if len(level5) ==2:
                                                # print(office_name1+','+office_name2+','+office_name3+','+office_name4+','+office_name5)
                                                sql_write_category(office_name4, office_name5, office_link5, office_count5)

def sql_write_category(title, name, link, count):

    sql = """
        declare @newCID int
        declare @CID int
        declare @CID2 int

        select @CID = CID from class where CName = '{0}' and type = 101
        if not exists (select * from class where Cname = '{1}' and type = 101)
        begin
            exec dbo.xp_insertClass @CID, 101, '{1}', null, @newCID out    
            update Class set Keywords = '{2}'  where CID = @newCID
            update Class set nObject = '{3}'  where CID = @newCID
        end
        else
        begin
            select @CID2 = CID from class where CName = '{1}'
            update Class set Keywords = '{2}'  where CID = @CID2
            update Class set nObject = '{3}'  where CID = @CID2
        end
    """.format(title, name, link, count)
    # print(sql)
    query2sql(sql)

user_agent = {
    'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
    'COOKIES_ENABLED':False,
    'DOWNLOAD_DELAY':3
}   
process = CrawlerProcess(user_agent)
process.crawl(Category_Spider)
process.start()
