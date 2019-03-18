import scrapy
import logging
import random

from scrapy.crawler import CrawlerProcess
from Developer import *

engine = connect_sql()
class Metadata_Spider(scrapy.Spider):
    name = "Metadata"
    ## disable print log on powershell
    logging.getLogger('scrapy').propagate = False
    
    def start_requests(self):
        sql = """ select Link from vd_crawl_url where Crawl = 0 """
        df = pd.read_sql(sql, engine)
        for i in range(0, len(df)):
            url = df.iloc[i]['Link']

            yield scrapy.Request(url=url, callback=self.parse, dont_filter=True)

    def parse(self, response):
        # print(response.url)
        response = BeautifulSoup(response.body,"lxml")
        CName = get_CName(response)
        CDes = get_CDes(response)
        Label_des = get_metadata_from_text(response, '資料集描述')
        OrgName = get_metadata_from_text(response, '提供機關')
        Supplier = get_metadata_from_text(response, '提供機關聯絡人姓名')
        Datalicense = get_metadata_from_text(response, '授權方式')
        UpdateDT = get_metadata_from_text(response, '詮釋資料更新時間')
        CreateDT = get_metadata_from_text(response, '上架日期')
        Datatype = get_metadata_from_text(response, '資料集類型')
        Topic = get_metadata_from_text(response, '主題分類')
        Updatefreqency = get_metadata_from_text(response, '更新頻率')
        Category = get_metadata_from_text(response, '服務分類')
        nClick = get_nClick(response)
        nInlinks = get_nInlinks(response)
        Tags = get_Tags(response)
        query = get_query(CName, CDes, Label_des, OrgName, Supplier, Datalicense, UpdateDT, CreateDT, Datatype, Topic, Updatefreqency, Category, nClick, nInlinks, Tags)
        query2sql(query)
        

def get_CName(response):
    try:
        CName = response.find("", {"class": "node-title"}).text
    except:
        CName = " "
    CName = CName.replace('\'', '\'\'')
    return CName

def get_CDes(response):
    try:
        CDes = response.find(
                    "", {"class": "field field-name-field-content field-type-text-long field-label-inline clearfix"}).find("", {"class": "field-item"}).text.replace('\xa0', ' ')
    except:
        CDes = " "
    CDes = CDes.replace('\'', '\'\'')
    return CDes

def get_nClick(response):
    try:
        nClick = response.find("span", {"class": "view-count"}).text[6:]
    except:
        nClick = " "
    return nClick

def get_nInlinks(response):
    try:
        nInlinks = response.find("span", {"class": "dl-count"}).text[6:]
    except:
        nInlinks = " "
    return nInlinks

def get_Tags(response):
    tagString = ""
    try:
        for x in response.select('.tag-wrapper a'):
            tagString += str(x.text) + ','
        Tags = tagString[:-1]
    except:
        Tags = " "
    return Tags

def get_metadata_from_text(response, text):
    label = "{}:\xa0".format(text)
    try:
        metedata = str(response.find(text=label).parent.parent.text.replace(label,""))
    except:
        metedata = " "
    metedata = metedata.replace('\'', '\'\'')
    return metedata

def get_query(CName, CDes, Label_des, OrgName, Supplier, Datalicense, UpdateDT, CreateDT, Datatype, Topic, Updatefreqency, Category, nClick, nInlinks, Tags):
    sql = """
    SET NOCOUNT ON
    declare @State bit = 0
    declare @OID int
    declare @CName nvarchar(max) = '{}'
    declare @CDes nvarchar(max) = '{}'
    declare @nClick int = '{}'
    declare @nInlinks int = '{}'
    declare @Label_des nvarchar(max) = '{}'
    declare @OrgName nvarchar(100) = '{}'
    declare @Supplier nvarchar(100) = '{}'
    declare @Datalicense nvarchar(100) = '{}'
    declare @UpdateDT datetime = '{}'
    declare @CreateDT datetime = '{}'
    declare @Datatype nvarchar(100) = '{}'
    declare @Category nvarchar(100) = '{}'
    declare @Topic nvarchar(100) ='{}'
    declare @Updatefreqency nvarchar(100) = '{}'
    declare @Tags nvarchar(200) = '{}'
    exec xp_OpenData @CName ,@CDes, @nClick, @nInlinks, @Label_des, @OrgName, @Supplier, @Datalicense, @UpdateDT, @CreateDT, @Datatype, @Topic, @Category, @Updatefreqency, @Tags, @OID output
    update URL set Crawl = Crawl + 1 where UID = @OID
    """.format(CName, CDes, nClick, nInlinks, Label_des, OrgName, Supplier, Datalicense, UpdateDT, CreateDT, Datatype, Category, Topic, Updatefreqency, Tags)
    print(CName)
    return sql

user_agent = {
    'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
    'COOKIES_ENABLED':False,
    'DOWNLOAD_DELAY':3
}   
process = CrawlerProcess(user_agent)
process.crawl(Metadata_Spider)
process.start()