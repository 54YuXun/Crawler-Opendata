import scrapy
import logging

from scrapy.crawler import CrawlerProcess
from Developer import *

engine = connect_sql()
class Entrance_Spider(scrapy.Spider):
    name = "Entrance"
    start_urls = ['https://data.gov.tw/datasets/search?qs=&order=pubdate&type=dataset']
    ## disable print log on powershell
    logging.getLogger('scrapy').propagate = False

    def parse(self, response):
        response = BeautifulSoup(response.body,"lxml") 
        check_url_queue()
        get_class_from_sql()
 
        sql = """ select ID, Link from URL_queue where Times = 0 """
        df = pd.read_sql(sql, engine)
        for i in range(0, len(df)):
            url = df.iloc[i]['Link']
            CID = df.iloc[i]['ID']
            yield scrapy.Request(url, callback=self.get_web_title_link, meta={'CID':CID})
    
    def get_web_title_link(self, response):
        gov_url = "https://data.gov.tw" 
        cid = response.meta['CID']
        url = response.url
        response = BeautifulSoup(response.body, "lxml")
        for data in response.select('.node-header h2 a'):
            title = str(data.text.replace('\'', '\'\''))
            url2 = gov_url + str(data.get('href'))
            query = get_query_inserturl(title, url2, url, cid)
            query2sql(query)
            print(title)
        modify_times = "update URL_queue set Times = Times + 1 where Link = '{0}'".format(url)
        query2sql(modify_times)
        

def check_url_queue():
    sql = """   
    if not exists(select * from INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'URL_queue')
    begin
        create table URL_queue
        (
            ID int null,
            Category nvarchar(10) null,
            Page int null,
            Link nvarchar(2000) null,
            Times int not null default(0),
            Total int not null default(0)
        )
    end
    """
    query2sql(sql)
    print('check_url_queue success')

def get_class_from_sql():
    sql = "select CName, Keywords, nObject  from class where Namepath like '%服務分類%' and nLevel > 1"  
    df = pd.read_sql(sql, engine)

    for i in range(0, len(df)):
        CName = df.iloc[i]['CName']
        link = df.iloc[i]['Keywords']
        count = df.iloc[i]['nObject']

        url = 'https://data.gov.tw/datasets/search?qs={0}&order=pubdate&page='.format(link)
        page = math.floor(count/15)
        for i in range(0, page+1):
            url2 = url + str(i)
            query2 = get_query_insert_url_queue(CName, i, url2)
            print(query2)
            # query2sql(query2)

def get_query_insert_url_queue(CName, page, link):
    sql = """
    SET NOCOUNT ON
    declare @Category nvarchar(10) = '{0}'
    declare @Web_page int = '{1}'
    declare @Link nvarchar(2000) = '{2}'
    exec xp_insert_url_queue @Category, @Web_page, @Link
    """.format(CName, page, link)
    sentence = "    分類: {} 第{}頁".format(CName, page)
    # print(sentence)
    return sql

def get_query_inserturl(title, url2, url, cid):
    sql = """ 
    SET NOCOUNT ON
    declare @UID_out int
    declare @CName nvarchar(800) = '{0}'
    declare @CDes nvarchar(800) = NULL
    declare @Scheme nvarchar(10) = (select [dbo].[fn_urlScheme]('{1}'))
    declare @HostName nvarchar(80) = (select [dbo].[fn_urlHost]('{1}'))
    declare @Path nvarchar(1000) = (select [dbo].[fn_urlPath]('{1}'))
    declare @MID int = 1
    declare @Keywords nvarchar(255) = NULL
    exec xp_insertUrl @CName, @CDes, @Scheme, @HostName, @Path, @MID, @Keywords
    set UID_out = @@Identity
    update object set type = 101 where oid = @UID_out
    update URL set Crawl = 0, Weight = 0 where UID = @UID_out
    update URL_queue set Total = Total + 1 where Link = '{2}'
    
    if not exists (select * from CO where CID = {3} and OID = @UID_out)
    begin
        insert into CO(CID, OID, Since)Values({3}, @UID_out, getdate())
    end
    """.format(title, url2, url, cid)
    return sql

user_agent = {
    'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
    'COOKIES_ENABLED':False,
    'DOWNLOAD_DELAY':3
}  
process = CrawlerProcess(user_agent)
process.crawl(Entrance_Spider)
process.start()

    