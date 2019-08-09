#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import json
import pymongo
from bs4 import BeautifulSoup
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


# ### Mysql

# In[ ]:


# engine


# In[2]:


mysql_client = create_engine("mysql://root:dssf@13.125.137.6/terraform?charset=utf8")
mysql_client


# In[ ]:


# Model


# In[26]:


base = declarative_base()

class NaverKeywords(base):
    __tablename__ = "naver"
    
    id = Column(Integer, primary_key=True)
    rank = Column(Integer, nullable=False)
    keyword = Column(String(50), nullable=False)
    rdate = Column(TIMESTAMP, nullable=False)
    
    def __init__(self, rank, keyword):
        self.rank = rank
        self. keyword = keyword
        
    def __repr__(self):
        return "<NaverKeyword {}, {}>".format(self.rank, self.keyword)


# In[3]:


# crawling code


# In[20]:


def crawling():
    response = requests.get("https://naver.com")
    dom = BeautifulSoup(response.content, "html.parser")
    keywords = dom.select(".ah_roll_area > .ah_l > .ah_item")
    datas = []
    for keyword in keywords:
        rank = keyword.select_one(".ah_r").text
        keyword = keyword.select_one(".ah_k").text
        datas.append((rank, keyword))
    return datas


# In[21]:


# test code
datas = crawling()
datas[-2:]


# In[22]:


# save mysql


# In[31]:


def save_mysql(datas, mysql_client=mysql_client):
    
    keywords = [NaverKeywords(rank, keyword) for rank, keyword in datas]
    
    # make session
    maker = sessionmaker(bind=mysql_client)
    session = maker()
    
    # save datas
    session.add_all(keywords)
    session.commit()
    
    session.close()


# In[27]:


# 테이블 생성
base.metadata.create_all(mysql_client)


# In[ ]:


datas = crawling()
save_mysql(datas, mysql_client)


# ### Mongodb

# In[13]:


mongo_client = pymongo.MongoClient("mongodb://13.125.137.6:27017")
mongo_client


# In[14]:


def save_mongo(datas, mongo_client=mongo_client):
    querys = [{"rank":rank, "keyword":keyword} for rank, keyword in datas]
    mongo_client.terraform.naver.insert(querys)


# In[15]:


# test code


# In[16]:


datas = crawling()
save_mongo(datas, mongo_client)


# ### send slack

# In[37]:


def send_slack(msg, channel="#dss", username="provision_bot" ):
    webhook_URL = "https://hooks.slack.com/services/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"
    payload = {
        "channel": channel,
        "username": username,
        "icon_emoji": ":provision:",
        "text": msg,
    }
    response = requests.post(
        webhook_URL,
        data = json.dumps(payload),
    )
    return response


# In[38]:


def run():
    
    # 데이터 베이스에 테이블 생성
    base.metadata.create_all(mysql_client)

    # 네이버 키워드 크롤링
    datas = crawling()

    # 데이터 베이스에 저장
    save_mysql(datas)
    save_mongo(datas)

    # 슬랙으로 메시지 전송
    send_slack("naver crawling done!")


# In[39]:


run()


# In[ ]:




