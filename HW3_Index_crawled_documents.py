
# coding: utf-8

# In[17]:


from elasticsearch import Elasticsearch    
from os import listdir
import glob
from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer


# In[7]:


import re


# In[18]:


es=Elasticsearch()


# In[19]:


path='C:/Users/Nikhar/Downloads/Assignment/InformationRetrievalCS6200/HW3/crawled_documents_final'
filenames=listdir(path)


# In[20]:


inlink_path=open('C:/Users/Nikhar/Downloads/Assignment/InformationRetrievalCS6200/HW3/crawled_documents_final/inlinks_outlinks/inlinks1.txt','r')
inlinks_x=inlink_path.readlines()


# In[31]:


inlink_path.close()


# In[21]:


inlinks_url={}
for line in inlinks_x:
    line_urls=line.split(':[')
    inlinks_url[line_urls[0]]= list(set(line_urls[1].strip(']\n').replace("'","").replace(' ','').split(',')))


# In[32]:


outlink_path.close()


# In[22]:


outlink_path=open('C:/Users/Nikhar/Downloads/Assignment/InformationRetrievalCS6200/HW3/crawled_documents_final/inlinks_outlinks/Outlinks.txt','r')
outlink_x=outlink_path.readlines()


# In[23]:


outlinks_url={}
for line in outlink_x:
    line_urls=line.split(':[')
    outlinks_url[line_urls[0]]=list(set(line_urls[1].strip(']\n').replace("'","").replace(' ','').split(',')))




count=0
merge=0
for file in filenames:
    if file :
        crawled_file = open(path+"/"+file)
        filedata=crawled_file.read()
        validPage = "<root>" + filedata + "</root>"
        
        soup = BeautifulSoup(validPage, 'html.parser')
        docno_x = soup.find('docno')
        for text in docno_x:
            docno=text
        
        
        

        docno=docno.replace('https','http')
        
        head_x = soup.find('head')
        for text in head_x:
            head=text
        header_x = soup.find('header')
        for text in header_x:
            header=text
        texts = soup.find_all('text')
        texts=str(texts)
#         string_text=''
#         for txt in texts:
#             string_text=txt
        texts=texts.replace('[','').replace('<text>','').replace('\\n','').replace(']','').replace('</text>','')
        author_x = soup.find_all('author')
        for text in author_x:
            author=text
        author=str(author)
        author=author.replace('<author>','').replace('</author>','')
        content_x = soup.find_all('content')
        content_x=str(content_x)
        content_x=content_x.replace('<content>','').replace('</content>','').replace('\\n','').replace('[','').replace(']','')
        
        try:
            inlinks_x=inlinks_url[docno]
        except:
            inlinks_x=""
        try:
            outlinks_x=outlinks_url[docno]
        except:
            outlinks_x=""
#         print(docno)
#         docno='abc123'

        try:
            input_text= es.get(index='nuclear_disasters',doc_type="document",id=docno, ignore=[404,400])
            inlink_temp = input_text['_source']['in_links']
            new_inlinks = list(set(inlink_temp + inlinks_x))
            outlink_temp = input_text['_source']['out_links']
            new_outlinks = list(set(outlink_temp + outlinks_x))
            body = {
                'doc': {
                    'in_links': new_inlinks,'out_links':new_outlinks,
                    'author': input_text['_source']['author'] + ';' + 'Nikhar'
                }
            }

            es.update(index='nuclear_disasters',doc_type="document",id=docno,body=body, ignore=[404,400])
            merge=merge+1
            print("merge:"+str(merge)+":"+str(docno))
        
        except:


            doc={
                "docno" : docno,
                "HTTPheader" : header,
                "title" : head,
                "text" : texts,
                "html_Source" : content_x,
                "in_links" : inlinks_x,
                "out_links" : outlinks_x,
                "author": author


            }
            res=es.index(index='nuclear_disasters', doc_type='document', id=docno, body=doc,ignore=[404,400])
        
        count=count+1
        if(count%100==0):
            print(count)
#         break
        

