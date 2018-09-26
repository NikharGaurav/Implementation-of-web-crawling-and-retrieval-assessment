
# coding: utf-8

# In[1]:


from elasticsearch import Elasticsearch  
from elasticsearch_dsl import Search


# In[2]:


import math


# In[3]:


es=Elasticsearch()


# In[4]:


s = Search(using=es, index="nuclear_disasters", doc_type='document')
s = s.source([])
docID = set(h.meta.id for h in s.scan())


# In[7]:


len(docID)


# In[40]:


doc=list(docID)[2505]


# In[36]:


for doc in docID:
    print(doc)
    break


# In[68]:


doc='http://en.wikipedia.org/wiki/Chernobyl_disaster'


# In[69]:


page=es.get(
  index = 'nuclear_disasters',
  doc_type = 'document',
  id=doc)


# In[70]:


inlinks=page['_source']['in_links']


# In[74]:


page['_source']['out_links']


# In[72]:


for i in inlinks:
    print(i)


# In[71]:


page['_source']['docno']


# In[9]:


pages_all=set()
inlinks_url={}
outlinks_url={}
for doc in docID:
    page=es.get(
        index = 'nuclear_disasters',
        doc_type = 'document',
        id=doc,
        ignore=[404,400]
    )
    inlinks_x=page['_source']['in_links']
    if len(inlinks_x)>0:
        inlinks_url[doc]=list(inlinks_x)
    
    pages_all.add(doc)
    

len(pages_all)


# In[10]:


len(inlinks_url)


# In[11]:


outlinks_count={}
for p in pages_all:
    outlinks_count[p]=0


# In[12]:


for v in inlinks_url.values():
    for i in v:
        if i in outlinks_count:
            outlinks_count[i]+=1


# In[13]:


#getting the sink nodes
sink_nodes=[]
for key in outlinks_count.keys():
    if (outlinks_count[key]==0):
        sink_nodes.append(key)


# In[15]:


len(sink_nodes)


# In[16]:


tot_pages=len(pages_all)


# In[17]:


tot_pages


# In[24]:


page_rank={}
for page in pages_all:
    page_rank[page]=float(1/tot_pages)


# In[25]:


initial_perplexity=0
new_page_rank={}
d=0.85
convergence=0
while convergence<=4:
    sink_pr=0
    total_entropy=0
    for p in sink_nodes:
        sink_pr=sink_pr+page_rank[p]
    for p in pages_all:
        new_page_rank[p]=(1-d)/tot_pages
        new_page_rank[p]=new_page_rank[p]+d*sink_pr/tot_pages
        if p in inlinks_url:
            for inlinks in inlinks_url[p]:
                try:
                    new_page_rank[p]=new_page_rank[p]+d*page_rank[inlinks]/outlinks_count[inlinks]
                except :
                    continue
                    
    for p in pages_all:
        page_rank[p]=new_page_rank[p]
        total_entropy=total_entropy+page_rank[p]*math.log(1/page_rank[p],2)
    perplexity=pow(2,total_entropy)
    if abs(perplexity-initial_perplexity)<1:
        convergence=convergence+1
    else:
        convergence=0
    initial_perplexity=perplexity


# In[26]:


rank = 1
sorted_list = sorted(page_rank, key=lambda x: page_rank[x], reverse=True)
#     rank = 1
for j in sorted_list:
    print(str(rank)+":"+j)
    rank = rank + 1
    if(rank==501):
        break


# In[50]:


inlinks_url


# In[ ]:


#creating a dictionary of inlinks and outlinks
pages_all=set()
inlinks_url={}
outlinks_url={}
for line in wt2g_inlinks_lines:
    urls=line.replace('\n','').strip(' ').split(' ')
    inlinks_url[urls[0]]=urls[1:]
    
    pages_all.add(urls[0])
    
    for inlinks in inlinks_url[urls[0]]:
        pages_all.add(inlinks)
    


# In[31]:


inlinks_url['http://www.mediawiki.org']


# In[41]:


doc='http://en.wikipedia.org/wiki/Lists_of_nuclear_disasters_and_radioactive_incidents'
page_rank[doc]


# In[42]:


doc='http://en.wikipedia.org/wiki/Chernobyl_disaster'
page_rank[doc]


# In[43]:




