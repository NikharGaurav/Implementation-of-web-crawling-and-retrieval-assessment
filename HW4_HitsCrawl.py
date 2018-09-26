
# coding: utf-8

# In[1]:


from elasticsearch import Elasticsearch
import math
from elasticsearch_dsl import Search


# In[165]:


import math
import numpy


# In[215]:


from collections import defaultdict


# In[3]:


es = Elasticsearch()
s = Search(using=es, index="nuclear_disasters", doc_type='document')
s = s.source([])
docID = set(h.meta.id for h in s.scan())


# In[227]:


len(docID)


# In[229]:


page = es.search(
  index = 'nuclear_disasters',
  doc_type = 'document',
  size = 1598,
  body = {
    "query": {
        "match": {
            "text": "MAJOR NUCLEAR ACCIDENTS"
            }
        }
    })


# In[230]:


temp_url={}
root_set=set()
base_set=set()
count=0
for p in page['hits']['hits']:
    
    inlinks=set(p.get('_source').get("in_links"))
    outlinks=set(p.get('_source').get("out_links"))
    if len(outlinks)!=0 and len(inlinks)!=0:
        root_set.add(p.get('_id'))
        if len(base_set)<10000:
            base_set.add(p.get('_id'))
        for i in outlinks:
            if i in docID:
                if len(base_set)<=10000:
                    base_set.add(i)
    


# In[7]:


d=200
iteration=0
first=True
root_set=set()
base_set=set()

while len(base_set)<=10000:
    #getting the outlinks
    if first:
        for p in page['hits']['hits']:
            root_set.add(p.get('_source').get("docno"))
            base_set.add(p.get('_source').get("docno"))
            outlinks=set(p.get('_source').get("out_links"))
            for i in outlinks:
                if i in docID:
                    if len(base_set)<=10000:
                        base_set.add(i)
            
        temp_set=set()
        for p in page['hits']['hits']:
            inlinks=set(p.get('_source').get("in_links"))
            for i in inlinks:
                if i in docID:
                    temp_set.add(i)
            if len(temp_set)<200:
                if len(base_set)<=10000:
                    base_set=set(list(temp_set)+list(base_set))
                    root_set=set(list(temp_set)+list(root_set))
            else:
                if len(base_set)<=10000:
                    base_set=set(list(random.sample(temp_set, 200))+list(base_set))
                    root_set=set(list(random.sample(temp_set, 200))+list(root_set))
    
    else:
        iteration=iteration+1
        temp_set=set()
        outlinks=set()
        inlinks=set()
        for doc in base_set:
            page=es.get(
            index = 'nuclear_disasters',
            doc_type = 'document',
            id=doc,
            ignore=[404,400]
                )
            outlinks=set(list(outlinks)+list(page.get('_source').get("out_links")))
            inlinks=set(list(inlinks)+list(page.get('_source').get("out_links")))

        for i in outlinks:
            if i in docID:
                base_set.add(i)

        for i in inlinks:
            if i in docID:
                temp_set.add(i)

    #     base_set=set(list(temp_set)+list(base_set))
    #     root_set=set(list(temp_set)+list(root_set))

        if len(temp_set)<200:
            base_set=set(list(temp_set)+list(base_set))
            root_set=set(list(temp_set)+list(root_set))
        else:
            base_set=set(list(random.sample(temp_set, 200))+list(base_set))
            root_set=set(list(random.sample(temp_set, 200))+list(root_set))
        print("iteration:"+str(iteration))
        print(str(len(base_set)))
        print(str(len(root_set)))


# In[233]:


page_hub={}
page_auth={}
inlink_count=0
outlink_count=0
inlinks_url={}
outlinks_url={}
both_count=0
for doc in base_set:
    page=es.get(
        index = 'nuclear_disasters',
        doc_type = 'document',
        id=doc,
        ignore=[404,400]
    )
    inlinks_x=page['_source']['in_links']
    outlinks_x=page['_source']['out_links']
    if len(inlinks_x)==0:
        inlink_count+=1
    
    if len(outlinks_x)==0:
        outlink_count+=1
    
    if len(inlinks_x)==0 and len(outlinks_x)==0:
        both_count+=1
    
    for inlink in inlinks_x:
        try:
            inlinks_url[doc].append(inlink)
            page_hub[inlink]=1
            page_auth[inlink]=1
        except:
            inlinks_url[doc]=[inlink]
            page_hub[inlink]=1
            page_auth[inlink]=1
    
    for outlink in outlinks_x:
        try:
            outlinks_url[doc].append(outlink)
            page_hub[outlink]=1
            page_auth[outlink]=1
        except:
            outlinks_url[doc]=[outlink]
            page_hub[outlink]=1
            page_auth[outlink]=1
                
    




# In[113]:


outlink_count


# In[115]:


len(inlinks_url.keys())


# In[116]:


len(outlinks_url.keys())


# In[121]:


len(page_auth.keys())


# In[232]:


page_hub= defaultdict(lambda:1.0)
page_auth= defaultdict(lambda:1.0)
for url in docID:
    page_hub[url]=1
    page_auth[url]=1


# In[235]:


initial_hubperplex=0
initial_authperplex=0
hubconvergence=0
authconvergence=0
iteration=0

while hubconvergence < 4 and authconvergence < 4:
    new_auth = defaultdict(lambda:0)
    new_hub = defaultdict(lambda:0)
    total_hubentropy=0
    total_authentropy=0
    
    norm=0
    
    for url in root_set:
        try:
            for in_link in inlinks_url[url]:
                    new_auth[url]=new_auth[url]+ page_hub[in_link]
                
        except:
            continue

        norm+=pow(new_auth[url],2)
    norm=math.sqrt(norm)
    for url,auth in new_auth.items():
        new_auth[url]=auth/norm
        try:
            total_authentropy=total_authentropy+ math.log(new_auth[url])
        except:
            total_authentropy=total_authentropy+ numpy.nextafter(0,1)
    
    norm=0
            
    
    for url in base_set:
        if url in outlinks_url:
            for out_link in outlinks_url[url]:
                try:
                    new_hub[url]=new_hub[url]+ new_auth[out_link]
                
                except:
                    continue
            norm=norm + pow(new_hub[url],2)
    norm=math.sqrt(norm)
    
    for url, hub in new_hub.items():
        new_hub[url] = hub/norm

        
        try:
            total_hubentropy=total_hubentropy+math.log(new_hub[url],2)
        except:
            total_hubentropy=total_hubentropy+numpy.nextafter(0,1)
    
    page_auth, page_hub=new_auth, new_hub
    
    new_authperplex=pow(2,total_authentropy)
    new_hubperplex=pow(2,total_hubentropy)
    
    iteration+=1
    print("iteration: "+str(iteration))
    
    if(abs(new_hubperplex-initial_hubperplex)<1):
        hubconvergence+=1
    else:
        hubconvergence=0
    
    if(abs(new_authperplex-initial_authperplex)<1):
        authconvergence+=1
    else:
        authconvergence=0
    
    
    initial_hubperplex=new_hubperplex
    initial_authperplex=new_authperplex
    


# In[239]:


rank = 1
sorted_list = sorted(page_hub, key=lambda x: page_hub[x],reverse=True)

fw = open("C:/Users/Nikhar/Downloads/Assignment/InformationRetrievalCS6200/HW4/output/page_hub.txt" , "a")
for j in sorted_list:
    
    fw.write(str(rank)+":"+j+" "+str(page_hub[j])+"\n")
    rank = rank + 1
    if(rank==501):
        break
fw.close()


# In[241]:


rank = 1
sorted_list = sorted(page_auth, key=lambda x: page_auth[x],reverse=True)

fw = open("C:/Users/Nikhar/Downloads/Assignment/InformationRetrievalCS6200/HW4/output/page_authority.txt" , "a")
for j in sorted_list:
    
    fw.write(str(rank)+":"+j+" "+str(page_auth[j])+"\n")
    rank = rank + 1
    if(rank==501):
        break
fw.close()


# In[240]:


fw.close()

