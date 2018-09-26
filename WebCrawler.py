
# coding: utf-8

# In[1]:


from urllib.request import urlopen

import time
import traceback
from bs4 import BeautifulSoup
import requests
from urllib.parse import urlparse
from urllib import robotparser


# In[11]:








# In[2]:


#function to conanicalize the url but need to check for rule number 3
#about https://en.wikipedia.org/wiki/Template:Milestone_nuclear_explosions with : in it
def canonicalize(url,include_scheme=True, get_domain=False):
#     url="http://www.example.com//a.html/abssdc/dsf#anything"
    parse=urlparse(url)
    scheme=parse.scheme.lower()
    domain=parse.netloc.lower()
    if scheme == 'http':
        domain=domain.split(':')
        domain=domain[0]
    elif scheme == 'https':
        domain=domain.split(':')
        domain=domain[0]
    path=parse.path.replace('//','/')
    if get_domain:
        return(scheme+'://'+domain)
    
    if include_scheme:
        return(scheme+'://'+domain+path)
    


# In[3]:


#checking with the robot.txt file of website
#if file is present in robot.txt of website it returns 1
def validate_robot(url):
    robotParser = robotparser.RobotFileParser()
    if robotParser.set_url(canonicalize(url,get_domain=True)+'/robots.txt')!= None:
        robotParser.read()
        valid_url=robotParser.can_fetch(url,'*')
    else:
        valid_url=True
    return valid_url
    



# In[27]:


#spider function to crawl pages with bfs
def spider(seed_url,max_pages,bfs_depth):
    #creating an array of links which have not been crawled
    pages_not_crawled=seed_url
    #array of pages already crawled
#     pages_crawled=[]
    #array for each depth of bfs
    next_depth=[]
    #depth of bfs
    depth=0
    
    while pages_not_crawled and len(pages_crawled)<max_pages and depth<bfs_depth:
        page=pages_not_crawled.pop(0)
#         print(page)
        page=canonicalize(page)
        #checking to make sure a page gets crawled only once
        if page not in pages_crawled:

            
            urls = get_all_urls(page)
            #updating link for next depth
            for link in urls:
                if link not in next_depth:
                    next_depth.append(link)
            pages_crawled.append(page)
            if (len(pages_crawled)%100==0):
                print(str(len(pages_crawled))+" "+str(time.time()))
            
            #if the array for links to be crawled is empty we update the next depth
            if not pages_not_crawled:
                pages_not_crawled, next_depth = next_depth, []
                #writing the whole page at once
                depth=depth+1

                if depth>0:
                    length_inlinks=[]
                    
                    for i in pages_not_crawled:
                        length_inlinks.append(len(url_inlink[i]))
                    pages_not_crawled = [pages_not_crawled for _,pages_not_crawled in sorted(zip(length_inlinks,pages_not_crawled),reverse=True)]


                
    return 0
            
            


# In[8]:


#function to crawl through pages
def get_all_urls(page):
    #list of url on current page
    list_url=[]
    try:
#         print(page)
        open_url=requests.get(page)
        time.sleep(1)
        soup1=BeautifulSoup(open_url.text,"html.parser")
        soup2=BeautifulSoup(open_url.text,"html.parser")
        headers=soup2.find_all('header')
        header=open_url.headers
        
        
        #finding all the links within the page
        links = soup1.find_all('a', href = True)
#         if page not in url_outlink.keys():
#                 url_outlink[page]={}
#                 url_outlink_count[page]=0
        #calling function to write content of web page
        
        for link in links:
            link1=link.get('href')
#             link1=canonicalize(link1)
            #removing all the useless texts
            if ':' not in link1 and '.au' not in link1 and '.class' not in link1 and '.jsp' not in link1 and 'javascript' not in link1 and 'special:' not in link1 and 'action=' not in link1 and '.apk' not in link1 and '%' not in link1 and'.pdf' not in link1 and '.jpg' not in link1 and '.png' not in link1 and '#' not in link1 and 'Main_Page' not in link1 and 'index' not in link1 and "?" not in link1 and "=" not in link1 and link1 != "" and link1 != '/':
                if '.jpeg' not in link1 and '.mpg' not in link1 and '.mpeg' not in link1 and '.mpa' not in link1 and '.mp4' not in link1 and '.ra' not in link1 and 'pdf' not in link1 and '.xml' not in link1 and '.tdf' not in link1 and '.001' not in link1 and '.301' not in link1 and '.mp3' not in link1 and '.svg' not in link1 :
                    
                    if '/wiki/' in link1.lower():
                        link1='https://en.wikipedia.org'+link1
                        if validate_robot(link1):
                            link1=canonicalize(link1)
                            
                            #need to check if link works or not
                            if link1 !=page:
                                list_url.append(link1)
                                try:
                                    url_outlink[page].append(link1)
                                except:
                                    url_outlink[page]=[]
                                    url_outlink[page].append(link1)
                                    
                                try:
                                    url_inlink[link1].append(page)
                                except:
                                    url_inlink[link1]=[]
                                    url_inlink[link1].append(page)

                    elif urlparse(link1).scheme!='':
                        link1=canonicalize(link1)
                        if validate_robot(link1):
                            link1=canonicalize(link1)
#                             list_url.append(link1)
                            if link1 !=page:
                                list_url.append(link1)
                                try:
                                    url_outlink[page].append(link1)
                                except:
                                    url_outlink[page]=[]
                                    url_outlink[page].append(link1)
                                    
                                try:
                                    url_inlink[link1].append(page)
                                except:
                                    url_inlink[link1]=[]
                                    url_inlink[link1].append(page)
            
#         write_contents(soup2,page,header)
        
    except Exception:
        
        
        print ("Error in try block of get_all_urls!")
        print (traceback.format_exc())
        print(page)
        print(type(page))
        pass
#         continue
    return list_url
            
        


# In[37]:



def write_contents(soup,page,header):
    global FILE_INDEX
    filename= 'file_' + str(FILE_INDEX) + '.txt'
    path='C:/Users/Nikhar/Downloads/Assignment/InformationRetrievalCS6200/HW3/crawled_documents2/' + filename
    file_content = open(path, 'w')
    text='' 
#     soup=BeautifulSoup(open_url.text,"html.parser")
    title=str((soup.find_all('title')[0]))
    title=str(title.replace('<title>','')).replace('</title>','')
    html_source=soup

#     soup=BeautifulSoup(open_url.text,"html.parser")
    for lImages in soup.find_all("div", {"class": "thumb tleft"}):
        lImages.decompose()
    for rImages in soup.find_all("div", {"class": "thumb tright"}):
        rImages.decompose()
    for sup in soup.find_all('sup', {'class': 'reference'}):
        sup.decompose()
    for tables in soup.find_all("table", {"class": "vertical-navbox nowraplinks"}):
        tables.decompose()
    for para in soup.find_all("p"):
        text += para.text + " "
    #storing the URL long with the page content
    final_text="<DOC>"+"<DOCNO>"+page+"</DOCNO>\n"+"<HEAD>"+ title+"</HEAD>\n"+"<HEADER>"+str(header) +"</HEADER>\n" + "<TEXT>"+text+"</TEXT>\n"+"<CONTENT>"+html_source.decode('utf-8')+"</CONTENT>\n"+"<AUTHOR>Nikhar</AUTHOR>\n"+"</DOC>"  
    file_content.write(str(final_text.encode('utf-8')))
    global FILENAME_URL_MAP
    FILENAME_URL_MAP.update({'"'+filename+'"' : '"'+page+'"'})
    file_url_list= open('crawled_urls.txt', 'a')
    file_url_list.write(str(FILE_INDEX) + "." + " "+ page + "\n")
    FILE_INDEX +=1
    file_content.close()
    file_url_list.close()


# In[437]:




# In[36]:


file_url_list.close()


# In[28]:


#ask once about handling 404 error
start=time.time()
print(start)
pages_crawled=[]
url_inlink={}
url_outlink={}
# url_inlink_count={}
# url_outlink_count={}
FILE_INDEX=1
FILENAME_URL_MAP={}
max_pages=20
bfs_depth=5

# keyword='nuclear'
url=['https://en.wikipedia.org/wiki/Fukushima_Daiichi_nuclear_disaster','https://en.wikipedia.org/wiki/List_of_nuclear_and_radiation_fatalities_by_country','http://en.wikipedia.org/wiki/Chernobyl_disaster','http://www.world-nuclear.org/info/Safety-and-Security/Safety-of-Plants/Chernobyl-Accident/']
# url=['https://www.history.com/this-day-in-history/nuclear-accident-at-three-mile-island']
spider(url,max_pages,bfs_depth)
print(time.time()-start)


# In[29]:




url_inlink1=url_inlink
url_outlink1=url_outlink


# In[121]:



with open('C:/Users/Nikhar/Downloads/Assignment/InformationRetrievalCS6200/HW3/crawled_documents2/inlinks_outlinks/inlinks1.txt', 'w') as f:
    for key, value in url_inlink1.items():
        f.write('%s:%s\n' % (key, value))
        
with open('C:/Users/Nikhar/Downloads/Assignment/InformationRetrievalCS6200/HW3/crawled_documents2/inlinks_outlinks/outlinks.txt', 'w') as f:
    for key, value in url_outlink1.items():
        f.write('%s:%s\n' % (key, value))

