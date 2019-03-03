
def dic_number_of_recipients(x):
    number_of_recipients = {}
    number_of_recipients["name"]= "number_of_recipients"
    number_of_recipients["value"]=len(x) 
    return(number_of_recipients)

import pandas as pd
#df1=pd.read_csv("C:\Niranjan\Personal\Python\data-scientist-exam-2019\data-engineer-exam\source\enron-event-history-20180203.csv", header=None)
df1=pd.read_csv("enron-event-history-20180203.csv", header=None)
df1.fillna('',inplace=True)
df1.columns=['timestamp_iso','message identifier','sender','recipients','topic','mode']
df1['recipients']=df1['recipients'].apply(lambda x: x.split('|'))
df1['attributes']= df1['recipients'].apply(lambda x:dic_number_of_recipients(x))
df1['unique_id'] = df1.index
df1 = df1.reindex_axis(['timestamp_iso','message identifier','unique_id','sender','recipients','topic','mode','attributes'], axis=1)
df1['timestamp_iso']=pd.to_datetime(df1['timestamp_iso'], unit='ms')
df1['timestamp_iso']=df1['timestamp_iso'].apply(lambda x: str(x))

import json
def gen_json(x):
    a = x.to_json()
    return a
	
a=[]
a=df1.apply(gen_json, axis=1)
n=len(a)
i=0
d="C:\Niranjan\Personal\Python\data-scientist-exam-2019\data-engineer-exam\processed\\"
while i<n:
    #print(a[i])
    fname=d+"enron-20180201-"+str(i)
    with open(fname+'.json','w') as f:
        f.write(  a[i] +"\n" )
    i+=1
    
    	