import sys
import os
import pandas as pd
import json
import time
from datetime import datetime
from zipfile import ZipFile

def dic_number_of_recipients(x):
    number_of_recipients = {}
    number_of_recipients["name"]= "number_of_recipients"
    number_of_recipients["value"]=len(x)
    l=[]
    l.append(number_of_recipients)
    return(l)

if __name__ == '__main__':

	prc_dt = datetime.today().strftime('%Y%m%d')
	file_path = os.getcwd()
	file_name = file_path + sys.argv[1]
	prc_path  = file_path + "/processed/"
	f_name    = file_name.split("-")[-1].split(".")[0]
	df1=pd.read_csv(file_name, header=None)
	df1.fillna('',inplace=True)
	df1.columns=['timestamp_iso','message identifier','sender','recipients','topic','mode']
	df1['recipients']=df1['recipients'].apply(lambda x: x.split('|'))
	df1['attributes']= df1['recipients'].apply(lambda x:dic_number_of_recipients(x))
	df1['unique_id'] = df1.index
	df1 = df1.reindex_axis(['timestamp_iso','message identifier','unique_id','sender','recipients','topic','mode','attributes'], axis=1)
	df1['timestamp_iso']=pd.to_datetime(df1['timestamp_iso'], unit='ms')
	df1['timestamp_iso']=df1['timestamp_iso'].apply(lambda x: str(x))
	df1.to_json("test.json",orient='records')


	config = json.loads(open('test.json').read())
	for i in range(0,len(config)) :
	  fname=prc_path+"enron-"+f_name+"-"+str(i) 
	  with open(fname+'.json','w') as f:
	   json.dump(config[i],f)

	os.remove("test.json")	
	zip_file_name = file_name.replace("/source/","/archive/").replace(".csv",".csv-procesed-" + prc_dt) 
	with ZipFile(zip_file_name + ".gz",'w') as gz:
		gz.write(file_name)
	
   
