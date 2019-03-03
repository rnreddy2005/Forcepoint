import sys
import os
import pandas as pd
import json
import time
from datetime import datetime
from zipfile import ZipFile
import logging
from collections import OrderedDict

def dic_number_of_recipients(x):
    number_of_recipients = {}
    number_of_recipients["name"]= "number_of_recipients"
    number_of_recipients["value"]=len(x)
    l=[]
    l.append(number_of_recipients)
    return(l)

if __name__ == '__main__':
 
 try:
 
    # Variable Declaration 
	
	prc_dt = datetime.today().strftime('%Y%m%d')
	log_dt = datetime.today().strftime('%Y:%m:%d:%M')
	file_path = os.getcwd()
	file_name = file_path + "/" + sys.argv[1]
	log_file_name = file_name.replace("/source/","/").replace(".csv",".csv-procesed-" + prc_dt) + ".log"
	zip_file_name = file_name.replace("/source/","/archive/").replace(".csv",".csv-procesed-" + prc_dt)
	prc_path  = file_path + "/processed/"
	f_name    = file_name.split("-")[-1].split(".")[0]
	
	logging.basicConfig(filename=log_file_name,level=logging.DEBUG,filemode='w')
	
	if len(sys.argv) > 2 :
	  print ("Error : Max Arugument Allowed : 1")
	  logging.info(log_dt + ":" + "Error : Max Arugument Allowed : 1")
	  sys.exit(1)
		
	
	logging.info(log_dt + ':Program Started processing CSV File :' + file_name)
	print('Program Started processing CSV File :' + file_name) 
	
	if (not os.path.isfile(file_name)):
	  print ("Error : File Not Found")
	  logging.info(log_dt + ":" + "Error : File Not Found")
	  sys.exit(1)
	  
	# Pandas Framework Conversion 
	logging.info(log_dt + ':Pandas Framework Conversion')
	
	df1=pd.read_csv(file_name, header=None)
	df1.fillna('',inplace=True)
	df1.columns=['timestamp_iso','message identifier','sender','recipients','topic','mode']
	df1['recipients']=df1['recipients'].apply(lambda x: x.split('|'))
	df1['attributes']= df1['recipients'].apply(lambda x:dic_number_of_recipients(x))
	df1['unique_id'] = df1.index
	df1 = df1.reindex_axis(['timestamp_iso','message identifier','sender','recipients','topic','mode','attributes','unique_id'], axis=1)
	#df1['timestamp_iso']=pd.to_datetime(df1['timestamp_iso'], unit='ms')
	df1['timestamp_iso']=df1['timestamp_iso'].apply(lambda x: datetime.fromtimestamp((x/1000.0)).strftime('%Y-%m-%dT%H:%M:%S.%f')  )
	df1['timestamp_iso']=df1['timestamp_iso'].apply(lambda x: str(x))
	df1['message identifier']=df1['message identifier'].apply(lambda x: x.replace("<","").replace(">",""))
	df1.to_json("test.json",orient='records')

    # Creating JSON Files
	logging.info(log_dt + ':Creating JSON Files')
	
	config = json.loads(open('test.json').read(),object_pairs_hook=OrderedDict)
	for i in range(0,len(config)) :
	  fname=prc_path+"enron-"+f_name+"-"+str(i) 
	  with open(fname+'.json','w') as f:
	   json.dump(config[i],f)
	   
    # ZIP and Cleanup
	logging.info(log_dt + ':ZIP and Cleanup')
	
	os.remove("test.json")	
	with ZipFile(zip_file_name + ".gz",'w') as gz:
		gz.write(file_name)
	logging.info(log_dt + ':Porgram Successfully Processed CSV to JSON File')	
	print("Porgram Successfully Processed CSV to JSON File")
	print("log file : " + log_file_name)
	
 except Exception as e:
    print('Error : ' + str(e))
   
