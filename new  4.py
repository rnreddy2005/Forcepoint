#!/usr/bin/python
import sys
import os
import subprocess
import glob
import getpass
import time
import csv
import json
import xml.etree.ElementTree as ET
from shutil import copyfile
from subprocess import call
config_path = sys.argv[1]
sys.path.insert(0,config_path)
from param_file import *
sys.path.insert(0,log_dir)
sys.path.insert(0,lib_dir)
import xlwt

table_type     = ""
primary_keys   = ""

wb = xlwt.Workbook()

style_main   = xlwt.easyxf('font: name calibri , colour black, bold True;' 'borders: left thin, right thin, top thin, bottom thin;')
pattern   = xlwt.Pattern()
pattern.pattern = xlwt.Pattern.SOLID_PATTERN
pattern.pattern_fore_colour = xlwt.Style.colour_map['pale_blue']
style_main.pattern = pattern

style_pk_key    = xlwt.easyxf('font: name calibri , colour red, bold True;' 'borders: left thin, right thin, top thin, bottom thin;')

style_tb_name   = xlwt.easyxf('font: name calibri , colour blue, underline on, bold True;' 'borders: left thin, right thin, top thin, bottom thin;')

style_border    = xlwt.easyxf('font: name calibri;' 'borders: left thin, right thin, top thin, bottom thin;')

header=["Sno","Data Item","Type Name","Length","Scale","Remarks","Country","SourceID","Table name in DOTOPAL"," Table Name in EDM","Filename","Frequency","Field Separator","Day0 Approach","Day1 Approach","Column  Header","Detail  Record","Footer/Trailer Record","Classification in EDM"]

con_type = ""

master_tables = ["TL_BANK_DATA","TL_BANK","TL_CUST_DR_AC","TL_PARAMETERS","TL_SYS_PARM"]
txn_tables   = ["TL_PYMT_DETAILS","TL_PYMT_DETAILS_EXTN","TL_PYMT_DETAILS_MERGER_ADDL","TL_PYMT_SBATCH","TL_CHQ_BATCH"]

class Tee:
    def write(self, *args, **kwargs):
        self.out1.write(*args, **kwargs)
        self.out2.write(*args, **kwargs)
    def __init__(self, out1, out2):
        self.out1 = out1
        self.out2 = out2

def get_columns(dbname, table_name,uname,pwd,driver,sys_name):
	
	con_type = ""

	if sys_name.upper() != "EDM": 
     
          cmdping = "hive -e 'desc %s.%s'"% (dbname,table_name)
          p = subprocess.Popen(cmdping, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
          (stdout, stderr) = p.communicate()
          table_desc = open(log_dir + "/ddl_output.txt","w")
          table_desc.write(stdout)
          con_type = "hive"
          
	else:

	    cmdping = "beeline --silent=true -u " + driver + " -n " + uname +  " -p " +  pwd + " --outputFormat=dsv --delimiterForDSV='\t' --showHeader=false -e 'desc  %s.%s'"% (dbname,table_name) 
	    p = subprocess.Popen(cmdping, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            (stdout, stderr) = p.communicate()
	    if len(stdout.strip('\n').split()) != 0: 
		   table_desc = open(log_dir + "/ddl_output.txt","w")
		   table_desc.write(stdout)
                   con_type = "beeline"

 
	    else:
		     print "User Access Probelm in Hive"

        
        return con_type
	table_desc.close()
	

def get_schema(src,country,table,system,uname,pwd,driver):
	global table_type
	global primary_keys
       
	dbname = system + "_ops"
        if system == "prd" :
	 hist= src + system + "_" + country + "_config_history"
        else :
         hist= src + system + "_" + country + "_config_history"
	 hist= src + "prd" + "_" + "all" + "_config_history"

	cmdping = "hive -e 777select concat_ws('|',schema,sourcetype,keycols,' '), asof from %s.%s where tablename=\'%s\' order by asof desc limit 1777"% (dbname,hist,table)
        cmdping = cmdping.replace('777','"')
        #print cmdping 
	p = subprocess.Popen(cmdping, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
	(stdout, stderr) = p.communicate()
        dict_schema = {}
        #print "stderr"
        #print stdout
        #print len(stdout)
        #print "stderr"
        #if len(stdout.strip('\n').split()) != 0: 
        if "|" in stdout :
        

	 schema = stdout.split('|')[0]
	 table_type = stdout.split('|')[1]
	 primary_keys  = stdout.split('|')[2]
         #print table_type
         #print primary_keys
	 
        #print "schema"
         #print schema
	 #if len(schema.strip('\n').split()) != 0: 
         for i in schema.split('^'):
		dict_schema[i.split()[0]] = i.split()[1]
	else:
		print "Inside Beeline ....."
                #cmdping = "beeline --silent=true -u " + driver + " -n " + uname +  " -p " +  pwd + " --outputFormat=dsv --delimiterForDSV='\t' --showHeader=false -e "select concat_ws('|',schema,sourcetype,keycols,' '), asof from %s.%s where tablename=\'%s\' order by asof desc limit 1""% (dbname,hist,table)
                cmdping = "beeline --silent=true -u " + driver + " -n " + uname +  " -p " +  pwd + " --outputFormat=dsv --delimiterForDSV='\t' --showHeader=false -e 777select concat_ws('|',schema,sourcetype,keycols,' '), asof from %s.%s where tablename=\'%s\' order by asof desc limit 1 777"% (dbname,hist,table)
                
                cmdping = cmdping.replace('777','"')
                #print cmdping 
		p = subprocess.Popen(cmdping, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
		(stdout, stderr) = p.communicate()
		schema = stdout.split('|')[0]
		table_type = stdout.split('|')[1]
		primary_keys  = stdout.split('|')[2]
	    	if len(schema.strip('\n').split()) != 0: 
			for i in schema.split('^'):
				dict_schema[i.split()[0]] = i.split()[1] 
	    	else:
			print "User Access Probelm in Hive"

        
        return dict_schema
	table_desc.close()

def get_schema_hash(src,country,table,system,uname,pwd,driver):
	pk_file_path='NULL'
	global table_type
	global primary_keys
    	dict_schema = {}
    	source = table.split('_')[0]
    	full_table_name = table
    	#print source
	json_path = pk_file_path + table + "/json/" + table.lower() + ".json"
    	table  = "_".join(table.split('_')[2:]).lower()
	json_path = pk_file_path + table + "/json/" +  full_table_name.lower() + ".json"	
	cmdping = "hadoop fs -cat " + json_path
	#cmdping = "hadoop fs -cat /sit/scudee/dotopalprd/metadata/ng/ststtben/json/dotopalprd_ng_ststtben.json"
	#print cmdping 
	p = subprocess.Popen(cmdping, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
	(stdout, stderr) = p.communicate()

	file_json = json.loads(stdout)
	j = 0
	for i in file_json["SourceSchema"]["columns"]:
		name		 = file_json["SourceSchema"]["columns"][j]["name"].strip().upper()
		data_type 	 = file_json["SourceSchema"]["columns"][j]["type"].strip().upper()
		precision        = file_json["SourceSchema"]["columns"][j]["precision"]
   		scale            = file_json["SourceSchema"]["columns"][j]["scale"]

		if data_type == "STRING" :
			#print name + "\t" + data_type
                	dict_schema[name] = data_type
                
		elif scale == 0 and data_type != "DECIMAL":
			#print name + "\t" + data_type + "(" + str(precision) + ")"
                	dict_schema[name] = data_type + "(" + str(precision) + ")"

		else:
			#print name + "\t" + data_type + "(" + str(precision) + "," +str(scale) + ")"
               		dict_schema[name] = data_type + "(" + str(precision) + "," +str(scale) + ")"
		j = j+1

	primary_keys = file_json["SourceSchema"]["PrimaryKey"][0].upper()
	table_type  = file_json["MetaInformation"]["classification"].upper()
	#print primary_key
	#print table_type

        
        return dict_schema

def get_schema_sdm(src,country,table,system,uname,pwd,driver):

 global table_type
 global primary_keys
 dict_schema = {}
 primary_key_dict = {}
 table_type_dict  = {}
 tabl_srch = 'publishedTableName="' + table.upper() + '"'

 xml_file_list = glob.glob(config_path + 'xml/'+ src.replace('DOTOPAL','OPL') + country + '*.xml')
 #print xml_file_list
 for xml_file_name in xml_file_list:
  xml_data_fp = open(xml_file_name)
  xml_data    = xml_data_fp.read()
  if tabl_srch in xml_data :
   xml_file_nm = xml_file_name
   #print xml_file_nm
   xml_data_fp.close()
   break;

 #tree = ET.parse(config_path + src + '_' + country + '.xml')
 tree = ET.parse(xml_file_nm)
 root = tree.getroot()

 for tableMapping in root.findall('./Subscription/TableMapping'):
  table_name = tableMapping.attrib['publishedTableName']
  #print table_name , table.upper()
 
  for sourcecol in tableMapping.findall('./SourceColumn'):
     
   if table_name == table.upper() : 
      
    col_name =  sourcecol.attrib['columnName']
    data_type = sourcecol.attrib['dataType']
    precision = sourcecol.attrib['precision']
    scale     = sourcecol.attrib['scale']
    key       = sourcecol.attrib['key']
    
    #if key == 'true':
      #primary_keys = col_name + ',' + primary_keys 
    

    #print "inside key files"
    with open(config_path + 'xml/' + src.lower() + "_" + country.lower() + "_keycols.list") as f:
     for line in f:
       (key, val) = line.split("->")
       primary_key_dict[key] = val.strip()
	   
    with open(config_path + 'xml/' + src.lower() + "_" + country.lower() + "_sourcetype.list") as f:
     for line in f:
       (key, val) = line.split("->")
       table_type_dict[key] = val.strip()
	   
    #print key,val
    full_table = src + "_" + country + "_" + table
    primary_keys = primary_key_dict[full_table.lower()]
    table_type   = table_type_dict[full_table.lower()]
    #print primary_keys

    if data_type == "STRING" :
      #print col_name + "\t" + data_type
      dict_schema[col_name] = data_type
                            
    elif scale == "0" and data_type != "DECIMAL":
      #print col_name + "\t" + data_type + "(" + str(precision) + ")"
      dict_schema[col_name] = data_type + "(" + str(precision) + ")"
            
    else:
      #print col_name + "\t" + data_type + "(" + str(precision) + "," +str(scale) + ")"
      dict_schema[col_name] = data_type + "(" + str(precision) + "," +str(scale) + ")"     
   
#print primary_keys[:-1]		
 #primary_keys = primary_keys[:-1]	
 # if table.upper() in master_tables:
  # table_type = "master"
 # else:
  # table_type = "txn"
 return dict_schema


def update(file,table,country,src,system,uname,pwd,driver,tb_name_short):
    f = open(log_dir + "/ddl_output.txt",'r').readlines()

    if db_type == "EDM" :
		 dict_schema = get_schema(src,country,table,system,uname,pwd,driver)
    elif db_type.upper() == "HAAS" :
		 dict_schema = get_schema_hash(src,country,table,system,uname,pwd,driver)
    else:
         #print ("calling sdm")
         dict_schema = get_schema_sdm(src,country,tb_name_short,system,uname,pwd,driver)
		
    final = open(file,'w')
    for line in f:
		if len(line.split('\t')) >= 2:
			key,value = line.split('\t')[0].strip(), line.split('\t')[1].strip()
			if key.upper() in dict_schema.keys():

				if dict_schema[key.upper()] == value.strip().upper():
					final.write(line)
				else :
					final.write(key + "\t" + dict_schema[key.upper()]+'\n')
			else :
				final.write(line)
		else :
			final.write(line)

def get_dll(uname,pwd,driver,connection_type):

	table_file         = open(config_path + "table_list.txt",'r')
	ddl_bl             = table_file.readlines()
	ddl_bl_out         = ""
	ddl_hive_out       = ""
	ddl_file_name      = ""

	for z in ddl_bl:
		ddl_bl_out   += 'desc formatted ' +  z.strip()  + ';' + "\n"
		ddl_hive_out += 'show create table ' + z.strip()  + ';' + "\n"

	ddl_bl_out   = ddl_bl_out.strip()
	ddl_hive_out = ddl_hive_out.strip()
    	ddl_bl_fp    = open(log_dir  + "ddl_bl_output.txt","w")
    	ddl_hive_fp  = open(log_dir  + "ddl_hv_output.txt","w")
    	ddl_bl_fp.write(ddl_bl_out)
        ddl_hive_fp.write(ddl_hive_out)
        ddl_bl_fp.close()
        ddl_hive_fp.close()
        ddl_bl_fp    = open(log_dir  + "ddl_bl_output.txt","ab+")
    	ddl_hive_fp  = open(log_dir  + "ddl_hv_output.txt","ab+")
        os.chmod(log_dir  + "ddl_bl_output.txt" , 0o777)
        os.chmod(log_dir  + "ddl_hv_output.txt" , 0o777)
        ddl_bl_fp.read()
        ddl_hive_fp.read()
    	if connection_type == "hive":

        	ddl_file_name = "ddl_" + src + "_" + ctry_code + ".hql"   
    		cmdping = "hive -f " +  log_dir  + "ddl_hv_output.txt" 
		#print cmdping
                ret = subprocess.call(['sh','./hiveShowCreateToDDL.sh',ddl_file_name,out_dir,cmdping])
             
                    
    	else:   
                ddl_file_name = "ddl_" + src + "_" + ctry_code + ".hql" 
                #print "ddl filename" + ddl_file_name
       		cmdping = "beeline -u " + driver + " -n " + uname +  " -p " +  pwd + " -f " + log_dir + "ddl_bl_output.txt " 
       		ret = subprocess.call(['sh','beelineDFtoDDL.sh',ddl_file_name,out_dir,cmdping]) 
              
          
	table_file.close()
        ddl_bl_fp.close()
        ddl_hive_fp.close()
	
    

sys.stdout = Tee(open(log_dir + "/isd_log.txt", "w"), sys.stdout)

### Main

print "\n" + "ISD PGM Started ...." + "\n"
print "Start Time   : " + time.strftime("%x") + " " +  time.strftime("%X") + "\n"
print pk_check

if not ctry_code.strip() or not src.strip() :
 print "Error        : Country code or Source is Empty"
 print "Country code : " + ctry_code.strip()
 print "Source       : " + src.strip()
 print "Check the param_file.py in config dir and rerun the same" + "\n"
 sys.exit(0)

#  Run Pgm
#  python isd_macro.py

table_name     = "" 
primary_key    = ""
no_of_del      = ""
tb_type_output = " ,  "
tb_type_out    = " "

for csv in glob.glob(out_dir + "*.csv"):
  os.remove(csv)

for csv in glob.glob(out_dir + "*.hql"):
  os.remove(csv)

x=0
head_file         = open(config_path + "header.txt",'r')
head_lines        = head_file.read().replace('ctry_code',ctry_code.upper())
head_file.close()


ind_file         = open(config_path + "index_file.txt",'r')
ind_lines        = ind_file.read()
ind_file.close()

index_fo         = open(out_dir + "MAIN.csv","a+")
index_fo.write(ind_lines.replace('source_system',src.upper()))
index_fo.close()


y = open(config_path + "table_list.txt",'r')
l = y.read()
count=l.splitlines()
count=len(count)
y.close()

#username =       raw_input("Enter Username for Hive DB : ")
#password = getpass.getpass("Enter Password for Hive DB : ") 
username = "sitamlapp"
password = "temp1234"
password = "Sailteam@1"

print "\n................................................ISD Preparation Started !!! ................................................\n"
print "Username               : " + username.upper() 
print "Source                 : " + src.upper() 
print "Country                : " + ctry_code.upper()
print "No of Tables           : " + str(count)
print "Primay Key File Path   : " + pk_file_path
print "ISD CSV Files Path     : " + out_dir
print "\n................................................ISD Preparation Started !!! ................................................"
print "\n"

#print "Sleep for 10 Seconds......"

	
y = open(config_path + "table_list.txt",'r')
for v in y:

        db_tb_name           = v.strip().split('.')[0]
        tb_name              = v.strip().split('.')[1] 
        var1                 = tb_name.split('_')
        var3                 = '_'.join(var1[2:])
        tb_name_short        = var3
        output_table_name    = ctry_code.upper() + '_' + src.upper() + '_' + var3.upper()
        output_file_name     = output_table_name  + ' File Layout'
        output_ind_file_name = output_table_name + '_YYYYMMDD_D_1.dat'
 
        connection_type = get_columns(str(db_tb_name),str(tb_name),str(username),str(password),str(db_driver),str(db_type))
        if pk_check == "YES" :
        	update(log_dir + 'ddl_output1.txt',tb_name,ctry_code,src,system,str(username),str(password),str(db_driver),tb_name_short)   

	#print "main fundtion "
        print table_type   
     
        
        if table_type.lower() in ["master","delta"] :
           tb_type_output = "Full Dump , Incremental"
           tb_type_out    = "Master"
        elif table_type.lower() in ["txn","transaction"] :
           tb_type_output = "Incremental , Incremental"
           tb_type_out    = "Transaction"

           
        wrk_file = var3.upper() + ".csv" 
        print "\n" + "Preparing File             : " + wrk_file
        wrk_file = out_dir + wrk_file
        x = x+1
        
        index_fo = open(out_dir + "MAIN.csv","a+")
        index_fo.write(str(x) + ',' + ctry_code.upper() + ',' + src.upper() + ',' + var3.upper() + ',' + tb_name  + ',' + output_ind_file_name + ',' + "Daily" + ',' +  field_separator + ',' + tb_type_out + ',' +  tb_type_output  + "\n")
        index_fo.close()
         
        
        fo = open(wrk_file, "w+")
        fo.write(output_file_name + "\n" + head_lines) 
        fo.close()
         
        print "Table Name                 : " + tb_name.upper() + "\n"
        print "....................Connecting Hive  !!! .............\n"



        
  	file_hive = open(log_dir + 'ddl_output.txt','r')
	count=0
	part_columns=[]
        for part_line in file_hive:
         pl  = part_line.split('\t')[0].strip()
         if 'Partition Information' in part_line:
	    count=1
	 if count==1 and len(pl) > 1 :
            part_columns.append(pl)	  
        file_hive.close()
	if pk_check == "YES" :
			file_hive = open(log_dir + 'ddl_output1.txt','r')
	else :
            file_hive = open(log_dir + 'ddl_output.txt','r')       
        fo = open(wrk_file, "a+")
        file_colnames = open(log_dir + 'file_col_part','w+')
        n=2
	for l in file_hive:
	  fields = l.split('\t')
          col_name = fields[0].strip()
	  if len(col_name.strip()) > 0 :
	   fo.write(str(n) + ',' + col_name.upper() + ',' + 'Hard coded' + ',' + ',' + ',' + col_name.upper() + '\n' )
           data_ty =  fields[1].strip().split('(')
           data_type = data_ty[0].upper()
           size=''
           if len(data_ty) > 1 :
           	size = data_ty[1].replace(')','')
                if "," in size:
                 no_of_del = ","
                else:
                  no_of_del = ",,"
           else:
                  no_of_del = ",,"

           if col_name.upper() in primary_keys :
               primary_key = "PRIMARY KEY"
           elif col_name in pl :
               primary_key = "PARTITION COLUMN" 
           else:
              primary_key  = "  "
                 
           file_colnames.write(str(n) + ',' + col_name.upper() + ',' + data_type + ',' + size + no_of_del + primary_key + '\n')
	   primary_key = ""
           n=n+1
	  else:
	   break 
       
        fo.close()
       
        file_colnames.close()
        fo = open(wrk_file, "a+")
        fo.write('Detail  Record'  + "\n" + '1,Record Identifier,	Hard Coded,' + ',' + ',' + 'D' + '\n')
        file_colnames = open(log_dir + 'file_col_part','r')
        coln_nm = file_colnames.read()
        file_colnames.close()   
        tail_file = open(config_path + "trailer.txt",'r')
        tail_lines = tail_file.read()
        tail_file.close()
        fo.write(coln_nm)
        fo.close()
        fo = open(wrk_file, "a+")
        fo.write(tail_lines)
        fo.close()

index_fo = open(out_dir + "MAIN.csv","a+")
index_fo.write("Note : It is recommended that Partition column be included as part of the composite primary key for the transaction tables" + "\n")
index_fo.close()

print "Excel Merging Started :.."

import glob, csv, xlwt, os

filelist = glob.glob(out_dir + '*.csv')
filelist.sort(key=os.path.getmtime)
filelist.remove(out_dir + 'MAIN.csv')
filelist.insert(0,out_dir + 'MAIN.csv')

for filename in filelist:
    (f_path, f_name) = os.path.split(filename)
    (f_short_name, f_extension) = os.path.splitext(f_name)
    ws = wb.add_sheet(f_short_name)
    spamReader = csv.reader(open(filename, 'rb'))
    for rowx, row in enumerate(spamReader):

        for colx, value in enumerate(row):

	    if "PRIMARY KEY"  in value or "PARTITION COLUMN" in value or "recommended that" in value:
            	ws.write(rowx, colx, value, style=style_pk_key)

	    elif (rowx in [0,1] and value in header) or value in ["Header Record","Column  Header","Detail  Record","Footer/Trailer Record"] or "File Layout" in value or "Table name in" in value: 
		ws.write(rowx, colx, value, style=style_main)
	    else :
        
		if rowx > 0 and colx == 3 and f_short_name=='MAIN' :

		    link = 'HYPERLINK("#%s!A1", "%s")' % (value,value)
		    ws.write(rowx, colx, xlwt.Formula(link),style=style_tb_name)

		else :

		    ws.write(rowx, colx, value,style=style_border)

wb.save(out_dir + ctry_code.upper() + "_" +  src.upper() + "_" + "Interface_File_Spec.xls" )
print  "End Time : " + time.strftime("%x") + " " + time.strftime("%X") + "\n"
#get_dll(str(username),str(password),str(db_driver),connection_type)


for csv in glob.glob(out_dir + "*.csv"):
  os.remove(csv)

for hql in glob.glob("./*.hql"):
  os.remove(hql)


print "SUCCESS .. Please check the output dir for xls and ddl files"
