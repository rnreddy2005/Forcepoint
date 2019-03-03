#!/bin/bash

#Write your script below
#####################################################################################
# Script Behaviour: Bash script analytic task:
# Input Parameter :source/<filename>.csv
# Example: sh sh get_metrics.sh source/enron-event-history-20180202.csv
#
# Return Code: 0 executed successfully.
#              1 execution failed and error message sent to Standard Output.
# Change History: Initial Version
#
# Created By: Niranjan Reddy Rachamala	 
# Date: 	02-Mar-2019
# Change Log: 	Initial Version
#####################################################################################


log_file_path="./get_metrics_`date -u +'%Y_%m_%d_%H'`.log"

echo "creating the log file.." 
rm $log_file_path
touch $log_file_path

echo "Checking number of parameters.." | tee -a $log_file_path

if [ $# -lt 1 ] || [ $# -gt 1 ]
then
    echo "Expecting 1 parameters, Received "$# | tee -a $log_file_path
    echo "[`date +%c`] ERROR: Unexpected number of parameters! Expected = 1, Received = $#" | tee -a $log_file_path
    exit 1
fi

echo "Received the expected parameters and the Executon started...." | tee -a $log_file_path

  if [ ! -f $1 ]
        then
        echo "the given files does not exists... Please check... : Exiting(127) "  | tee -a $log_file_path
        exit 127
  fi

#Receiving parameters are assigned to variable

FName=$1

echo " total number of records are as  follows" | tee -a $log_file_path
wc -l ./$FName | tee -a $log_file_path

echo "-------------------------------------------------------------------------------------"| tee -a $log_file_path
echo " "| tee -a $log_file_path
echo "start: creating the senders.txt to count the number of messages send by each sender at `date  -u +'%Y-%m-%d %H:%M:%S'`"  | tee -a $log_file_path

rm senders.txt
touch senders.txt

echo "Count,Sender" > senders.txt
awk -F ',' '{print $3}' ./$FName | sort | uniq -c | sed 's/^[ \t]*//' | awk '{sub(/.$/,"")}1' |sed 's/"/,/g' >> senders.txt
sed -i 's/',,'//g' senders.txt

echo "end: senders.txt to count the number of messages send by each sender is generated at `date  -u +'%Y-%m-%d %H:%M:%S'`"  | tee -a $log_file_path

echo "-------------------------------------------------------------------------------------"| tee -a $log_file_path
echo " "| tee -a $log_file_path

echo "start: creating the recipients.txt to count the number of messages received by each at `date  -u +'%Y-%m-%d %H:%M:%S'`"  | tee -a $log_file_path
rm recipients.txt
touch recipients.txt

echo "Count, Recipient" > recipients.txt
awk '{split($0,a,","); print a[4]}' ./$FName | cut -d'|' --output-delimiter=$'\n ,'  -f1-  | sed 's/^[ \t]*//' | sed 's/^"/,/'|sed 's/"//g'|sort | uniq -c >> recipients.txt
sed -i 's/^ *//' recipients.txt

echo "end: recipients.txt to count the number of of messages received by each receipent at `date  -u +'%Y-%m-%d %H:%M:%S'`"  | tee -a $log_file_path

echo " "| tee -a $log_file_path
echo "---------------------------------End of the Program--------------------------------------------"| tee -a $log_file_path

