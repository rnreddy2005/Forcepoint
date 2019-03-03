import pandas as pd

# read both sender and receiver data in to dataframes
sender=pd.read_csv("senders.txt")
receiver=pd.read_csv("recipients.txt")
# replace NA values with blank
sender.fillna('',inplace=True)
receiver.fillna('',inplace=True)
# Gave proper column headers
sender.columns=['sender_count','user_name']
receiver.columns=['receiver_count','user_name']
#merge on full outer join to generate the desired output
result=pd.merge(sender,receiver, on='user_name',how='outer')
result = result.reindex_axis(['user_name','sender_count','receiver_count'] , axis=1)
# Write the results into CSV file
result.to_csv("Summary.csv",index=False)

