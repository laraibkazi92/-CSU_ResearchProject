import sys
import os
import csv
from datetime import datetime
import time
from collections import OrderedDict
#from pymongo import MongoClient

# writefile = open('CSULander_Modified.txt', 'w')

#client = MongoClient("localhost", 27017)
#db = client.ntp

with open(sys.argv[1], 'r') as data:
  mycsv = csv.reader(data)
	
  # Writing the Header File to the output file
  # writefile.write('Date,TimeBucket,TimeStamp,FromIP,ToIP,PortNumber,DataIn,DataOut\n')	
	
  for row in mycsv:
    day = 1
    month = 12 
    year = 2014
		
    timeBucket = 0
    time = 0
    in_pos = 2
    out_pos = 291
		
    while(in_pos < 290):
        date = str(month).zfill(2) + '-' + str(day).zfill(2) + '-' + str(year) + ' ' + str(timeBucket).zfill(4)
        dateFormat = '%m-%d-%Y %H%M'
        finalDate = datetime.strptime(date, dateFormat)
	  
        minute = str(time).zfill(4)
        minuteFormat = '%H%M'
        finalMinute = datetime.strptime(minute, minuteFormat).time()
		
        if(row[in_pos] == 'NA'):
          dataIn = 'null'
        else:
          dataIn = int(row[in_pos])
        if(row[out_pos] == 'NA'):
          dataOut = 'null'
        else:
          dataOut = int(row[out_pos])
			  
        #result = db.landerNew.insert_one(
        result = (
		{
		   "ts": finalDate,
		   "min": finalMinute,
		   "from": row[0],
		   "to": row[1],
		   "port": 123,
		   "in": dataIn,
		   "out": dataOut
		
		}
        )
	  
        print(result)
	  
        in_pos = in_pos + 1
        out_pos = out_pos + 1
        timeBucket = timeBucket + 100
        time = time + 100
			
        if(time == 2400):
          day = day + 1
          timeBucket = 0
          time = 0
				
		# Further code to be written for addressing :
		# 1. End of Month
		# 2. End of Year
			
#	  writefile.write(finalString)
# writefile.close()

# End of Script -----------------------------------

print('Process Completed!\n')
