import sys
import os
import csv
from datetime import datetime
import time
import cql


# writefile = open('CSULander_Modified.txt', 'w')

con = cql.connect('localhost', cql_version='3.0.0')
print("Connected!")


with open(sys.argv[1], 'r') as data:
  mycsv = csv.reader(data)
	
  # Writing the Header File to the output file
  # writefile.write('Date,TimeBucket,TimeStamp,FromIP,ToIP,PortNumber,DataIn,DataOut\n')	
	
  for row in mycsv:
    day = 1
    month = 12 
    year = 2014
		
    timeBucket_H = 0
    timeBucket_M = 0
    time = 0
    in_pos = 2
    out_pos = 291

    port = 123
		
    while(in_pos < 290):
        
        cursor = con.cursor()
        date = str(year) + '-' + str(month).zfill(2) + '-' + str(day).zfill(2) + ' ' + str(timeBucket_H).zfill(2) + ':' + str(timeBucket_M).zfill(2)
        dateFormat = '%m-%d-%Y %H%M'
        #finalDate = datetime.strptime(date, dateFormat)
	  
        minute = int(str(time).zfill(4))
        #minuteFormat = '%H%M'
        #finalMinute = datetime.strptime(minute, minuteFormat).time()
		
        if(row[in_pos] == 'NA'):
          dataIn = 'null'
        else:
          dataIn = int(row[in_pos])
        if(row[out_pos] == 'NA'):
          dataOut = 'null'
        else:
          dataOut = int(row[out_pos])
			  
        cql_query = "INSERT INTO ntp.lander (ts, min, from_ip, to_ip, port, in_data, out_data) VALUES ('" + date + "', " + str(minute) + ", '" +  row[0] + "', '" +  row[1] + "', " +  str(port) + ", " +  str(dataIn) + ", " +  str(dataOut) + ") " ;
	  
        cursor.execute(cql_query)
	  
        in_pos = in_pos + 1
        out_pos = out_pos + 1
        timeBucket_H = timeBucket_H + 1
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
