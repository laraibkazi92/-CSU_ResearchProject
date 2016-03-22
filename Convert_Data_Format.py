import sys
import os
import csv
from collections import OrderedDict
from pymongo import MongoClient

writefile = open('CSULander_Modified.txt', 'w')


with open(sys.argv[1], 'r') as data:
  mycsv = csv.reader(data)
	
  # Writing the Header File to the output file
  writefile.write('Date,TimeBucket,TimeStamp,FromIP,ToIP,PortNumber,DataIn,DataOut\n')	
	
  for row in mycsv:
	day = 1
	month = 12 
	year = 2014
		
	timeBucket = 0
	time = 0
	in_pos = 2
	out_pos = 291
		
	while(in_pos < 290):
		 
	  date = str(month).zfill(2) + '-' + str(day).zfill(2) + '-' + str(year)
	  #finalString format is : <Date> , <Time Bucket> , <Time Stamp> , <From IP> , <To IP> , <Service Port Number> , <Data In> , <Data Out>
	  finalString = date + ',' + str(timeBucket).zfill(4) + ',' + str(time).zfill(4) + ',' + row[0] + ',' + row[1] + ',' + str(123) + ',' +row[in_pos] + ',' + row[out_pos] + '\n'
			
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
			
	  writefile.write(finalString)
					

writefile.close()

print('Temp .csv File Created.\n Writing data to MongoDB now...\n')

# MongoDB Variable Initialization

new_csv = open('CSULander_Modified.txt', 'r')
reader = csv.DictReader(new_csv)
client = MongoClient("localhost", 27017)
db = client.ntp

header = [ "Date","TimeBucket","TimeStamp","FromIP","ToIP","PortNumber","DataIn","DataOut"]

for row in reader:
  str1 = OrderedDict([(f, row[f]) for f in header])

  db.landerNew.insert(str1)

new_csv.close()

print('All Records added to MongoDB.\n')
print('Database: ntp \n')
print('Collection: landerNew \n')

os.remove('CSULander_Modified.txt')

print('Temp .csv file removed!\nProcess Completed!\n')
