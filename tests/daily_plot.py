#!/usr/bin/env python3
import os
import re
from pathlib import Path
import glob
import matplotlib.pyplot as plt
import numpy as np
import smtplib
import imghdr
from email.message import EmailMessage
from datetime import date
import requests

URL = "http://ctl040pc.fnal.gov:8080/lcap/servlet"

app_path = '.'
#input_dir = os.path.realpath('C:\myPython\linec\linac-logger-device-cleaner\output')
input_dir = os.path.realpath('\\pnfs\\ldrd\\accelai\\l-cape')

#input_dir = os.path.realpath('C:\\gov\\fnal\\controls\\webapps\\lcap')
output_dir = os.path.join(app_path,'output')

today = date.today()
month = str(today.month) if today.month>9 else '0'+ str(today.month)
day = str(today.day) if today.day>9 else '0'+str(today.day)
today_path = '\\'+str(today.year)+month+'\\'+day 
#today_path = '\\'+str(today.year)+str(today.month)+'\\'+str(today.day) if today.month>9 else '\\'+str(today.year)+'0'+str(today.month)+'\\'+str(today.day) 
input_dir=input_dir+today_path

print('input=',input_dir)

size = []
x = []
filename = []
data = {}
email_from = 'zyuan@fnal.gov'
email_to = 'zongweiyuan@gmail.com, zyuan@fnal.gov'




def main():
#	obj = os.scandir(input_dir)
	count =1
	# need to change file extension to h5
	for entry in glob.glob(os.path.join(input_dir,'*.*')):     
		#print(entry, os.path.getsize( os.path.join(input_dir,entry)))
		print(os.path.basename( os.path.join(input_dir,entry)), os.path.getsize( os.path.join(input_dir,entry)))
		size.append(os.path.getsize( os.path.join(input_dir,entry))/1024)
		x.append(count)
		filename.append(os.path.basename(os.path.join(input_dir,entry)))
		count += 1
		
	
	fig, ax = plt.subplots()
	ax.plot(x, size)
	#ax.plot(filename, size)
	ax.set(xlabel='files',ylabel='size (k)',title='Input Data Scan')
	fig.suptitle('File Size')
	plt.savefig('FileSize_'+str(today.year)+month+day+'.png')
	#plt.show()
	
	#send parameters
	data = dict(zip(filename,size))
	r = requests.post(url = URL, data = data)
	print("r = ", r)	  
	
	
	
def sendEmail():
	msg = EmailMessage()
	msg['Subject'] = 'L-CAPE Data Files Brief'
	msg['From'] = email_from
	msg['To'] = email_to
	s = smtplib.SMTP('smtp-ux.fnal.gov:25')
	msg.set_content('sss')
	with open('filesize.png','rb') as fp:
		img_data = fp.read()
	msg.add_attachment(img_data, maintype='image',subtype=imghdr.what(None, img_data)) 
	s.send_message(msg)
	s.quit()
	print('done')

if __name__ == "__main__":
	main()
#	sendEmail()

