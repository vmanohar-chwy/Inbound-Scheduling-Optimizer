# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 10:45:47 2019

@author: AA-VManohar
"""

from subprocess import call
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import logging
import logging.handlers
logger = logging.getLogger('Email_run')
logger.setLevel(logging.DEBUG)
rh = logging.handlers.RotatingFileHandler('Email.log',maxBytes = 100*1024,backupCount = 1)
rh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
rh.setFormatter(formatter)
ch.setFormatter(formatter)
if (logger.hasHandlers()):
    logger.handlers.clear()
logger.addHandler(rh)
logger.addHandler(ch)
try:
    start = time.time()
    call(["python", "ISO_EFC.py"])
    time.sleep(10)
    call(["python", "ISO_WFC.py"])
    time.sleep(10)
    call(["python", "ISO_PHX.py"])
    time.sleep(10)
    call(["python", "ISO_DFW.py"])
    time.sleep(10)
    call(["python", "csv_template.py"])
    end = time.time()
    execution = end-start
    print(execution)
    fromaddr = 'scsystems@chewy.com'
    toaddr = 'vmanohar@chewy.com,DL-Supply_Chain_Central_Scheduling@chewy.com,sromero1@chewy.com,igonzalez1@chewy.com,EAlfonso@chewy.com,jxie@chewy.com'
    to = ', '.join(toaddr)
    file_list = ['SCHEDULE_EFC3','SCHEDULE_PHX1','SCHEDULE_DFW1','SCHEDULE_WFC2']
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = "Algorithm Output for "+ str(date) 
    body = "Hello team, \n\nKindly find the algorithm output file for today attached with this mail for your kind notice.\n\nThanks"
    msg.attach(MIMEText(body, 'plain'))
    for j in file_list:
        file_path = "E:\\VManohar\\ISO\\FC\\Mail Script\\" + j +'_.csv'
        attachment = open(file_path, "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload((attachment).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= %s" % (j +'_.csv'))
        msg.attach(part)
        attachment.close()
    server = smtplib.SMTP('smtp.chewymail.com', 25)
    text = msg.as_string()
    server.sendmail(fromaddr,toaddr.split(','), text)
    logger.info("Email was sent to the recipients: %s" %(toaddr))
    server.quit()
    print("Email was sent to the recipients: %s" %(toaddr))
    fromaddr = 'scsystems@chewy.com'
    toaddr = 'vmanohar@chewy.com,DL-Supply_Chain_Central_Scheduling@chewy.com,sromero1@chewy.com,igonzalez1@chewy.com,EAlfonso@chewy.com,jxie@chewy.com'
    to = ', '.join(toaddr)
    file_list = ['Schedule_AVP1','Schedule_CFC1','Schedule_DAY1','Schedule_MCO1']
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = "CSV template for all FC" 
    body = "Hello team, \n\nPlease find CSV template files attached for your reference.\n\nThanks"
    msg.attach(MIMEText(body, 'plain'))
    for j in file_list:
        file_path = "E:\\VManohar\\ISO\\FC\\Mail Script\\" + j +'_.csv'
        attachment = open(file_path, "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload((attachment).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= %s" % (j +'_.csv'))
        msg.attach(part)
        attachment.close()
    server = smtplib.SMTP('smtp.chewymail.com', 25)
    text = msg.as_string()
    server.sendmail(fromaddr,toaddr.split(','), text)
    logger.info("Email was sent to the recipients: %s" %(toaddr))
    server.quit()
    print("Email was sent to the recipients: %s" %(toaddr))
except Exception as e:
    print("Error Reported")
    logger.error("Error in the code: "+str(e))
rh.close()
