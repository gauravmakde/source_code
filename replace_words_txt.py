import os
import time
import xml.etree.ElementTree as et

import pandas as pd

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

if not os.path.exists(directory):
    os.mkdir(directory)


files=open("C://Users//gmakde//PycharmProjects//pythonProject//"+directory+"//Trial_1.txt", 'r')

replace_word={"EDW_Prod_104":'Gaurav','N_FINANCIAL_ACCOUNT_TV':'Saurav','$$':'@@'}


with open("C://Users//gmakde//PycharmProjects//pythonProject//"+directory+"//output_trail_1.py", 'w') as output_file:
    for file in files:

        for key, value in replace_word.items():
            print(key, value)
            file=file.replace(key,value)

        output_file.write(file.replace(key, value))

