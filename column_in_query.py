import os
import time
import xml.etree.ElementTree as et

import pandas as pd

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

if not os.path.exists(directory):
    os.mkdir(directory)

file_path_1="C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-03-13//initial_query.txt"

output_file="C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-02-05//output_final.txt"

f = open(file_path_1, "r")

for file_line in f:
    print(file_line)