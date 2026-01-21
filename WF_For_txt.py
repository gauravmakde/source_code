import os
import time
import xml.etree.ElementTree as et

import pandas as pd

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

if not os.path.exists(directory):
    os.mkdir(directory)

file_path_1="C://Users//gmakde//PycharmProjects//pythonProject//reports 2024-04-23//SQL_TEXT001.txt"
# C:\Users\gmakde\PycharmProjects\pythonProject\reports 2024-04-23
# file_path_2="C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-02-05//output_2.txt"

print(file_path_1)

# output_file="C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-02-05//output_final.txt"

f = open(file_path_1, "r")

for file_line in f:
    # print(file_line)
    
    if '$$' in file_line:
        print(file_line)
exit()

# f1 = open(file_path_2, "r")
count=0
# for file_line in f:
#     count+=1
# print(count)
# count1=0
# for file_line in f1:
#     count1+=1
# print(count1)

file_paths = [file_path_1,file_path_2]

# with open(output_file, 'w') as output_file:
#     for file_path in file_paths:
#         with open(file_path, 'r') as input_file:
#             for line in input_file:
#                 output_file.write(line)
# f2 = open('C://Users//Gaurav.Makde//PycharmProjects//pythonProject//reports 2024-02-05//output_final.txt', "r")
count2=0
# for file_line in f2:
#     count2+=1
# print(count2)

for file_line in f2:
    # print(file_line)
    if ("REPLACE VIEW" in file_line ):

        if ("WHERE" not in file_line or file_line.count("From")>1):
            try:
                print(file_line)
                view=file_line.split("VIEW")[1].split("AS")[0].replace("\\r","").strip()

                table_name=file_line.lower().split("from")[1].replace("\\r","").replace(";","").replace("'","").replace("]","").strip()

                print("View name:", view )
                print("Table name:", table_name)

            except Exception as e:
                # print(file_line)
                print(e)

    if ("CREATE VIEW" in file_line ):
        print(file_line.split("VIEW")[1])
        # view = file_line.split("VIEW")[1].split("AS")[0].replace("\\r", "").strip()
        # print(view)
