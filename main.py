import os
import time
import xml.etree.ElementTree as et

import pandas as pd

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

if not os.path.exists(directory):
    os.mkdir(directory)

df = pd.read_csv("reports 2024-03-05\wf_user_details.csv")

def highlight_salary(s):
    '''
    Highlight cells in Salary column with values above 50000
     '''
    is_above_threshold = s == 'Ganesh Muppala'
    return ['background-color: yellow' if v else '' for v in is_above_threshold]
    # is_above_threshold = s == 'Yashodeep Rahul Daiv'
    # return ['background-color: orange' if v else '' for v in is_above_threshold]
    # is_above_threshold = s == 'Ajay Sunil Hoke'
    # return ['background-color: red' if v else '' for v in is_above_threshold]

# df.style.apply(highlight_salary, subset=['Assigned'])
df.style.apply(highlight_salary, subset=['Assigned']).to_excel('reports 2024-03-05\output.xlsx', index=False)