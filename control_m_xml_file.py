import os
import time
import xml.etree.ElementTree as ET

import pandas as pd

datestr = time.strftime("%Y-%m-%d")
timestr = time.strftime("%Y-%m-%d-%H%M")
directory = 'reports ' + datestr

if not os.path.exists(directory):
    os.mkdir(directory)


files=open("C://Users//Gaurav.Makde//PycharmProjects//pythonProject//"+directory+"//controlm_input.xml", 'r')

with open("C://Users//Gaurav.Makde//PycharmProjects//pythonProject//"+directory+"//output.xml", 'w') as output_file:
    for file in files:
        output_file.write(file.replace("TDPROD","EDWBQ"))



def replace_field(get_job, output_file):
    for key in get_job.keys():
        print(key)
        if key=='RUN_AS':
            get_job.attrib[key]='cgwbqrefuser'
        elif key=='NODEID':
            get_job.attrib[key] = 'HG_CGW_BQ'
        elif key == 'CMDLINE':
            if 'INIT' in get_job.attrib[key]:
                print("It is INIT script")

            elif 'END' in get_job.attrib[key]:
                print("It is END script")
            else:
                print("It is Normal script")
                get_job.attrib[key] = '####Normal#####'

    # Write the modified XML to a new file
    tree.write(output_file)

# Example usage

def variable_redefined (input_file,output_file):
    tree = ET.parse(input_file)
    root = tree.getroot()
    parents = root.findall('SMART_FOLDER/VARIABLE')
    print(parents)

    new_variable = {'email': 'gaurav.makde@cisco.com'}
#
    # for parent in parents:
    for variable_name, variable_value in new_variable.items():
        new_child = ET.Element('VARIABLE')
        new_child.set("NAME", variable_name)
        new_child.set("VALUE", variable_value)
        print(new_child)
        parents.append(new_child)
    # for i in parents:
    #     print(i.attrib)
            # print(parent)

#     new_child = ET.Element('VARIABLE')
#     new_child.set("NAME", 'email')
#     new_child.set("VALUE", 'gaurav.makde@cisco.com')
#     print(new_child)
#     parents.append(new_child)
#     print(parents)
#     for i in parents:
#         print(i.attrib)
# #
    tree.write(final_file)
    tree = ET.parse(final_file)
    root = tree.getroot()
    parents = root.findall('SMART_FOLDER/VARIABLE')
    for i in parents:
        print(i.attrib)

xml_file = "C://Users//Gaurav.Makde//PycharmProjects//pythonProject//"+directory+"///output.xml"
output_file = "C://Users//Gaurav.Makde//PycharmProjects//pythonProject//"+directory+"//final.xml"
final_file="C://Users//Gaurav.Makde//PycharmProjects//pythonProject//"+directory+"//updated_All.xml"


# tree = ET.parse(xml_file)
# root = tree.getroot()
# for get_job in root.findall('SMART_FOLDER/SUB_FOLDER/JOB'):
#     replace_field(get_job, output_file)
# tree_2 = ET.parse(output_file)
# root_2 = tree.getroot()
# for get_job in root_2.findall('SMART_FOLDER/SUB_FOLDER'):
#     replace_field(get_job, output_file)
# print(root.findall('SMART_FOLDER/VARIABLE'))


variable_redefined(output_file,output_file)
#
#
#     sub_variable_tags.clear()
#     new_child = ET.Element('NAME')
#     sub_variable_tags.append(new_child)
# new_child = ET.Element('NAME')
# variable_tags.append('GAURAV')

# new_child = ET.Element('GMMMMMMMM')
# new_child.append('gaurav')
# for variable_tag in variable_tags:

    # print(variable_tag)
    # for Variable_key in variable_tag.it():
        # NAME = Variable_key.append('NAME')
        # NAME = Variable_key.get('NAME')
        # print(Variable_key.text)
        # print(NAME)

        # print(variable_tag)

        # print(Variable_key)
        # if Variable_key=='NAME':
        #     variable_tag.attrib[Variable_key]='email'
        # if Variable_key=='VALUE':
        #     variable_tag.attrib[Variable_key]='gaurav.makde@datametica.com'
    # variable_tag.text = 'gaurav'

# tree.write(output_file)
