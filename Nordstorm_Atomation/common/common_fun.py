import glob
import time
# import chardet
import os
import re
from common.config_parser import *

flagID = re.I|re.DOTALL
flagI = re.I
flagD = re.DOTALL


class Common:

    def create_directory(self,directory):
        print("inside the main class")
        if not os.path.exists(directory):
            os.mkdir(directory)
            print(f"Directory {directory} created.")
        else:
            print(f"Directory {directory} already exists.")

    # def FileList(self, path):
    #     return glob.glob(path + '**/*', recursive = True)
    #
    # def Read_file(self, file):
    #     with open(file, "rb") as f:
    #         result = chardet.detect(f.read())
    #         e = result['encoding']
    #     f.close()
    #     with open(file, "r", encoding = e) as f:
    #         file_obj = f.read()
    #     f.close()
    #     return file_obj
    #
    # def Readlines_file(self, file):
    #     with open(file, "rb") as f:
    #         result = chardet.detect(f.read())
    #         e = result['encoding']
    #     with open(file, "r", encoding = e) as f:
    #         file_obj = f.readlines()
    #         f.close()
    #     return file_obj
    #
    # def Write_file(self, file, file_obj):
    #     with open(file, "w") as w:
    #         w.write(file_obj)
    #         w.close()
    #
    # def Remove_comments(self, file):
    #     fileLines = common_obj.Readlines_file(file)
    #     comment = 0
    #     counter = 0
    #     obj = ''
    #     final_obj = ''
    #     for fileLine in fileLines:
    #         if "/*" in fileLine:
    #             if '*/' in fileLine:
    #                 comment = 0
    #                 pass
    #             else:
    #                 comment = 1
    #                 pass
    #         elif comment == 1:
    #             if "*/" in fileLine:
    #                 comment = 0
    #                 pass
    #             else:
    #                 pass
    #         elif fileLine.startswith("--"):
    #             pass
    #         elif counter == 0:
    #             obj = ''
    #             obj = obj + fileLine
    #             final_obj = final_obj + obj
    #         # else:
    #         #     final_obj=final_obj+fileLine
    #     return final_obj

common_obj = Common()

# def read_file_with_comments(filelist):
#     final_obj=''
#     for filename in filelist:
#         # print("filename",filename)
#         with open(filename,'r') as f:
#             fileobj = f.readlines()
#             # print("fileobj", fileobj)
#             for fileLine in fileobj:
#                 fileLine=" "+fileLine
#                 # print("fileLine", fileLine)
#                 final_obj+=fileLine
#
#     return final_obj
#
# def read_file(filelist):
#     ## Object With removed comment
#     final_obj=''
#     for filename in filelist:
#         # print('filename',filename)
#         with open(filename,'r') as f:
#             fileobj=f.readlines()
#             comment=0
#             for fileLine in fileobj:
#                 fileLine=" "+fileLine
#                 cmt=re.findall(r'^[\s]*--',fileLine)
#                 fileLine = re.sub(r'\/\*.*\*\/', '', fileLine)
#                 if comment == 0 and fileLine.strip().startswith("/*"):
#                     if fileLine.strip().endswith("*/"):
#                         comment=0
#                     else:
#                         comment=1
#                 elif comment==1:
#                     if fileLine.strip().endswith("*/"):
#                         comment=0
#                     else:
#                         pass
#                 elif cmt:
#                     pass
#                 else:
#                     fileLine=re.sub(r'\/\*.*\*\/','',fileLine)
#                     fileLine=re.sub(r'\-\-.*','',fileLine)
#                     final_obj+=fileLine
#     return final_obj
#
#
# def get_single_line_comment_sql(file_obj):
#     sql_list = []
#     line_obj = ''
#
#     for line in file_obj.split('\n'):
#         line = re.sub(r"^[\s]*\-\-", "--", line)
#         line = re.sub(r"^[\s]*", "", line)
#
#         if line.startswith('--'):
#             line_obj=line_obj+line
#
#     if line_obj:
#         line_obj = re.sub(r"\-\-", " ", line_obj, flags=flagID)
#         sql_list.append(line_obj)
#
#     return sql_list
#
# def replace_comment(fileobj):
#     MultilineComment=re.findall(r'\/\*.*?\*\/',fileobj,flags=re.I|re.DOTALL)
#     multiline_counter=0
#     for comment in MultilineComment:
#         multiline_counter+=1
#         fileobj=fileobj.replace(comment,'AutomationCiscoAssuranceMultiLineComment'+str(multiline_counter),1)
#
#     SinglelineComment=re.findall(r'\-\-.*',fileobj)
#     singleline_counter=0
#     for comment in SinglelineComment:
#         singleline_counter+=1
#         fileobj=fileobj.replace(comment,'AutomationCiscoAssuranceSingleLineComment'+str(singleline_counter),1)
#
#     return fileobj,MultilineComment,SinglelineComment
#
# def remove_all_comments(str_obj):
#     str_obj = re.sub(r"\-\-.*?\n", "", str_obj, flags=flagID)
#     str_obj = re.sub(r"\/\*.*?\*\/", "", str_obj, flags=flagID)
#
#     return str_obj
#
# def remove_multi_line_commments(file_obj):
#     pattern = r'/\*\*.*?\*/'
#     result = re.sub(pattern, '', file_obj, flags=re.DOTALL)
#     return result