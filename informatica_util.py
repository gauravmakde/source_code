
from functions.common.config_parser import *
from functions.common.common_fun import *
import glob
import re

class Util:
    
        
    def fun_Replace_param_in_XML(self, XmlFolderLst, Sql_Folder_Path):
        try:
            ParamFolderLst = glob.glob(Sql_Folder_Path+'**/*.parm', recursive=True)
            # print("XmlFolderLst", XmlFolderLst)

            for p_file in ParamFolderLst:
                
                for x_file in XmlFolderLst:
                    
                    p_file_r = re.sub(r"_PDO.parm", '.xml', p_file, flags=flagI)
                    p_file_r = os.path.basename(p_file_r).lower()
                    x_file_r = 'wf'+os.path.basename(x_file).split('_wf')[1].lower()
                    if p_file_r == x_file_r:

                        p_file_obj = common_obj.Read_file(p_file)
                        # print(p_file_obj)
                        p_lst = p_file_obj.split('\n')
                        # print(len(p_lst))

                        x_file_obj = common_obj.Read_file(x_file)
                        for ele in p_file_obj.split('\n'):
                            if '=' in ele:
                                # print(ele.split('=')[1])
                                dollar = ele.split('=')[0]
                                param = ele.split('=')[1]

                                x_file_obj = x_file_obj.replace(dollar, param)

                        common_obj.Write_file(x_file, x_file_obj)

        except Exception as e:
            print("Error in processing: class-Util Fun-fun_Replace_param_in_XML", e)

    def fun_Upper_case_object(self, Sql_Folder_Path):
        try:
            
            sqlfolderst = glob.glob(Sql_Folder_Path+'**/*.[sS][qQ][lL]', recursive=True)

            for p_file in sqlfolderst:
                file_obj = common_obj.Read_file(p_file)
                # print("file_obj", file_obj)

                full_object = re.findall(r'UPDATE[\s]+[\w]*\.*[\w]+\.[\w]+|INTO[\s]+[\w]*\.*[\w]+\.[\w]+|FROM[\s]+[\w]*\.*[\w]+\.[\w]+|JOIN[\s]+[\w]*\.*[\w]+\.[\w]+|TABLE[\s]+[\w]*\.*[\w]+\.[\w]+|EXISTS[\s]+[\w]*\.*[\w]+\.[\w]+|UPDATE[\s]+\{[\w]+\}[\w]+\.[\w]+|INTO[\s]+\{[\w]+\}[\w]+\.[\w]+|FROM[\s]+\{[\w]+\}[\w]+\.[\w]+|JOIN[\s]+\{[\w]+\}[\w]+\.[\w]+|TABLE[\s]+\{[\w]+\}[\w]+\.[\w]+|EXISTS[\s]+\{[\w]+\}[\w]+\.[\w]+', file_obj, flags=flagID)

                print("full_object", full_object)
                for ele in full_object:
                    new_ele = re.sub(r'defaultdatabase\.', '', ele, flags=flagI)
                    # print("new_ele", new_ele)
                    file_obj = file_obj.replace(ele, new_ele.upper())

                # print("file_obj", file_obj)
                common_obj.Write_file(p_file, file_obj)

        except Exception as e:
            print("Error in processing: class-Util Fun-fun_Upper_case_object", e)

    def fun_Replace_functions(self, Sql_Folder_Path):
        try:
            sqlfolderst = glob.glob(Sql_Folder_Path+'**/*.[sS][qQ][lL]', recursive=True)

            for p_file in sqlfolderst:
                file_obj = common_obj.Read_file(p_file)
                # print("file_obj", file_obj)

                file_obj = re.sub(r'CURRENT_DATETIME[\s]*\([\s]*\)', "CURRENT_DATETIME('America/Los_Angeles')", file_obj, flags=flagI)
                file_obj = re.sub(r'CURRENT_DATE\b', "CURRENT_DATE('America/Los_Angeles')", file_obj, flags=flagI)
            
                # # print("file_obj", file_obj)
                common_obj.Write_file(p_file, file_obj)

        except Exception as e:
            print("Error in processing: class-Util Fun-fun_Replace_functions", e)