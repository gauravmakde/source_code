
# import configparser
import os
import re
# import sqlparse
import sys
import chardet
import glob
import shutil


flagID=re.I|re.DOTALL
flagI=re.I
flagD=re.DOTALL

## Subtitude from regex
class Sub:

    # Ignorecase
    def subI(self, old, new, obj):
        obj = re.sub(r""+old, new, obj, flags=flagI)
        return obj

    # Dotall
    def subD(self, old, new, obj):
        obj = re.sub(r""+old, new, obj, flags=flagD)
        return obj

    # Ignorecase|Dotall
    def subID(self, old, new, obj):
        obj = re.sub(r""+old, new, obj, flags=flagID)
        return obj

sub = Sub()

class Common:
    def FileList(self, path):
        return glob.glob(path + '**/*', recursive = True)

    def Read_file(self, file):
        with open(file, "rb") as f:
            result = chardet.detect(f.read())
            e = result['encoding']
        f.close()
        with open(file, "r", encoding = e) as f:
            file_obj = f.read()
        f.close()
        return file_obj

    def Readlines_file(self, file):
        with open(file, "rb") as f:
            result = chardet.detect(f.read())
            e = result['encoding']
        with open(file, "r", encoding = e) as f:
            file_obj = f.readlines()
            f.close()
        return file_obj

    def Write_file(self, file, file_obj):
        with open(file, "w") as w:
            w.write(file_obj)
            w.close()

    def Remove_comments(self, file):
        fileLines = common_obj.Readlines_file(file)
        comment = 0
        counter = 0
        obj = ''
        final_obj = ''
        for fileLine in fileLines:
            fileLine = re.sub(r"\/\*.*?\*\/", "", fileLine)
            if "/*" in fileLine:
                if '*/' in fileLine:
                    comment = 0
                    pass
                else:
                    comment = 1
                    pass
            elif comment == 1:
                if "*/" in fileLine:
                    comment = 0
                    pass
                else:
                    pass
            elif fileLine.startswith("--"):
                pass
            elif counter == 0:
                fileLine = re.sub(r"\-\-.*?\n", "", fileLine)
                obj = ''
                obj = obj + fileLine
                final_obj = final_obj + obj
            # else:
            #     final_obj=final_obj+fileLine
        return final_obj

    def fun_Add_Semicolon_At_End_Of_File(self, file):
        try:
            with open(file, "rb") as f:
                result = chardet.detect(f.read())
                e = result['encoding']
            with open(file,'r',encoding=e) as f:
                filelines=f.readlines()
                comment=0
                # cmt_obj=''
                # fileobj=''
                file_len=len(filelines)-1
                for idx,line in enumerate(filelines[::-1]):
                    if comment == 0 and line.strip().endswith('*/'):
                        comment=1
                        # cmt_obj+=line
                        if '/*' in line:
                            comment=0
                    elif comment == 1:
                        if '/*' in line:
                            comment = 0
                    elif line.strip().startswith('--') or line.strip() == '':
                        pass
                    else:
                        if not line.strip().endswith(';'):
                            filelines[file_len-idx]=line+';\n'
                        break

            fileobj=''
            for line in filelines:
                fileobj+=line
            with open(file,'w',encoding=e) as w:
                w.write(fileobj)
        except Exception as e:
            print(e)
            print("Error in processing: ", file)
            print("Unexpected error:", sys.exc_info()[0])

    def create_output_folder(self, name, folder_path):
        name = name + '/'
        try:
            if os.path.isdir(name):
                shutil.rmtree(name)
            shutil.copytree(folder_path, name)
        except Exception as e:
            print(e)
        
        return name


    # def specific_files(self, file_list, contain_and_str_lst, contain_or_str_lst):

    #     # any(ext in file for ext in extensions)

    #     return [file for file in file_list if os.path.isfile(file) and any(ext in file for ext in contain_and_str_lst)]

        # name = name + '/'
        # try:
        #     if os.path.isdir(name):
        #         shutil.rmtree(name)
        #     shutil.copytree(folder_path, name)
        # except Exception as e:
        #     print(e)
        
        # return name





common_obj = Common()