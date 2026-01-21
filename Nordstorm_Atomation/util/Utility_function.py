#
# from functions.common.config_parser import *
# from functions.common.common_fun import *
import glob,sys
import re
import json

class Util:

    def dag_name_from_csv(seld,sql_file, file_name, location):



    def fetch_key(self,sql_file, file_name, location):

        data=[]
        with open(sql_file, "r") as f:
            py = f.read()
            print(py)
            pattern = re.compile("(PRIMARY KEY\s*\(([^)]+)\)\s*NOT ENFORCED)")
            search_primary_key = pattern.search(py)

            pattern_cluster = re.compile("(CLUSTER BY\s*([^)]+));")
            search_cluster_key = pattern_cluster.search(py)
            status_primary_key = ""
            status_cluster_key = ""
            primary_key_values = ""
            cluster_by_remove = ""
            cluster_key = ""
            primary_sub = py

            if search_primary_key:
                primary_key_values = search_primary_key.group(2)
                primary_key = search_primary_key.group(1)
                status_primary_key = "Primary Key is present"

                primary_by_remove = primary_key
                for primary_key_value in primary_key_values.split(","):
                    pattern_cluster_check = re.compile("[^\w*]" + primary_key_value.strip() + "\s*STRING")
                    primary_key_check = pattern_cluster_check.search(py)

                    if primary_key_check:
                        primary_by_remove = primary_by_remove.replace(primary_key_value.strip() + ",", "").replace(
                            primary_key_value.strip(), "")

                remove_space_bracket = re.sub("PRIMARY KEY \((\s*)\) NOT ENFORCED", "PRIMARY KEY () NOT ENFORCED",
                                              primary_by_remove)

                if remove_space_bracket.strip() == "PRIMARY KEY () NOT ENFORCED":
                    primary_sub = re.sub("(,\nPRIMARY KEY\s*\(([^)]+)\)\s*NOT ENFORCED)", "", py)
                else:
                    primary_sub = re.sub("PRIMARY KEY\s*\(([^)]+)\)\s*NOT ENFORCED", remove_space_bracket.strip(), py)

            if search_cluster_key:
                cluster_keys = search_cluster_key.group(2) if search_cluster_key.group(2) else search_cluster_key.group(
                    3)
                status_cluster_key = "Cluster Key is present"

                cluster_by_remove = search_cluster_key.group(1)
                for cluster_key in cluster_keys.split(","):
                    pattern_cluster_check = re.compile("[^\w*]" + cluster_key.strip() + "\s*STRING")
                    cluster_check = pattern_cluster_check.search(py)
                    if cluster_check:
                        cluster_by_remove = cluster_by_remove.replace(cluster_key.strip() + ",", "").replace(
                            cluster_key.strip(), "")

            if cluster_by_remove.strip() == "CLUSTER BY":

                sub = re.sub("(CLUSTER BY\s*([^)]+))(;)", "\\3", primary_sub)
            else:
                sub = re.sub("(CLUSTER BY\s*([^)]+))(;)", cluster_by_remove.strip() + "\\3", primary_sub)
            data.append({
                'File_name': file_name,
                'location': location,
                'primary_key_status': status_primary_key,
                'primary_value': primary_key_values,
                'cluster_key_status': status_cluster_key,
                'cluster_value': cluster_key
            })

        return data, sub

    def replace_period_sequence(self,location, file_name, file):
        print(location)

        with open(file, "r") as f:
            py = f.read()
            print(py)
            replace_period = ""

            pattern_period = re.compile("([^\w]PERIOD\s*FOR\s*[^;\n]+,)")
            present_period = pattern_period.search(py)

            if present_period:
                replace_period = re.sub("SEQUENCED VALIDTIME", "/* SEQUENCED VALIDTIME */",
                                        pattern_period.sub("--\\1", py))

                return replace_period

        return py


multiple_utility = Util()
# multiple_utility.sample()

