# Updates the old Vault Path to the new, localized Vault Path with a find and replace - avoids replacing the Spark->TD ones

import yaml
import os

nonprod_dest = './pypeline_connection_yml/nonprod/main.yml'
prod_dest = './pypeline_connection_yml/prod/main.yml'

def add_to_yml(dest):
    with open(dest, 'r') as file:
        yml_out = yaml.safe_load(file)
    
    td_conn_list = {k: v for k, v in yml_out.items() if k.startswith('teradata_connection') and not k.endswith('_spark_td')}

    for td_var in td_conn_list:
        data = open(dest).readlines()
        for (num, line) in enumerate(data):
             if f"{td_var}:" in line:
                 data[num+5] = data[num+5].replace('/APP03676/11521/', '/APP07324/')
                 with open(dest, 'w') as file:
                    file.writelines(data)

add_to_yml(nonprod_dest)
add_to_yml(prod_dest)