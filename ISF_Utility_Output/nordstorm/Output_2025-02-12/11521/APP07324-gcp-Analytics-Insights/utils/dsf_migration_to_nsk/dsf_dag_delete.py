##############################################################################################################
# Code to create a JSON from all existing DSF AE DAG's to perform a DAG DELETE on DSF before NSK switchover  #
##############################################################################################################

import json
import csv

def create_delete_dags_json(env):
    delete_dict = {"dags": [], "backfill_dags": []}
    
    with open(f'./utils/dsf_migration_to_nsk/{env}_dsf_dags.csv', newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for row in spamreader:
            string = ' '.join(row)
            if string.startswith('backfill_'):
                delete_dict['backfill_dags'].append(string)
            else:
                delete_dict['dags'].append(string)
            
    with open(f"./utils/dsf_migration_to_nsk/{env}_dsf_delete.json", "w") as outfile:
        outfile.write(json.dumps(delete_dict, indent=4))

create_delete_dags_json('dev')
create_delete_dags_json('prod')