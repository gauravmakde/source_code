import glob

########################################################################
# Adds whitespace to every JSON                                        #
########################################################################

dags = glob.glob('./pypeline_jobs/*.json')

for dag in dags:
    dag_lines = open(dag).readlines()
    dag_lines[-1] = dag_lines[-1]+" "
    with open(dag, 'w') as file:
        file.writelines(dag_lines)
    # for (num, line) in enumerate(dag_lines):
    #     dag_lines[1] = dag_lines[1].join(" ")
    #     with open(dag, 'w') as file:
    #             file.writelines(dag_lines)
    #     break