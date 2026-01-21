# standard imports
import os

# local imports
os.sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.SqlExecutor import SqlExecutor


if __name__ == "__main__":

    sqlexec = SqlExecutor()

    if sqlexec.config['run_ddl']:
        sql_ddl = sqlexec.get_file_contents(sqlexec.config['ddl_filepath'])
        sqlexec.logger.info('Running DDL...')
        sqlexec.exec_sql_with_splitter(sql_ddl.format(**sqlexec.config['params']))
        sqlexec.logger.info('DDL complete.')

    sql_main = sqlexec.get_file_contents(sqlexec.config['main_filepath'])

    if sqlexec.config['run_main']:
        sqlexec.logger.info('Running main sql...')
        sqlexec.exec_sql_with_splitter(sql_main.format(**sqlexec.config['params']))
        sqlexec.logger.info('SQL complete.')

    if sqlexec.config['run_loop']:
        # check for loop markers
        if not '-- begin' in sql_main:
            raise LookupError('Missing `-- begin` in sql')
        if not '-- end' in sql_main:
            raise LookupError('Missing `-- end` in sql')
        # get sql strings
        sql_begin = sql_main.split('-- begin')[0]
        sql_loop = sql_main.split('-- begin')[1].split('-- end')[0]
        temp_table_names = [x.split(' ')[0] for x in sql_loop.split('CREATE MULTISET VOLATILE TABLE ')[1:]]
        sql_drop = '\n'.join(['DROP TABLE ' + name + ';' for name in temp_table_names])
        sql_end = sql_main.split('-- end')[1]
        # run beginning
        sqlexec.logger.info('Running beginning of sql...')
        sqlexec.exec_sql_with_splitter(sql_begin.format(**sqlexec.config['params']))
        sqlexec.logger.info('Beginning of query complete.')
        # run loop over date ranges
        sqlexec.logger.info('Running loop over sql...')
        # TODO: option to get list of date ranges
        # date_sql =  """
        #             SELECT 
        #                 DISTINCT CASE WHEN fiscal_month_num = 1 THEN month_start_day_date 
        #                     WHEN fiscal_month_num = 6 THEN month_end_day_date
        #                     WHEN fiscal_month_num = 7 THEN month_start_day_date 
        #                     WHEN fiscal_month_num = 12 THEN month_end_day_date END AS day_date
        #             FROM prd_nap_usr_vws.day_cal_454_dim 
        #             WHERE fiscal_year_num in (2022,2021,2020,2019)
        #                 AND fiscal_month_num in (1,6,7,12) -- 6 month chunks
        #             ORDER BY 1
        #             """
        for i, loop_values in enumerate(sqlexec.config['params']['loop_values']):
            if i != 0:
                sqlexec.logger.info('Dropping temp tables before next loop...')
                sqlexec.exec_sql_with_splitter(sql_drop)
            loop_params = sqlexec.config['params'].copy()
            for name, value in zip(loop_params['loop_value_names'], eval(loop_values)):
                loop_params[name] = value
            sqlexec.logger.info(f'Running loop {i+1}.')
            sqlexec.logger.info(f'Loop variables {loop_params}')
            sqlexec.exec_sql_with_splitter(sql_loop.format(**loop_params))
        sqlexec.logger.info('Loops complete.')

        sqlexec.logger.info('Running remaining sql...')
        sqlexec.exec_sql_with_splitter(sql_end.format(**sqlexec.config['params']))
        sqlexec.logger.info('SQL complete.')

    sqlexec.td_conn.close()
