SET QUERY_BAND = 'App_ID=app04216; DAG_ID=marketing_mix_model_store_hrs_teradata_6761_DAS_MARKETING_das_marketing_insights; Task_Name=marketing_mix_model_store_hrs_teradata_job;'
FOR SESSION VOLATILE;

ET;

CREATE VOLATILE multiset TABLE store_hrs (  
        indate                            DATE FORMAT 'YYYY-MM-DD',	
        dept_desc                         VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
        store_num                         VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
        dept_num                          VARCHAR(150) CHARACTER SET UNICODE NOT CASESPECIFIC,
        tot_hrs                           DECIMAL(20,12)
) ON COMMIT PRESERVE ROWS; 

ET;

insert into store_hrs
select 
cast(in_time as date) As indate,
OREPLACE(OREPLACE (OREPLACE(OREPLACE(COALESCE ((cast(t.override_codes_department as varchar(10))),
(cast(payroll_department as varchar(10))) ),'5607','Delivery'),'8821','Fulfillment'),'5559','Service Experience'),'5348','Cash Room')as dept_desc,
COALESCE ((cast(t.override_codes_store as varchar(10))),(cast(payroll_store as varchar(10))) ) AS store_num,
COALESCE ((cast(t.override_codes_department as varchar(10))),(cast(payroll_department as varchar(10))) ) AS dept_num,
sum(t.total_hours) as tot_hrs
FROM 

(SELECT 
v.worker_number,
og.payroll_department,
og.payroll_store,
og.eff_begin_date,
og.eff_end_date,
v.eff_begin_date as vb,
v.eff_end_date as ve
FROM prd_nap_hr_usr_vws.hr_job_details_dim_eff_date_vw v
 left join prd_nap_hr_usr_vws.hr_org_details_dim_eff_date_vw og
on v.worker_number =og.worker_number 
join prd_nap_hr_usr_vws.HR_WORKER_JOB_DIM as j
on j.job_profile_id=v.job_profile_id
and j.job_title not like '%Mgr%'
--and og.cost_center<>'41104'
) as W


JOIN 
prd_nap_hr_usr_vws.hr_timeblock_fact as t
ON
t.worker_number=W.worker_number
and  ( t.in_time  ) >=  (W.vb) and ( t.in_time) <= (W.ve)
and (t.in_time)>=(W.eff_begin_date) and (t.in_time)<=(W.eff_end_Date)

where 
cast(t.in_time as date) between (current_date - 14) and (current_date - 1)
--and W.cost_center<>'41104'
and (dept_desc like '%delivery%' or dept_desc like '%fulfillment%' or dept_desc like '%service experience%' or dept_desc like '%cash room%' )
and dept_desc is not null

/*and FSTORE in ('0001','0002','0004','0005','0006','0009','0010','0012','0020','0024','0025','0032','0034','0035','0037','0057','0073','0201','0202','0210','0212','0220','0221','0222','0223','0225',
'0227','0228','0230','0232','0234','0235','0236','0237','0238','0239','0240','0282','0283','0320','0321','0322','0323','0325','0326','0330','0333','0340','0341','0342','0344','0345','0348','0349',
'0353','0360','0361','0364','0380','0381','0384','0386','0420','0421','0422','0423','0425','0426','0427','0429','0430','0434','0520','0521','0522','0523','0524','0526','0527','0531','0534','0535',
'0536','0538','0600','0621','0622','0623','0626','0628','0629','0631','0635','0637','0639','0701','0706','0720','0722','0723','0724','0730','0731','0732','0733','0746','0750','0751','0759','0760',
'0762','0763','0765','0766','0768','0772','0773','0774','0775','0789','0830','0831','0832','0833','0834','0835')*/

GROUP BY 
1,2,3,4


union


select 

cast(in_time as date) As indate,
OREPLACE(OREPLACE(OREPLACE(OREPLACE (OREPLACE(OREPLACE(OREPLACE(OREPLACE (OREPLACE(OREPLACE(COALESCE ((cast(t.override_codes_department as varchar(10))),
(cast(payroll_department as varchar(10))) ),'2080','Service Experience'),'2080','Cash Room'),'5607','Delivery')
,'2065','Selling Floor'),'3916','Inventory Management'),'2064','Merchandising and Inventory'),'2030','Accessories'),'2060','Shoes')
,'2032','Accessories/Shoes'),'2048','Womens/Mens/Kids')
as pd_2,
COALESCE ((cast(t.override_codes_store as varchar(10))),(cast(payroll_store as varchar(10))) ) AS FSTORE,
COALESCE ((cast(t.override_codes_department as varchar(10))),(cast(payroll_department as varchar(10))) ) AS FDEPT,
sum(t.total_hours) as tot_hrs
FROM 
(SELECT 
v.worker_number,
og.cost_center,
og.payroll_department,
og.payroll_store,
og.eff_begin_date,
og.eff_end_date,
v.eff_begin_date as vb,
v.eff_end_date as ve
FROM PRD_NAP_HR_USR_VWS.HR_JOB_DETAILS_DIM_EFF_DATE_VW v
 left JOIN PRD_NAP_HR_USR_VWS.HR_ORG_DETAILS_DIM_EFF_DATE_VW og
on v.worker_number =og.worker_number 
join prd_nap_hr_usr_vws.HR_WORKER_JOB_DIM as j
on j.job_profile_id=v.job_profile_id
and j.job_title not like '%Mgr%'
--and og.cost_center<>'41104'
) as W



JOIN 
prd_nap_hr_usr_vws.hr_timeblock_fact as t
ON
t.worker_number=W.worker_number
and  ( t.in_time  ) >=  (W.vb) and ( t.in_time) <= (W.ve)
and (t.in_time)>=(W.eff_begin_date) and (t.in_time)<=(W.eff_end_Date)

where 
cast(t.in_time as date) between (current_date - 14) and (current_date - 1)
and (pd_2 like '%delivery%' or pd_2 like '%fulfillment%' or pd_2 like '%service experience%' or pd_2 like '%cash room%' or pd_2 like '%selling floor%' or pd_2 like '%Inventory Management%' 
or pd_2 like '%Accessories%' or pd_2 like '%Shoes%' or pd_2 like '%Accessories/Shoes%' or pd_2 like '%Womens/Mens/Kids%' or pd_2 like '%Merchandising and Inventory%')
--and W.cost_center<>'41104'

/*and FSTORE in ('0003','0011','0014','0015','0016','0017','0022','0027',
'0028','0033','0036','0047','0048','0071','0072','0074','0109','0110','0111','0112','0113','
0120','0125','0130','0135','0136','0137','0138','0139','0140','0148','0150','0151','0154','0155','0160',
'0161','0162','0163','0164','0165','0166','0167','0168','0224','0229','0231','0233','0241','0242','0243',
'0244','0245','0246','0247','0248','0249','0253','0254','0256','0260','0264','0265','0266','0267','0268',
'0269','0270','0271','0272','0273','0274','0275','0276','0277','0278','0279','0280','0281','0284','0285',
'0286','0287','0288','0289','0328','0329','0331','0332','0334','0336','0337','0338','0347',
'0350','0351','0352','0354','0356','0357','0358','0359','0363','0366','0367','0368','0369','0370',
'0371','0372','0373','0374','0376','0377','0378','0379','0383','0388','0389','0393','0396','0411',
'0416','0428','0431','0432','0433','0435','0470','0471','0472','0474','0475','0476','0477','0478',
'0479','0481','0482','0483','0484','0509','0510','0511','0513','0515','0519','0529','0533','0537',
'0539','0541','0542','0543','0544','0545','0546','0547','0548','0550','0551','0552','0553','0554',
'0555','0560','0624','0625','0627','0633','0634','0640','0641','0642','0643','0644','0645','0646',
'0647','0648','0649','0650','0651','0652','0656','0660','0661','0670','0671','0673','0674','0675',
'0676','0677','0708','0709','0711','0712','0713','0714','0715','0716','0717','0719','0721','0727',
'0734','0735','0736','0738','0739','0740','0741','0743','0744','0745','0747','0748','0749','0754','0755',
'0756','0757','0758','0764','0770','0771','0777','0778','0779','0781','0782','0783','0784','0785','0786',
'0787','0788','0791','0792','0793','0796','0797','0798','0840','0841','0842','0843','0844','0845','0846','0691',
'0491','0200','0400','0413','0450','0250','0441','0658','0692','0403','0414','0485','0497','0417','0700')*/

GROUP BY 
1,2,3,4


union

select 

cast(in_time as date) As indate,


OREPLACE(OREPLACE(OREPLACE(OREPLACE (OREPLACE(OREPLACE(OREPLACE(OREPLACE (OREPLACE(OREPLACE(COALESCE ((cast(t.override_codes_department as varchar(10))),
(cast(payroll_department as varchar(10))) ),'2080','Service Experience'),'2080','Cash Room'),'5607','Delivery')
,'2065','Selling Floor'),'3916','Inventory Management'),'2064','Merchandising and Inventory'),'2030','Accessories'),'2060','Shoes')
,'2032','Accessories/Shoes'),'2048','Womens/Mens/Kids')
as pd_2,
COALESCE ((cast(t.override_codes_store as varchar(10))),(cast(payroll_store as varchar(10))) ) AS FSTORE,   
COALESCE ((cast(t.override_codes_department as varchar(10))),(cast(payroll_department as varchar(10))) ) AS FDEPT, 
sum(t.total_hours) as tot_hrs

FROM 
(SELECT 
v.worker_number,
og.cost_center,
og.payroll_department,
og.payroll_store,
og.eff_begin_date,
og.eff_end_date,
v.eff_begin_date as vb,
v.eff_end_date as ve
FROM PRD_NAP_HR_USR_VWS.HR_JOB_DETAILS_DIM_EFF_DATE_VW v
 left JOIN PRD_NAP_HR_USR_VWS.HR_ORG_DETAILS_DIM_EFF_DATE_VW og
on v.worker_number =og.worker_number 
join prd_nap_hr_usr_vws.HR_WORKER_JOB_DIM as j
on j.job_profile_id=v.job_profile_id
and j.job_title  like 'Asst Mgr%'
--and og.cost_center<>'41104'
) as W


JOIN 
prd_nap_hr_usr_vws.hr_timeblock_fact as t
ON
t.worker_number=W.worker_number
and  ( t.in_time  ) >=  (W.vb) and ( t.in_time) <= (W.ve)
and (t.in_time)>=(W.eff_begin_date) and (t.in_time)<=(W.eff_end_Date)

where 
cast(t.in_time as date) between (current_date - 14) and (current_date - 1)
--and pd_2 is not null
and (pd_2 like '%delivery%' or pd_2 like '%fulfillment%' or pd_2 like '%service experience%' or pd_2 like '%cash room%' or pd_2 like '%selling floor%' or pd_2 like '%Inventory Management%' 
or pd_2 like '%Accessories%' or pd_2 like '%Shoes%' or pd_2 like '%Accessories/Shoes%' or pd_2 like '%Womens/Mens/Kids%' or pd_2 like '%Merchandising and Inventory%')
--and W.cost_center<>'41104'

/*and FSTORE in ('0003','0011','0014','0015','0016','0017','0022','0027',
'0028','0033','0036','0047','0048','0071','0072','0074','0109','0110','0111','0112','0113','
0120','0125','0130','0135','0136','0137','0138','0139','0140','0148','0150','0151','0154','0155','0160',
'0161','0162','0163','0164','0165','0166','0167','0168','0224','0229','0231','0233','0241','0242','0243',
'0244','0245','0246','0247','0248','0249','0253','0254','0256','0260','0264','0265','0266','0267','0268',
'0269','0270','0271','0272','0273','0274','0275','0276','0277','0278','0279','0280','0281','0284','0285',
'0286','0287','0288','0289','0328','0329','0331','0332','0334','0336','0337','0338','0347',
'0350','0351','0352','0354','0356','0357','0358','0359','0363','0366','0367','0368','0369','0370',
'0371','0372','0373','0374','0376','0377','0378','0379','0383','0388','0389','0393','0396','0411',
'0416','0428','0431','0432','0433','0435','0470','0471','0472','0474','0475','0476','0477','0478',
'0479','0481','0482','0483','0484','0509','0510','0511','0513','0515','0519','0529','0533','0537',
'0539','0541','0542','0543','0544','0545','0546','0547','0548','0550','0551','0552','0553','0554',
'0555','0560','0624','0625','0627','0633','0634','0640','0641','0642','0643','0644','0645','0646',
'0647','0648','0649','0650','0651','0652','0656','0660','0661','0670','0671','0673','0674','0675',
'0676','0677','0708','0709','0711','0712','0713','0714','0715','0716','0717','0719','0721','0727',
'0734','0735','0736','0738','0739','0740','0741','0743','0744','0745','0747','0748','0749','0754','0755',
'0756','0757','0758','0764','0770','0771','0777','0778','0779','0781','0782','0783','0784','0785','0786',
'0787','0788','0791','0792','0793','0796','0797','0798','0840','0841','0842','0843','0844','0845','0846','0691',
'0491','0200','0400','0413','0450','0250','0441','0658','0692','0403','0414','0485','0497','0417','0700' )*/

GROUP BY 
1,2,3,4; 

ET;

--DELETING AND INSERTING DATA IN THE LANDING TABLE
DELETE FROM {proto_schema}.MMM_STORE_HRS_LDG ALL;

INSERT INTO {proto_schema}.MMM_STORE_HRS_LDG
select
indate,                            
dept_desc,                         
store_num,                         
dept_num,                          
tot_hrs,           
current_date as dw_batch_date,
current_timestamp as dw_sys_load_tmstp
from store_hrs;

ET;

SET QUERY_BAND = NONE FOR SESSION;

ET;