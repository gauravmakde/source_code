select ( select start_wk_idnt from    (
		select c.week_num as start_wk_idnt , rank() over (order by c.week_num desc) 	wk_rank
		from {db_env}_nap_base_vws.day_cal_454_dim a join {db_env}_nap_base_vws.etl_batch_dt_lkup b
		on b.interface_code='mric_day_agg_wkly' and b.dw_batch_dt=a.day_date
		join {db_env}_nap_base_vws.week_cal_vw c on c.week_num<= a.week_idnt 
		) end_wk where wk_rank = 26 ) start_wk_idnt
		, 
		( select end_wk_idnt from    (
		select c.week_num as end_wk_idnt , rank() over (order by c.week_num desc) 	wk_rank
		from {db_env}_nap_base_vws.day_cal_454_dim a join {db_env}_nap_base_vws.etl_batch_dt_lkup b
		on b.interface_code='mric_day_agg_wkly' and b.dw_batch_dt=a.day_date
		join {db_env}_nap_base_vws.week_cal_vw c on c.week_num<= a.week_idnt 
		) end_wk where wk_rank = 1 ) end_week_idnt;
