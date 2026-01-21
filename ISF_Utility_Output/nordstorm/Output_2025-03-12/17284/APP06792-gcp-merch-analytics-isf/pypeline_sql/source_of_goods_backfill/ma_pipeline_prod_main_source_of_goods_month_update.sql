update {dsa_ai_secure_schema}.sog_backfill_lkup
set week_num = (sel min(week_idnt) from PRD_nap_base_vws.day_cal_454_dim 
where week_idnt > (sel week_num from {dsa_ai_secure_schema}.sog_backfill_lkup));