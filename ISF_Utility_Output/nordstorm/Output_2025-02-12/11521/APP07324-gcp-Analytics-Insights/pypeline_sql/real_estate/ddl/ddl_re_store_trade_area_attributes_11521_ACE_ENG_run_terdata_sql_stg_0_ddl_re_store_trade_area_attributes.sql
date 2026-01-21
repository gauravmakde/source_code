SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_re_store_trade_area_attributes_11521_ACE_ENG;
     Task_Name=ddl_re_store_trade_area_attributes;'
     FOR SESSION VOLATILE;

/*

Table: T2DL_DAS_REAL_ESTATE.re_store_trade_area_attributes
Owner: Rujira Achawanantakun
Modified: 2023-05-05
Note:
- This ddl is used to create re_store_trade_area_attributes table structure into tier 2 datalab.

*/

CREATE MULTISET TABLE T2DL_DAS_REAL_ESTATE.re_store_trade_area_attributes
     ,FALLBACK
     ,NO BEFORE JOURNAL
     ,NO AFTER JOURNAL
     ,CHECKSUM = DEFAULT
     ,DEFAULT MERGEBLOCKRATIO
    (
	 store_number int not null
	,data_year int not null
	,ta_avg_drivedistance float
	,ta_avg_drivetime float
    ,land_area_square_miles	float
    ,total_population	int
    ,population_density	float
    ,total_population_5yr_proj	int
    ,average_age	float
    ,median_age	float
    ,age_0_to_4	int
    ,age_lt_1	int
    ,age_1	int
    ,age_2	int
    ,age_3	int
    ,age_4	int
    ,age_5	int
    ,age_5_to_9	int
    ,age_6	int
    ,age_7	int
    ,age_8	int
    ,age_9	int
    ,age_10	int
    ,age_10_to_14	int
    ,age_11	int
    ,age_12	int
    ,age_13	int
    ,age_14	int
    ,age_15	int
    ,age_15_to_17	int
    ,age_16	int
    ,age_17	int
    ,age_lt_18	int
    ,age_18	int
    ,age_18_to_19	int
    ,age_18_to_24	int
    ,age_18_plus	int
    ,age_19	int
    ,age_lt_20	int
    ,age_20	int
    ,age_21	int
    ,age_21_plus	int
    ,age_22_to_24	int
    ,age_25_to_29	int
    ,age_25_to_34	int
    ,age_30_to_34	int
    ,age_35_to_39	int
    ,age_35_to_44	int
    ,age_40_to_44	int
    ,age_45_to_49	int
    ,age_45_to_54	int
    ,age_50_to_54	int
    ,age_55_to_59	int
    ,age_55_to_64	int
    ,age_55_plus	int
    ,age_60_to_61	int
    ,age_62_to_64	int
    ,age_65	int
    ,age_65_to_74	int
    ,age_65_plus	int
    ,age_66	int
    ,age_67_to_69	int
    ,age_70_to_74	int
    ,age_75_to_79	int
    ,age_75_to_84	int
    ,age_80_to_84	int
    ,age_85_plus	int
    ,white	int
    ,black_or_african_american	int
    ,american_indian_alaska_native	int
    ,asian	int
    ,native_hawaiian_pacific_isld	int
    ,some_other_race	int
    ,two_or_more_races	int
    ,caucasian	int
    ,african_american	int
    ,asian_or_pacific_islander	int
    ,hispanic_latino	int
    ,other	int
    ,total_population_age_25_plus	int
    ,college_student_population	int
    ,no_schooling_completed	int
    ,nursery_to_4th_grade	int
    ,grade_5th_and_6th	int
    ,grade_7th_and_8th	int
    ,grade_9th	int
    ,grade_10th	int
    ,grade_11th	int
    ,grade_12th_or_no_diploma	int
    ,hs_grad_or_ged_or_alternative	int
    ,some_coll_or_less_than_1yr	int
    ,some_coll_1yr_or_more_no_degr	int
    ,associate_degree	int
    ,bachelors_degree	int
    ,masters_degree	int
    ,professional_school_degree	int
    ,doctorate_degree	int
    ,college_undergraduate	int
    ,graduate_or_prof_school	int
    ,less_than_high_school	int
    ,some_college	int
    ,college_degree	int
    ,adult_population	int
    ,potential_best_customers	int
    ,a01_american_royalty	int
    ,a02_platinum_prosperity	int
    ,a03_kids_and_cabernet	int
    ,a04_picture_perfect_families	int
    ,a05_couples_with_clout	int
    ,a06_jet_set_urbanites	int
    ,b07_across_the_ages	int
    ,b08_babies_and_bliss	int
    ,b09_family_fun_tastic	int
    ,b10_cosmopolitan_achievers	int
    ,c11_sophisticated_city_dweller	int
    ,c12_golf_carts_and_gourmets	int
    ,c13_philanthropicsophisticates	int
    ,c14_boomers_and_boomerangs	int
    ,d15_sports_utility_families	int
    ,d16_settled_in_suburbia	int
    ,d17_cul_de_sac_diversity	int
    ,d18_suburban_nightlife	int
    ,e19_consummate_consumers	int
    ,e20_no_place_like_home	int
    ,e21_unspoiled_splendor	int
    ,f22_fast_track_couples	int
    ,f23_families_matter_most	int
    ,g24_ambitious_singles	int
    ,g25_urban_edge	int
    ,h26_progressive_assortment	int
    ,h27_life_of_leisure	int
    ,h28_everyday_moderates	int
    ,h29_destination_recreation	int
    ,i30_potlucks_and_grt_outdoors	int
    ,i31_hard_working_values	int
    ,i32_steadfast_conventionalists	int
    ,i33_balance_and_harmony	int
    ,j34_suburban_sophisticates	int
    ,j35_rural_escape	int
    ,j36_settled_and_sensible	int
    ,k37_wired_for_success	int
    ,k38_modern_blend	int
    ,k39_metro_fusion	int
    ,k40_bohemian_groove	int
    ,l41_booming_and_consuming	int
    ,l42_rooted_flower_power	int
    ,l43_homemade_happiness	int
    ,m44_creative_comfort	int
    ,m45_growing_and_expanding	int
    ,n46_true_grit_americans	int
    ,n47_countrified_pragmatics	int
    ,n48_rural_southern_bliss	int
    ,n49_touch_of_tradition	int
    ,o50_full_steam_ahead	int
    ,o51_digitally_savvy	int
    ,o52_urban_ambition	int
    ,o53_colleges_and_cafes	int
    ,o54_influenced_by_influencers	int
    ,o55_family_troopers	int
    ,p56_mid_scale_medley	int
    ,p57_modest_metro_means	int
    ,p58_heritage_heights	int
    ,p59_expanding_horizons	int
    ,p60_striving_forward	int
    ,p61_simple_beginnings	int
    ,q62_enjoying_retirement	int
    ,q63_footloose_and_family_free	int
    ,q64_established_in_society	int
    ,q65_mature_and_wise	int
    ,r66_ambitious_dreamers	int
    ,r67_passionate_parents	int
    ,s68_small_town_sophisticates	int
    ,s69_urban_legacies	int
    ,s70_thrifty_singles	int
    ,s71_modest_retirees	int
    ,u99_unclassified	int
    ,rk_t5_mosaic_sgm_pop	int
    ,rk_t10_mosaic_sgm_pop	int
    ,rk_b10_mosaic_sgm_pop	int
    ,bargainista_t5_mosaic_sgm_pop	int
    ,bargainista_t10_mosaic_sgm_pop	int
    ,designer_mosaic_sgm_pop	int
    ,ns_t5_mosaic_sgm_pop	int
    ,ns_t10_mosaic_sgm_pop	int
    ,total_population_age_16_plus	int
    ,empld_mgmt_profess_occu	int
    ,empld_service_occu	int
    ,empld_sales_and_office_occu	int
    ,empld_farm_fish_forestry_occu	int
    ,empld_con_extra_maint_rep_occu	int
    ,empld_prd_tran_mat_moving_occu	int
    ,empld_blue_collar_occu	int
    ,empld_service_and_farming_occu	int
    ,empld_white_collar_occu	int
    ,empld_pop_age_16_plus	int
    ,professionals	int
    ,total_employees	int
    ,total_establishments	int
    ,total_households	int
    ,hhld_w_children_age_0_to_5	int
    ,hhld_w_children_age_6_to_17	int
    ,average_hhld_income	int
    ,median_hhld_income	int
    ,hhld_income_lt_10000	int
    ,hhld_income_10000_to_14999	int
    ,hhld_income_15000_to_19999	int
    ,hhld_income_20000_to_24999	int
    ,hhld_income_25000_to_29999	int
    ,hhld_income_30000_to_34999	int
    ,hhld_income_35000_to_39999	int
    ,hhld_income_40000_to_44999	int
    ,hhld_income_45000_to_49999	int
    ,hhld_income_50000_to_59999	int
    ,hhld_income_60000_to_74999	int
    ,hhld_income_75000_to_99999	int
    ,hhld_income_100000_to_124999	int
    ,hhld_income_125000_to_149999	int
    ,hhld_income_150000_to_199999	int
    ,hhld_income_200000_to_249999	int
    ,hhld_income_250000_to_499999	int
    ,hhld_income_lt_50000	int
    ,hhld_income_50000_to_74999	int
    ,hhld_income_75000_plus	int
    ,hhld_income_100000_plus	int
    ,hhld_income_150000_plus	int
    ,hhld_income_250000_plus	int
    ,hhld_income_500000_plus	int
    ,total_households_5yr_proj	int
    ,apparel_and_services_spend	int
    ,women_apparel_spend	int
    ,girl_apparel_spend	int
    ,men_apparel_spend	int
    ,boy_apparel_spend	int
    ,infant_apparel_spend	int
    ,footwear_spend	int
    ,women_footwear_spend	int
    ,girl_footwear_spend	int
    ,men_footwear_spend	int
    ,boy_footwear_spend	int
    ,accessories_spend	int
    ,women_accessories_spend	int
    ,girl_accessories_spend	int
    ,men_accessories_spend	int
    ,boy_accessories_spend	int
    ,infant_accessories_spend	int
    ,jewelry_spend	int
    ,watches_spend	int
    ,apparel_spend_per_hhld	float
    ,total_hotels	int
	,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
)
PRIMARY INDEX(store_number, data_year)
;

-- Table Comment
COMMENT ON  T2DL_DAS_REAL_ESTATE.re_store_trade_area_attributes IS 'Store level demographic attributes';

SET QUERY_BAND = NONE FOR SESSION;