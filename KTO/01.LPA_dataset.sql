-- python LPA LPA_dataset.csv 파일
-- , T999.result_group
SELECT T000.SIDO_NM, T000.SGG_NM, T000.REGN_CTGR
      , T001.*
      , T003.SGG_CONT_TOU_NUM
      , T004.AVG_LODG_DAYS
      , T005.AVG_STAY_TM
      , T201.natur_tar_num
      , T201.acad_tar_num
      , T201.rpts_tar_num
      , T201.shpn_tar_num
      , T201.food_tar_num
      , T201.lodg_tar_num
      , T201.totl_biz_num
      , T201.totl_tursm_biz_num
      , T201.tursm_shpn_num
      , T201.tursm_trs_num
      , T201.tursm_lodg_num
      , T201.tursm_food_pub_num
      , T201.tour_asstn_svc_num
      , T201.intl_mtg_num
      , T201.cltur_amsmt_rpts_ind_num
      , T201.cso_num
      , T201.totl_wker_num
      , T201.totl_male_wker_num
      , T201.totl_fmale_wker_num
      , T201.totl_tursm_biz_wker_num
      , T201.totl_tursm_biz_male_wker_num
      , T201.totl_tursm_biz_fmale_wker_num
      , T201.tursm_shpn_male_wker_num
      , T201.tursm_shpn_fmale_wker_num
      , T201.tursm_trs_male_wker_num
      , T201.tursm_trs_fmale_wker_num
      , T201.tursm_lodg_male_wker_num
      , T201.tursm_lodg_fmale_wker_num
      , T201.tursm_food_pub_male_wker_num
      , T201.tursm_food_pub_fmale_wker_num
      , T201.tour_asstn_svc_male_wker_num
      , T201.tour_asstn_svc_fmale_wker_num
      , T201.intl_mtg_male_wker_num
      , T201.intl_mtg_fmale_wker_num
      , T201.cltur_amsmt_rpts_ind_male_wker_num
      , T201.cltur_amsmt_rpts_ind_fmale_wker_num
      , T201.cso_male_wker_num
      , T201.cso_fmale_wker_num
      , T201.fstv_num_sgg + T201.fstv_num_sido AS fstv_num
      , T201.sido_tursm_bdgt_2023
      , T201.ct_tour_num_sgg + T201.ct_tour_num_sido AS ct_tour_num
      , T201.socty_corpo_num
      , T201.car_pub_zon_num
      , T201.mrkt_totl_shop_num
      , T201.mrkt_emt_shop_num
      , T201.mrkt_emt_shop_rat
      , T202.INFO_CNT
      , T203.TRAD_COMT
      , T301.5year_popl
      , T301.popl_dnsty_rat
      , T301.youth_pur_mvmn_rat
      , T301.dtme_popl_num
      , T301.agn_rat
      , T301.boyhd_rat
      , T301.born_rate
      , T301.osf_imp_anrev_af_rat
      , T301.tens_blow_popl_num
      , T301.tens_popl_num
      , T301.twnes_popl_num
      , T301.thrtes_popl_num
      , T301.fortes_popl_num
      , T301.fites_popl_num
      , T301.sxtes_popl_num
      , T301.sevnes_over_popl_num
      , T301.popl_num_2023_10
      , T301.govern_2023
      , T302.`외국인-계` AS fore_popl_num
      , T303.GRDP
      , ifnull(T304.`전체예산대비 관광예산`,0) AS sgg_tursm_bdgt
      , IFNULL(T031.train_stp_cnt, 0) AS train_stp_cnt
      , IFNULL(T032.bus_stp_cnt, 0) AS bus_stp_cnt
      , IFNULL(T033.subway_stp_cnt, 0) AS subway_stp_cnt
      , T034.road_pvm_rat
      , T035.highway_IC_acb
      , T036.rail_acb
      , T037.park_pop_rat
FROM ods.sido_sgg_group T000
-- LEFT OUTER JOIN 
--    ods.sido_sgg_group T999
-- ON T000.sgg_cd = T999.sgg_cd
LEFT OUTER JOIN 
   (SELECT sgg_cd
     ,AVG(male_tens_visitr_rat)   AS    male_tens_visitr_rat
     ,AVG(male_twnes_visitr_rat)   AS    male_twnes_visitr_rat
     ,AVG(male_thrtes_visitr_rat)   AS    male_thrtes_visitr_rat
     ,AVG(male_fortes_visitr_rat)   AS    male_fortes_visitr_rat
     ,AVG(male_fites_visitr_rat)   AS    male_fites_visitr_rat
     ,AVG(male_sxtes_visitr_rat)   AS    male_sxtes_visitr_rat
     ,AVG(male_sevnes_visitr_rat)   AS    male_sevnes_visitr_rat
     ,AVG(fmale_tens_visitr_rat)   AS    fmale_tens_visitr_rat
     ,AVG(fmale_twnes_visitr_rat)   AS    fmale_twnes_visitr_rat
     ,AVG(fmale_thrtes_visitr_rat)   AS    fmale_thrtes_visitr_rat
     ,AVG(fmale_fortes_visitr_rat)   AS    fmale_fortes_visitr_rat
     ,AVG(fmale_fites_visitr_rat)   AS    fmale_fites_visitr_rat
     ,AVG(fmale_sxtes_visitr_rat)   AS    fmale_sxtes_visitr_rat
     ,AVG(fmale_sevnes_visitr_rat)   AS    fmale_sevnes_visitr_rat
     ,AVG(natur_tursm_srch_cnt)   AS    natur_tursm_srch_cnt
     ,AVG(hstry_tursm_srch_cnt)   AS    hstry_tursm_srch_cnt
     ,AVG(exprc_tursm_srch_cnt)   AS    exprc_tursm_srch_cnt
     ,AVG(etc_tursm_srch_cnt)   AS    etc_tursm_srch_cnt
     ,AVG(cltur_tursm_srch_cnt)   AS    cltur_tursm_srch_cnt
     ,AVG(lesu_spts_srch_cnt)   AS    lesu_spts_srch_cnt
     ,AVG(shpn_srch_cnt)   AS    shpn_srch_cnt
     ,AVG(food_srch_cnt)   AS    food_srch_cnt
     ,AVG(lodg_srch_cnt)   AS    lodg_srch_cnt
     ,AVG(usa_frgn_visitr_num)   AS    usa_frgn_visitr_num
     ,AVG(jpn_frgn_visitr_num)   AS    jpn_frgn_visitr_num
     ,AVG(chn_frgn_visitr_num)   AS    chn_frgn_visitr_num
     ,AVG(hnk_frgn_visitr_num)   AS    hnk_frgn_visitr_num
     ,AVG(twn_frgn_visitr_num)   AS    twn_frgn_visitr_num
     ,AVG(tha_frgn_visitr_num)   AS    tha_frgn_visitr_num
     ,AVG(phl_frgn_visitr_num)   AS    phl_frgn_visitr_num
     ,AVG(vnm_frgn_visitr_num)   AS    vnm_frgn_visitr_num
     ,AVG(idn_frgn_visitr_num)   AS    idn_frgn_visitr_num
     ,AVG(mys_frgn_visitr_num)   AS    mys_frgn_visitr_num
     ,AVG(rus_frgn_visitr_num)   AS    rus_frgn_visitr_num
     ,AVG(sgp_frgn_visitr_num)   AS    sgp_frgn_visitr_num
     ,AVG(lodg_tob_card_compt_amt)   AS    lodg_tob_card_compt_amt
     ,AVG(tour_tob_card_compt_amt)   AS    tour_tob_card_compt_amt
     ,AVG(trs_tob_card_compt_amt)   AS    trs_tob_card_compt_amt
     ,AVG(shpn_tob_card_compt_amt)   AS    shpn_tob_card_compt_amt
     ,AVG(sptm_svc_tob_rela_card_compt_amt)   AS    sptm_svc_tob_rela_card_compt_amt
     ,AVG(food_drk_tob_rela_card_compt_amt)   AS    food_drk_tob_rela_card_compt_amt
     ,AVG(ntv_tou_num)   AS    ntv_tou_num
     ,AVG(osd_tou_num)   AS    osd_tou_num
     ,AVG(frn_tou_num)   AS    frn_tou_num
     ,AVG(1nght_visitr_num)   AS    1nght_visitr_num
     ,AVG(2nght_visitr_num)   AS    2nght_visitr_num
     ,AVG(3nght_visitr_num)   AS    3nght_visitr_num
     ,AVG(seoul_wgt_rflt_dstnc_ojuri_tou_num)   AS    seoul_wgt_rflt_dstnc_ojuri_tou_num
     ,AVG(busan_wgt_rflt_dstnc_ojuri_tou_num)   AS    busan_wgt_rflt_dstnc_ojuri_tou_num
     ,AVG(daegu_wgt_rflt_dstnc_ojuri_tou_num)   AS    daegu_wgt_rflt_dstnc_ojuri_tou_num
     ,AVG(incheon_wgt_rflt_dstnc_ojuri_tou_num)   AS    incheon_wgt_rflt_dstnc_ojuri_tou_num
     ,AVG(gwangju_wgt_rflt_dstnc_ojuri_tou_num)   AS    gwangju_wgt_rflt_dstnc_ojuri_tou_num
     ,AVG(daejeon_wgt_rflt_dstnc_ojuri_tou_num)   AS    daejeon_wgt_rflt_dstnc_ojuri_tou_num
     ,AVG(ulsan_wgt_rflt_dstnc_ojuri_tou_num)   AS    ulsan_wgt_rflt_dstnc_ojuri_tou_num
     ,AVG(sejong_wgt_rflt_dstnc_ojuri_tou_num)   AS    sejong_wgt_rflt_dstnc_ojuri_tou_num
     ,AVG(gyeonggi_wgt_rflt_dstnc_ojuri_tou_num)   AS    gyeonggi_wgt_rflt_dstnc_ojuri_tou_num
     ,AVG(gangwon_wgt_rflt_dstnc_ojuri_tou_num)   AS    gangwon_wgt_rflt_dstnc_ojuri_tou_num
     ,AVG(chungbuk_wgt_rflt_dstnc_ojuri_tou_num)   AS    chungbuk_wgt_rflt_dstnc_ojuri_tou_num
     ,AVG(chungnam_wgt_rflt_dstnc_ojuri_tou_num)   AS    chungnam_wgt_rflt_dstnc_ojuri_tou_num
     ,AVG(jeonbuk_wgt_rflt_dstnc_ojuri_tou_num)   AS    jeonbuk_wgt_rflt_dstnc_ojuri_tou_num
     ,AVG(jeonnam_wgt_rflt_dstnc_ojuri_tou_num)   AS    jeonnam_wgt_rflt_dstnc_ojuri_tou_num
     ,AVG(gyeongbuk_wgt_rflt_dstnc_ojuri_tou_num)   AS    gyeongbuk_wgt_rflt_dstnc_ojuri_tou_num
     ,AVG(gyeongnam_wgt_rflt_dstnc_ojuri_tou_num)   AS    gyeongnam_wgt_rflt_dstnc_ojuri_tou_num
     ,AVG(jeju_wgt_rflt_dstnc_ojuri_tou_num)   AS    jeju_wgt_rflt_dstnc_ojuri_tou_num
     ,ROUND(AVG(ntv_tou_num+osd_tou_num+frn_tou_num),0)   AS    sgg_tou_num
     ,ROUND(AVG(natur_tursm_srch_cnt
            +hstry_tursm_srch_cnt
            +exprc_tursm_srch_cnt
            +etc_tursm_srch_cnt
            +cltur_tursm_srch_cnt
            +lesu_spts_srch_cnt
            +shpn_srch_cnt
            +food_srch_cnt
            +lodg_srch_cnt),0)    AS    srch_num
     ,ROUND(AVG(lodg_tob_card_compt_amt
            +tour_tob_card_compt_amt
            +trs_tob_card_compt_amt
            +shpn_tob_card_compt_amt
            +sptm_svc_tob_rela_card_compt_amt
            +food_drk_tob_rela_card_compt_amt),0) AS compt_amt
     ,ROUND(AVG(seoul_wgt_rflt_dstnc_ojuri_tou_num
         +busan_wgt_rflt_dstnc_ojuri_tou_num
         +daegu_wgt_rflt_dstnc_ojuri_tou_num
         +incheon_wgt_rflt_dstnc_ojuri_tou_num
         +gwangju_wgt_rflt_dstnc_ojuri_tou_num
         +daejeon_wgt_rflt_dstnc_ojuri_tou_num
         +ulsan_wgt_rflt_dstnc_ojuri_tou_num
         +sejong_wgt_rflt_dstnc_ojuri_tou_num
         +gyeonggi_wgt_rflt_dstnc_ojuri_tou_num
         +gangwon_wgt_rflt_dstnc_ojuri_tou_num
         +chungbuk_wgt_rflt_dstnc_ojuri_tou_num
         +chungnam_wgt_rflt_dstnc_ojuri_tou_num
         +jeonbuk_wgt_rflt_dstnc_ojuri_tou_num
         +jeonnam_wgt_rflt_dstnc_ojuri_tou_num
         +gyeongbuk_wgt_rflt_dstnc_ojuri_tou_num
         +gyeongnam_wgt_rflt_dstnc_ojuri_tou_num
         +jeju_wgt_rflt_dstnc_ojuri_tou_num),0)    AS    wgt_rflt_dstnc_tou_num
   FROM ods.`00_관광활성화지수`
   WHERE LEFT(BASE_YM,4)=2023
   GROUP BY  sgg_cd) T001
ON T000.SGG_CD = T001.SGG_CD
LEFT OUTER JOIN
   (SELECT SGG_CD, AVG(CONT_TOU_NUM) AS SGG_CONT_TOU_NUM
   FROM 
      (SELECT  T02.SGG_CD, T01.*
      FROM    (SELECT *
            FROM    ods.`04_kt관광코드기본`
            WHERE BASE_YMD = '20230930' ) T02
      LEFT OUTER JOIN 
            (SELECT CONT_ID, BASE_YM, SUM(TOU_NUM) AS CONT_TOU_NUM
            FROM ods.`20_기초지자체지역별관광자원분포, 방문자월별집계` 
            WHERE LEFT(BASE_YM,4) = 2023
            GROUP BY CONT_ID, BASE_YM) T01
      ON T01.CONT_ID = T02.CONT_ID
      ) T101
   GROUP BY SGG_CD) T003
ON T000.SGG_CD = T003.SGG_CD
LEFT OUTER JOIN 
   (SELECT SGG_CD, AVG(AVG_AVG_LODG_DAYS) AS AVG_LODG_DAYS -- 시군구별 평균 숙박일수
   FROM 
      (SELECT BASE_YM, SGG_CD, SUM(TOU_NUM)/SUM(TOU_NUM/(LODG_DAYS+1)) AS AVG_AVG_LODG_DAYS -- 시군구,월별 평균숙박일수
      FROM ods.`19_기초지자체관광객숙박연월별집계` 
      WHERE  LODG_CD != 1
      AND    LEFT(BASE_YM, 4) = '2023'
      GROUP BY BASE_YM, SGG_CD
      ) T001
   GROUP BY T001.SGG_CD
   ) T004
ON T000.SGG_CD = T004.SGG_CD
LEFT OUTER JOIN
   (SELECT SGG_CD, AVG(AVG_WSL_TOU_STAY_TM) AS AVG_STAY_TM -- 시군구별 평균체류시간
   FROM 
      (SELECT BASE_YM, SGG_CD, SUM(TOU_NUM*WSL_TOU_STAY_TM)/SUM(TOU_NUM) AS AVG_WSL_TOU_STAY_TM -- 시군구,월별 평균체류시간
      FROM ods.`19_기초지자체관광객숙박연월별집계` 
      WHERE  lodg_cd = 1
      AND    LEFT(BASE_YM, 4) = '2023'
      GROUP BY base_ym, sgg_cd
      ) T001
   GROUP BY T001.SGG_CD) T005
ON T000.SGG_CD = T005.SGG_CD
LEFT OUTER JOIN 
   dw.`02_tursm_resr` T201
ON T000.SGG_CD = T201.SGG_CD
LEFT OUTER JOIN
   (SELECT T101.SGG_CD, IFNULL(T102.CNT,0) AS INFO_CNT -- T102.RESULT_GROUP, AVG(CNT)
   FROM ods.sido_sgg_group T101
   LEFT OUTER JOIN 
      (SELECT T001.시도명, T001.시군구명, COUNT(1) AS CNT
      FROM 
         (SELECT  CASE WHEN 시도명 LIKE '강원%' THEN '강원도'
                  WHEN 시도명 LIKE '경기%' THEN '경기도'
                  WHEN 시도명 LIKE '경남%' THEN '경상남도'
                  WHEN 시도명 LIKE '경북%' THEN '경상북도'
                  WHEN 시도명 LIKE '광주%' THEN '광주광역시'
                  WHEN 시도명 LIKE '대구%' THEN '대구광역시'
                  WHEN 시도명 LIKE '대전%' THEN '대전광역시'
                  WHEN 시도명 LIKE '부산%' THEN '부산광역시'
                  WHEN 시도명 LIKE '서울%' THEN '서울특별시'
                  WHEN 시도명 LIKE '세종%' THEN '세종특별자치시'
                  WHEN 시도명 LIKE '울산%' THEN '울산광역시'
                  WHEN 시도명 LIKE '인천%' THEN '인천광역시'
                  WHEN 시도명 LIKE '전남%' THEN '전라남도'
                  WHEN 시도명 LIKE '전북%' THEN '전라북도'
                  WHEN 시도명 LIKE '제주%' THEN '제주특별자치도'
                  WHEN 시도명 LIKE '충남%' THEN '충청남도'
                  WHEN 시도명 LIKE '충북%' THEN '충청북도'
                   ELSE 시도명
                   END AS 시도명, 시군구명
         FROM  ods.안내소
         ) T001
      GROUP BY T001.시도명, T001.시군구명) T102
   ON   T101.SIDO_NM = T102.시도명
   AND T101.SGG_NM LIKE T102.시군구명
   WHERE T101.RESULT_GROUP IS NOT NULL) T202
ON T000.SGG_CD = T202.SGG_CD
LEFT OUTER JOIN
   (SELECT  T001.SGG_CD, T002.문화관광해설사_수_2017 AS TRAD_COMT -- 시군구별 문화해설사
   FROM    ods.sido_sgg_group T001
   LEFT OUTER JOIN 
      (SELECT CASE WHEN 시도명 LIKE '강원%' THEN '강원도'
                  WHEN 시도명 LIKE '경기%' THEN '경기도'
                  WHEN 시도명 LIKE '경남%' THEN '경상남도'
                  WHEN 시도명 LIKE '경북%' THEN '경상북도'
                  WHEN 시도명 LIKE '광주%' THEN '광주광역시'
                  WHEN 시도명 LIKE '대구%' THEN '대구광역시'
                  WHEN 시도명 LIKE '대전%' THEN '대전광역시'
                  WHEN 시도명 LIKE '부산%' THEN '부산광역시'
                  WHEN 시도명 LIKE '서울%' THEN '서울특별시'
                  WHEN 시도명 LIKE '세종%' THEN '세종특별자치시'
                  WHEN 시도명 LIKE '울산%' THEN '울산광역시'
                  WHEN 시도명 LIKE '인천%' THEN '인천광역시'
                  WHEN 시도명 LIKE '전남%' THEN '전라남도'
                  WHEN 시도명 LIKE '전북%' THEN '전라북도'
                  WHEN 시도명 LIKE '제주%' THEN '제주특별자치도'
                  WHEN 시도명 LIKE '충남%' THEN '충청남도'
                  WHEN 시도명 LIKE '충북%' THEN '충청북도'
                    ELSE 시도명
                    END AS 시도명
                 , 시군구명
                 , 문화관광해설사_수_2017
      FROM    ods.문화관광해설사_수 t001) T002
   ON T001.SIDO_NM = T002.시도명
   AND T001.SGG_nm LIKE T002.시군구명) T203
ON T000.SGG_CD = T203.SGG_CD
LEFT OUTER JOIN
   dw.`03_regn_popl_chrr` T301
ON T000.SGG_CD = T301.SGG_CD
LEFT OUTER JOIN
    ods.`26_외국인_인구` T302 
ON T000.SGG_CD = T302.SGG_CD
LEFT OUTER JOIN 
   (SELECT t002.SGG_CD, t001.당해년가격_2020 AS GRDP
   FROM    ods.`16_GRDP` t001 
   LEFT OUTER JOIN ods.sido_sgg_group t002
   ON    t001.SIDO_NM = t002.SIDO_NM 
   AND t001.SGG_NM = t002.SGG_NM 
   WHERE t002.SGG_NM IS NOT NULL) T303
ON T000.SGG_CD = T303.SGG_CD
LEFT OUTER JOIN
     ods.`05_관광예산` T304
ON T000.SGG_CD = T304.SGG_CD
LEFT OUTER JOIN 
	(SELECT sgg_cd, count(1) AS train_stp_cnt
	FROM ods.`31_기차정보`
	GROUP BY sgg_cd) T031
ON T000.sgg_cd = T031.sgg_cd
LEFT OUTER JOIN 
	(SELECT sgg_cd, count(1) AS bus_stp_cnt
	FROM ods.`32_버스정류소_위치정보`
	GROUP BY sgg_cd) T032
ON T000.sgg_cd = T032.sgg_cd
LEFT OUTER JOIN 
	(SELECT sgg_cd, count(1) AS subway_stp_cnt
	FROM ods.`33_도시철도역사정보`
	GROUP BY sgg_cd) T033
ON T000.sgg_cd = T033.sgg_cd
LEFT OUTER JOIN 
	(SELECT  시도, 시군구, 지표명, IFNULL(avg(지자체_2021),0) AS road_pvm_rat
	FROM 	ods.NABIS_교통 
	WHERE 	지표명 = '도로포장율'
	GROUP BY 시도, 시군구, 지표명) T034
ON T000.sido_nm = T034.시도
AND T000.sgg_nm = T034.시군구
LEFT OUTER JOIN
	(SELECT  시도, 시군구, 지표명, IFNULL(avg(지자체_2021),0) AS highway_IC_acb
	FROM 	ods.NABIS_교통 
	WHERE 	지표명 = '고속도로 IC접근성'
	GROUP BY 시도, 시군구, 지표명) T035
ON T000.sido_nm = T035.시도
AND T000.sgg_nm = T035.시군구
LEFT OUTER JOIN 
	(SELECT  시도, 시군구, 지표명, IFNULL(avg(지자체_2021),0) AS rail_acb
	FROM 	ods.NABIS_교통 
	WHERE 	지표명 = '고속·고속화철도 접근성'
	GROUP BY 시도, 시군구, 지표명) T036
ON T000.sido_nm = T036.시도
AND T000.sgg_nm = T036.시군구
LEFT OUTER JOIN 
	(SELECT  시도, 시군구, 지표명, IFNULL(avg(지자체_2021),0) AS park_pop_rat
	FROM 	ods.NABIS_교통 
	WHERE 	지표명 = '주차장 서비스권역(0.75Km) 내 인구비율'
	GROUP BY 시도, 시군구, 지표명) T037
ON T000.sido_nm = T037.시도
AND T000.sgg_nm = T037.시군구
;






