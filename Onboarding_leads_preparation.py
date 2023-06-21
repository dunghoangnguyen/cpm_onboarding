# Databricks notebook source
# DBTITLE 1,Step0a - Declare params for lead month
agt_scorecard_dt = "2023-03-31" #Agent Scorecard: latest available monthend_dt of vn_published_analytics_db.agent_scorecard
cseg_mthend_dt   = "2023-03"    #Customer Life Stage Segment: latest available monthend_dt of vn_published_analytics_db.cust_lifestage
needs_mthend_dt  = "2023-03"    #Needs Model: monthend_dt = campaign launch date - 1 mth, of vn_lab_project_customer_needs_model_db.needs_model
campaign_launch  = "2023-04-01" #Tentative launch date, to be used to exclude leads from other ongoing campaigns by time of launch
onb_mth_end      = "2023-02-28" #ONB target month =  campaign launch date - 2 months


# COMMAND ----------

# DBTITLE 1,Step0b - Prepare working tables from LS_ADLS containers
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql.types import NullType
from pyspark.sql.functions import countDistinct

tcoverages = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TCOVERAGES/')
tpolicys = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TPOLICYS/')
tclient_policy_links  = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TCLIENT_POLICY_LINKS/')
tams_agents = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/TAMS_AGENTS/')
tams_candidates = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/TAMS_CANDIDATES/')
loc_code_mapping = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/LOC_CODE_MAPPING/')
loc_to_sm_mapping = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_REPORTS_DB/LOC_TO_SM_MAPPING/')
agent_scorecard = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/AGENT_SCORECARD/')
tmis_stru_grp_sm = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TMIS_STRU_GRP_SM/')
tcustdm_daily = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/TCUSTDM_DAILY/')
tpolidm_daily = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/TPOLIDM_DAILY/')
tagtdm_daily = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/TAGTDM_DAILY/')
cust_lifestage = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/CUST_LIFESTAGE/')
tclient_details = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TCLIENT_DETAILS/')
targetm_dm = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/TARGETM_DM/')
vn_existing_customer_needs_model = spark.read.format("orc").option("recursiveFileLookup","True").load(f'abfss://wandisco@abcmfcadovnedl01psea.dfs.core.windows.net/asia/vn/lab/project/campaign_module/hive/vn_existing_customer_needs_model/monthend_dt={needs_mthend_dt}')

# Convert HIVE tables
Row_list=[]
Row_list = vn_existing_customer_needs_model.toPandas()['row']
vn_existing_customer_needs_model = spark.createDataFrame(Row_list,[])

# Compute the unique count of po_num of the latest existing needs model
result_existing_df = vn_existing_customer_needs_model.agg(countDistinct("cli_num").alias("unique_cli_num_count"))

#Display the result
result_existing_df.show()

# COMMAND ----------

# Set the ABFSS path to the delta/partitioned table
onboarding_ci_invest_path = "abfss://wandisco@abcmfcadovnedl01psea.dfs.core.windows.net/asia/vn/lab/project/customer_needs_model/hive/onboarding_ci_invest/"

# Create a Spark session
spark = SparkSession.builder.appName("List Delta Parts").getOrCreate()

# Create a DBUtils object
dbutils = DBUtils(spark)

# List the directories under the delta/partitioned table path
directories_new = dbutils.fs.ls(onboarding_ci_invest_path)

# Store the directory names in a list
onboarding_ci_invest_files = [onboarding_ci_invest_path+directory.name for directory in directories_new]

onboarding_ci_invest = None
for file in onboarding_ci_invest_files:
    temp_df = spark.read.format("orc").load(file)
    non_null_columns = [c for c in temp_df.columns if temp_df.schema[c].dataType != NullType()]
    temp_df = temp_df.select(*non_null_columns)
    if len(temp_df.columns) > 7: #add a check to filter out partition with less than 8 columns
        if onboarding_ci_invest is None:
            onboarding_ci_invest = temp_df
        else:
            onboarding_ci_invest = onboarding_ci_invest.union(temp_df)

# Compute the unique count of po_num by month
result_new_df = onboarding_ci_invest.groupBy("month").agg(countDistinct("po_num").alias("unique_po_num_count"))

# Display the result
result_new_df.show()

# COMMAND ----------

# DBTITLE 1,Step1 - Capture all Agency in-force policies onboarded during the target month
tcoverages.createOrReplaceTempView("cvg")
tpolicys.createOrReplaceTempView("pol")
tclient_policy_links.createOrReplaceTempView("po_lnk")
tclient_policy_links.createOrReplaceTempView("ins_lnk")
tams_agents.createOrReplaceTempView("wa")
tams_agents.createOrReplaceTempView("sa")

onb_actv_cvg = sqlContext.sql(f"""
SELECT
    cvg.pol_num
   ,po_lnk.cli_num                                      AS po_num
   ,ins_lnk.cli_num                                     AS ins_num
   ,cvg.plan_code
   ,cvg.vers_num
   ,cvg.cvg_stat_cd
   ,cvg.cvg_typ
   ,cvg.cvg_reasn
   ,CAST(cvg.cvg_prem AS INT)
   ,pol.pmt_mode
   ,cvg.cvg_prem*12/pol.pmt_mode                        AS inforce_APE
   ,to_date(cvg.cvg_iss_dt)                             AS cvg_iss_dt
   ,to_date(pol.pol_iss_dt)                             AS pol_iss_dt
   ,DENSE_RANK() OVER (
        PARTITION BY
            po_lnk.cli_num
        ORDER BY
            pol.pol_iss_dt
           ,pol.pol_num
    )                                                   AS purchase_order_base
   ,pol.wa_cd_1                                         AS wagt_cd
   ,wa.comp_prvd_num                                    AS wagt_comp_prvd_num
   ,pol.agt_code                                        AS sagt_cd
   ,sa.comp_prvd_num                                    AS sagt_comp_prvd_num
   ,CASE
        WHEN sa.trmn_dt IS NOT NULL
            AND sa.comp_prvd_num IN ('01','04', '97', '98') THEN 'Orphaned'
        WHEN sa.comp_prvd_num = '01'
            AND (
                pol.agt_code = pol.wa_cd_1
                OR pol.agt_code = pol.wa_cd_2
                )                                           THEN 'Original Agent'
        WHEN sa.comp_prvd_num = '01'                        THEN 'Reassigned Agent'
        WHEN sa.comp_prvd_num = '04'                        THEN 'Orphaned-Collector'
        WHEN sa.comp_prvd_num IN ('97', '98')               THEN 'Orphaned-Agent is SM'
        ELSE 'Unknown'
    END                                                 AS cus_agt_rltnshp
   ,CONCAT(
        SUBSTR('{onb_mth_end}',1,4)
       ,SUBSTR('{onb_mth_end}',6,2))                     AS onb_mth_yr
FROM cvg INNER JOIN 
    pol ON cvg.pol_num = pol.pol_num INNER JOIN 
    po_lnk ON pol.pol_num = po_lnk.pol_num AND po_lnk.link_typ = 'O' INNER JOIN 
    ins_lnk ON pol.pol_num = ins_lnk.pol_num AND ins_lnk.link_typ = 'I' INNER JOIN 
    wa ON pol.wa_cd_1 = wa.agt_code INNER JOIN 
    sa ON pol.agt_code = sa.agt_code
WHERE YEAR(pol.pol_iss_dt) = YEAR('{onb_mth_end}')
    AND MONTH(pol.pol_iss_dt) = MONTH('{onb_mth_end}')
    AND pol.pol_stat_cd IN ('1','2','3','5')
    AND wa.comp_prvd_num IN ('01','97','98')
    AND sa.comp_prvd_num IN ('01','04','97','98')
""")

display(onb_actv_cvg)

# COMMAND ----------

# DBTITLE 1,Step2 - Get Onboarding customers' in-force coverages from inception until onboarding target month.
#This is for calculation of customer total APE holdings, and Near-VIP/VIP status
onb_actv_cvg.createOrReplaceTempView("onb_actv_cvg")

onb_allcvg = sqlContext.sql(f"""
SELECT
    cvg.pol_num
   ,po_lnk.cli_num                                      AS po_num
   ,cvg.plan_code
   ,cvg.vers_num
   ,cvg.cvg_stat_cd
   ,cvg.cvg_typ
   ,cvg.cvg_reasn
   ,cvg.cvg_prem
   ,pol.pmt_mode
   ,cvg.cvg_prem*12/pmt_mode                            AS inforce_APE
   ,cvg.cvg_iss_dt
   ,pol.pol_iss_dt
   ,FLOOR(
        DATEDIFF('{onb_mth_end}', cvg.cvg_iss_dt)/365.25
    )                                                   AS inforce_year
   ,DENSE_RANK() OVER(
        PARTITION BY
            po_lnk.cli_num
        ORDER BY
            pol.pol_iss_dt
    )                                                   AS purchase_order_base
   ,pol.wa_cd_1                                         AS wagt_cd
   ,wa.comp_prvd_num                                    AS wagt_comp_prvd_num
   ,pol.agt_code                                        AS sagt_cd
   ,sa.comp_prvd_num                                    AS sagt_comp_prvd_num
   ,CONCAT(
        SUBSTR('{onb_mth_end}',1,4)
       ,SUBSTR('{onb_mth_end}',6,2)
    )                                                   AS onb_mth_yr
FROM cvg INNER JOIN 
     pol ON cvg.pol_num = pol.pol_num INNER JOIN 
     po_lnk ON pol.pol_num = po_lnk.pol_num AND po_lnk.link_typ = 'O' INNER JOIN 
     wa ON pol.wa_cd_1 = wa.agt_code INNER JOIN 
     sa ON pol.agt_code = sa.agt_code
WHERE pol.pol_iss_dt <= '{onb_mth_end}'
    AND pol.pol_stat_cd in ('1','2','3','5')
    AND wa.comp_prvd_num in ('01','97','98')
    AND sa.comp_prvd_num in ('01','04','97','98')
    AND po_lnk.cli_num IN(
        SELECT DISTINCT
            po_num
        FROM onb_actv_cvg
        WHERE onb_mth_yr = CONCAT(SUBSTR('{onb_mth_end}',1,4),SUBSTR('{onb_mth_end}',6,2))
    )
""")

display(onb_allcvg)

# COMMAND ----------

# DBTITLE 1,Step3 - Get Agent/Collector details
tams_candidates.createOrReplaceTempView("can")
agent_scorecard.createOrReplaceTempView("sc")
loc_code_mapping.createOrReplaceTempView("loc")
loc_to_sm_mapping.createOrReplaceTempView("rh")
tmis_stru_grp_sm.createOrReplaceTempView("stru")

onb_agt_hist = sqlContext.sql(f"""
SELECT DISTINCT
    sa.agt_code
   ,sa.agt_nm
   ,to_date(sa.cntrct_eff_dt)       AS cntrct_eff_dt
   ,to_date(sa.trmn_dt)             AS trmn_dt
   ,CASE
        WHEN sa.stat_cd = '01' THEN 'Active'
        WHEN sa.stat_cd = '99' THEN 'Terminated'
        ELSE 'Unknown'
    END                             AS agt_status
   ,sa.comp_prvd_num
   ,sa.loc_code
   ,loc.city
   ,loc.region
   ,sa.mobl_phon_num
   ,can.email_addr
   ,can.id_num                                  AS agt_id_num
   ,smgr.loc_mrg                                AS mgr_cd
   ,smgr.mgr_nm
   ,IF(LOWER(sc.agent_tier) = 'mdrt', 'Y', 'N') AS mdrt_ind
   ,IF(LOWER(sc.agent_tier) = 'fc', 'Y', 'N')   AS fc_ind
   ,IF(LOWER(sc.agent_tier) = '1maa', 'Y', 'N') AS 1maa_ind
   ,IF(LOWER(sc.agent_tier) = '3maa', 'Y', 'N') AS 3maa_ind
   ,sc.agent_tier
   ,sc.agent_cluster
   ,rh.rh_name AS agent_rh
   ,CONCAT(
        SUBSTR('{onb_mth_end}',1,4)
       ,SUBSTR('{onb_mth_end}',6,2)
    )                                           AS onb_mth_yr
FROM sa LEFT JOIN 
     can ON can.can_num = sa.can_num LEFT JOIN 
     loc ON loc.loc_code = sa.loc_code LEFT JOIN 
     rh ON rh.loc_cd = sa.loc_code LEFT JOIN 
     sc ON sc.agt_code = sa.agt_code
        AND sc.monthend_dt = '{agt_scorecard_dt}'
LEFT JOIN (
    SELECT DISTINCT
        stru.loc_code
       ,stru.loc_mrg
       ,sa.agt_nm AS mgr_nm
    FROM stru INNER JOIN 
         sa ON stru.loc_mrg = sa.agt_code
    WHERE stru.loc_code IS NOT NULL
) smgr ON smgr.loc_code = sa.loc_code
WHERE sa.comp_prvd_num IN ('01', '04', '97', '98')
""")

display(onb_agt_hist)

# COMMAND ----------

# DBTITLE 1,Step 4 - Get client details
onb_allcvg.createOrReplaceTempView("onb_allcvg")
tcustdm_daily.createOrReplaceTempView("tcli")
cust_lifestage.createOrReplaceTempView("cseg")

onb_cli_dtl = sqlContext.sql(f"""
SELECT DISTINCT
    tcli.cli_num                                         AS po_num
   ,tcli.cli_nm                                          AS po_name
   ,tcli.id_num                                          AS po_id_num
   ,tcli.sex_code                                        AS po_sex_code
   ,to_date(tcli.birth_dt)                               AS po_birth_dt
   ,tcli.cur_age                                         AS po_age_curr --age as of time of analysis
   ,CASE
        WHEN tcli.cur_age < 18               THEN '1. <18'
        WHEN tcli.cur_age BETWEEN 18 AND 25  THEN '2. 18-25'
        WHEN tcli.cur_age BETWEEN 26 AND 35  THEN '3. 26-35'
        WHEN tcli.cur_age BETWEEN 36 AND 45  THEN '4. 36-45'
        WHEN tcli.cur_age BETWEEN 46 AND 55  THEN '5. 46-55'
        WHEN tcli.cur_age BETWEEN 56 AND 65  THEN '6. 56-65'
        WHEN tcli.cur_age > 65               THEN '7. >65'
        ELSE 'Unknown'
    END                                                 AS po_age_curr_grp
   ,tcli.prim_phon_num                                   AS po_prim_phon_num
   ,tcli.mobl_phon_num                                   AS po_mobl_phon_num
   ,tcli.othr_phon_num                                   AS po_othr_phon_num
   ,IF(tcli.mobl_phon_num IS NOT NULL, 'Y', 'N')         AS mobile_ind
   ,tcli.sms_ind                                         AS opt_sms_ind
   ,tcli.email_addr                                      AS po_email_addr
   ,IF(tcli.email_addr IS NOT NULL, 'Y', 'N')            AS email_ind
   ,tcli.sms_eservice_ind                                AS opt_email_ind
   ,tcli.full_address                                    AS po_full_addr
   ,tcli.city                                            AS customer_city
   ,CASE
        WHEN tcli.city = 'Hà Nội'            THEN 'Hà Nội'
        WHEN tcli.city = 'Hồ Chí Minh'       THEN 'Hồ Chí Minh'
        WHEN tcli.city IS NULL               THEN 'Unknown'
        ELSE 'Others'
    END                                                 AS cust_city_cat
   ,NVL(cseg.customer_segment, 'Undefined Segmentation') AS lifestage_segment
   ,tcli.no_dpnd                                         AS max_no_dpnd
   ,tcli.occp_code
   ,tcli.mthly_incm
   ,CASE
        WHEN tcli.mthly_incm > 0
            AND tcli.mthly_incm < 20000      THEN '1) < 20M'
        WHEN tcli.mthly_incm >= 20000
            AND tcli.mthly_incm < 40000      THEN '2) 20-40M'
        WHEN tcli.mthly_incm >= 40000
            AND tcli.mthly_incm < 60000      THEN '3) 40-60M'
        WHEN tcli.mthly_incm >= 60000
            AND tcli.mthly_incm < 80000      THEN '4) 60-80M'
        WHEN tcli.mthly_incm >= 80000        THEN '5) 80M+'
        ELSE '6) Unknown'
    END                                                 AS mthly_incm_cat
   ,tcli.mthly_incm*12                                   AS annual_incm
   ,to_date(iss.frst_join_dt)                           AS frst_join_dt
   ,IF(
        iss.frst_join_dt BETWEEN ADD_MONTHS(DATE_ADD('{onb_mth_end}', 1 - DAY('{onb_mth_end}')),-6) AND '{onb_mth_end}'
       ,'New'
       ,'Existing'
    )                                                   AS cus_type
   ,hld.pol_cnt
   ,hld.10yr_pol_cnt
   ,hld.total_APE
   ,CASE
         WHEN hld.pol_cnt = 1               THEN '1'
         WHEN hld.pol_cnt = 2               THEN '2'
         WHEN hld.pol_cnt BETWEEN 3 AND 5   THEN '3-5'
         WHEN hld.pol_cnt > 5               THEN '>5'
         ELSE 'NA'
     END                                                AS pol_cnt_cat
   ,CASE
        WHEN hld.total_APE <  20000         THEN '1) < 20M'
        WHEN hld.total_APE >= 20000
            AND hld.total_APE < 40000       THEN '2) 20-40M'
        WHEN hld.total_APE >= 40000
            AND hld.total_APE < 60000       THEN '3) 40-60M'
        WHEN hld.total_APE >= 60000
            AND hld.total_APE < 80000       THEN '4) 60-80M'
        WHEN hld.total_APE >= 80000         THEN '5) 80M+'
        ELSE '6) Unknown'
    END                                                 AS total_APE_cat
   ,CASE
        WHEN hld.total_APE >= 20000
            AND hld.total_APE < 65000
            AND hld.10yr_pol_cnt >= 1       THEN 'Silver'
        WHEN hld.total_APE >= 65000
            AND hld.total_APE < 150000      THEN 'Gold'
        WHEN hld.total_APE >= 150000
            AND hld.total_APE < 300000      THEN 'Platinum'
        WHEN hld.total_APE >= 300000        THEN 'Platinum Elite'
        ELSE 'Not VIP'
    END                                                 AS VIP_cat
   ,CASE
        WHEN hld.total_APE >= 15000
            AND hld.total_APE < 20000
            AND hld.10yr_pol_cnt >= 1       THEN 'Near Silver'
        WHEN hld.total_APE >= 55000
            AND hld.total_APE < 60000       THEN 'Near Gold1'
        WHEN hld.total_APE >= 60000
            AND hld.total_APE < 65000       THEN 'Near Gold2'
        ELSE 'Not Near VIP'
    END                                                 AS near_VIP_cat
   ,CAST(NULL AS STRING)                                AS hml_propensity_grp
   ,CAST(NULL AS STRING)                                AS no_high_prop
   ,CAST(NULL AS STRING)                                AS hl_propensity_grp
   ,CAST(NULL AS STRING)                                AS hml_acc_med
   ,CAST(NULL AS STRING)                                AS hml_inv
   ,CAST(NULL AS STRING)                                AS hml_term
   ,CAST(NULL AS STRING)                                AS hml_ul
   ,CAST(NULL AS STRING)                                AS 1h_high_prop
   ,onb.onb_mth_yr
FROM onb_actv_cvg onb LEFT JOIN 
     tcli ON onb.po_num = tcli.cli_num LEFT JOIN 
     cseg ON onb.po_num = cseg.client_number AND cseg.monthend_dt = '{cseg_mthend_dt}'
LEFT JOIN (
    SELECT
        po_lnk.cli_num
       ,MIN(pol.frst_iss_dt)
       ,MIN(pol.pol_iss_dt)
       ,MIN(pol.pol_eff_dt)
       ,COALESCE(
            MIN(pol.frst_iss_dt)
           ,MIN(pol.pol_iss_dt)
           ,MIN(pol.pol_eff_dt)
        )                                               AS frst_join_dt
    FROM pol INNER JOIN 
         po_lnk ON pol.pol_num = po_lnk.pol_num AND po_lnk.link_typ = 'O'
    WHERE pol.pol_stat_cd IN ('1', '2', '3', '5', '7', '9', 'A', 'B', 'D', 'E', 'F', 'H', 'L', 'M', 'T', 'X')
    GROUP BY
        po_lnk.cli_num
) iss ON onb.po_num = iss.cli_num 
LEFT JOIN (
    SELECT
        po_num
       ,SUM(inforce_APE)                                    AS total_APE
       ,COUNT(DISTINCT pol_num)                             AS pol_cnt
       ,COUNT(DISTINCT IF(inforce_year>=10,pol_num,NULL))   AS 10yr_pol_cnt
    FROM onb_allcvg
    WHERE onb_mth_yr = CONCAT(YEAR('{onb_mth_end}'),LPAD(MONTH('{onb_mth_end}'),2,0))
    GROUP BY po_num
) hld ON onb.po_num = hld.po_num
WHERE onb.onb_mth_yr = CONCAT(YEAR('{onb_mth_end}'),LPAD(MONTH('{onb_mth_end}'),2,0))
    AND tcli.cli_typ = 'Individual' --exclude corporate customers
""")

display(onb_cli_dtl)

# COMMAND ----------

# DBTITLE 1,Step5 - Get IDs of the policy owner, insured, other insured, beneficiary.
tclient_details.createOrReplaceTempView("dtl")

#This will be used to identify if policy's servicing agent is also owner/insured/dependent
onb_dpdnt_ids = sqlContext.sql(f"""
SELECT
    po_lnk.pol_num
   ,COLLECT_SET(dtl.id_num) AS dpdnt_id_list
   ,onb.onb_mth_yr
FROM onb_actv_cvg onb INNER JOIN 
    po_lnk ON onb.pol_num = po_lnk.pol_num INNER JOIN 
    dtl ON po_lnk.cli_num = dtl.cli_num
WHERE onb.onb_mth_yr = CONCAT(YEAR('{onb_mth_end}'),LPAD(MONTH('{onb_mth_end}'),2,0))
    AND po_lnk.link_typ IN ('O', 'I', 'T', 'B') --O: Owner, I: Insured, T: Other Insured, B: Beneficiary
GROUP BY
    po_lnk.pol_num
   ,onb.onb_mth_yr
""")

display(onb_dpdnt_ids)

# COMMAND ----------

# DBTITLE 1,Step6 - Combine Onboarding customer info, policy info, and servicing agent info
onb_cli_dtl.createOrReplaceTempView("onb_cli_dtl")
onb_agt_hist.createOrReplaceTempView("onb_agt_hist")
onb_dpdnt_ids.createOrReplaceTempView("onb_dpdnt_ids")
tpolidm_daily.createOrReplaceTempView("tpol")
tagtdm_daily.createOrReplaceTempView("tagt")
targetm_dm.createOrReplaceTempView("targetm_dm")

onb_cli_pol = sqlContext.sql(f"""
SELECT DISTINCT
    cli.po_num
   ,cli.po_name
   ,cli.po_id_num
   ,cli.po_sex_code
   ,cli.po_birth_dt
   ,cli.po_age_curr
   ,cli.po_age_curr_grp
   ,cli.po_prim_phon_num
   ,cli.po_mobl_phon_num
   ,cli.po_othr_phon_num
   ,cli.mobile_ind
   ,cli.opt_sms_ind
   ,cli.po_email_addr
   ,cli.email_ind
   ,cli.opt_email_ind
   ,cli.po_full_addr
   ,cli.customer_city
   ,cli.cust_city_cat
   ,cli.lifestage_segment
   ,cli.max_no_dpnd
   ,cli.occp_code
   ,cli.mthly_incm
   ,cli.mthly_incm_cat
   ,cli.annual_incm
   ,cli.frst_join_dt
   ,cli.cus_type
   ,cli.pol_cnt
   ,cli.10yr_pol_cnt
   ,cli.total_APE
   ,cli.pol_cnt_cat
   ,cli.total_APE_cat
   ,cli.VIP_cat
   ,cli.near_VIP_cat
   ,CASE
        WHEN cli.near_VIP_cat <> 'Not Near VIP'
            AND cli.VIP_cat <> 'Silver'             THEN 'Near VIP' --If both Silver and Near Gold1/Gold2 then classified as Silver
        WHEN cli.VIP_cat <> 'Not VIP'               THEN 'VIP'
        WHEN cli.cus_type = 'New'
            AND cli.mthly_incm >= 20000             THEN 'New'
        ELSE 'Others'
    END                                                 AS lead_segment
   ,cli.hml_propensity_grp
   ,cli.hl_propensity_grp
   ,cli.no_high_prop
   ,cli.hml_acc_med
   ,cli.hml_inv
   ,cli.hml_term
   ,cli.hml_ul
   ,cli.1h_high_prop
   ,IF(tgt.tgt_cust_id IS NULL, 'N', 'Y')               AS in_ONB_po --if owner is present in other ONB batches
   ,IF(ins.tgt_insrd_id IS NULL, 'N', 'Y')              AS in_ONB_ins --if insured is present as lead in other ONB batches
   ,CASE
        WHEN tgt.tgt_cust_id IS NULL
            AND ins.tgt_insrd_id IS NULL    THEN 'N'
        ELSE 'Y'
    END                                                 AS in_ONB --if either owner or insured is present as lead in other ONB batches
   ,IF(bnc.po_num IS NULL, 'N', 'Y')                    AS in_banca --if owner is contacted in Banca
   ,IF(dmtm.po_num IS NULL, 'N', 'Y')                   AS in_dmtm --if owner is contacted in DMTM
   ,IF(ARRAY_CONTAINS(dpd.dpdnt_id_list,CAST(agt.agt_id_num AS VARCHAR(20))), 'Y', 'N')
                                                        AS sagt_rltv --if customer is related to agent
   ,IF(rep.last_pol_iss <= onb.pol_iss_dt, 'N', 'Y')     AS repurchased --if customer repurchased 0-2 months from onboarding
   ,IF(onb.cus_agt_rltnshp IN ('Original Agent', 'Reassigned Agent'), 'N', 'Y')
                                                        AS unassigned
   ,onb.pol_num
   ,onb.plan_code
   ,onb.vers_num
   ,onb.ins_num                                         AS insrd_num
   ,onb.inforce_APE                                     AS base_APE
   ,onb.pol_iss_dt
   ,onb.sagt_cd
   ,agt.agt_nm                                          AS sagt_name
   ,agt.agt_status                                      AS sagt_status
   ,agt.mobl_phon_num                                   AS sagt_mobl_phon
   ,agt.email_addr                                      AS sagt_email_addr
   ,agt.agt_id_num                                      AS sagt_id_num
   ,agt.comp_prvd_num                                   AS sagt_comp_prvd_num
   ,agt.city                                            AS sagt_city
   ,agt.loc_code                                        AS sagt_loc_code
   ,agt.region                                          AS sagt_region
   ,agt.mgr_cd                                          AS sagt_mgr_cd
   ,agt.mgr_nm                                          AS sagt_mgr_nm
   ,onb.cus_agt_rltnshp
   ,agt.agent_tier                                      AS sagt_tier
   ,agt.agent_cluster                                   AS sagt_cluster
   ,agt.agent_rh                                        AS sagt_rh
   ,onb.onb_mth_yr
FROM onb_actv_cvg onb INNER JOIN 
     onb_cli_dtl cli ON onb.po_num = cli.po_num AND onb.onb_mth_yr = cli.onb_mth_yr INNER JOIN 
     onb_agt_hist agt ON onb.sagt_cd = agt.agt_code AND onb.onb_mth_yr = agt.onb_mth_yr
LEFT JOIN (
    SELECT DISTINCT
        t.tgt_cust_id AS tgt_cust_id
    FROM targetm_dm t
    WHERE t.batch_start_date > LAST_DAY(ADD_MONTHS(current_date,-4))	-- Amend to exclude leads distributed past 3 months
)tgt ON onb.po_num = tgt.tgt_cust_id  --exclude leads from past campaigns
LEFT JOIN (
    SELECT DISTINCT
        i.cli_num AS tgt_insrd_id
    FROM targetm_dm t INNER JOIN 
         po_lnk i ON t.tgt_cust_id = i.pol_num AND i.link_typ = 'I'
    WHERE t.batch_start_date > LAST_DAY(ADD_MONTHS(current_date,-4))	-- Amend to exclude leads distributed past 3 months
)ins ON onb.po_num  = ins.tgt_insrd_id --exclude insureds from past campaigns
LEFT JOIN (
    SELECT DISTINCT
        tpol.po_num
    FROM tpol LEFT JOIN 
         tagt cwa ON tpol.wa_code = cwa.agt_code LEFT JOIN 
         tagt csa ON tpol.sa_code = csa.agt_code
    WHERE  cwa.channel = 'Banca'
        OR csa.channel = 'Banca'
)bnc ON onb.po_num = bnc.po_num --exclude owners that are joint Banca customers
LEFT JOIN (
    SELECT DISTINCT
        tpol.po_num
    FROM tpol LEFT JOIN 
         tagt cwa ON tpol.wa_code = cwa.agt_code LEFT JOIN 
         tagt csa ON tpol.sa_code = csa.agt_code
    WHERE  cwa.loc_cd LIKE 'DMO%'
        OR cwa.loc_cd LIKE 'FPC%'
        OR csa.loc_cd LIKE 'DMO%'
        OR csa.loc_cd LIKE 'FPC%'
)dmtm ON onb.po_num = dmtm.po_num --exclude owners that are joint DMTM customers
LEFT JOIN (
    SELECT
        po_lnk.cli_num      AS po_num
       ,MAX(pol.pol_iss_dt) AS last_pol_iss
    FROM pol INNER JOIN 
        po_lnk ON pol.pol_num = po_lnk.pol_num AND po_lnk.link_typ = 'O' INNER JOIN 
        wa ON pol.wa_cd_1 = wa.agt_code INNER JOIN 
        sa ON pol.agt_code = sa.agt_code
    WHERE pol.pol_iss_dt BETWEEN DATE_ADD('{onb_mth_end}', 1 - DAY('{onb_mth_end}')) AND ADD_MONTHS('{onb_mth_end}', 2)
        AND pol.pol_stat_cd IN ('1', '2', '3', '5', '7', '9', 'A', 'B', 'D', 'E', 'F', 'H', 'L', 'M', 'T', 'X')
        AND wa.comp_prvd_num IN ('01','97','98')
        AND sa.comp_prvd_num IN ('01','04','97','98')
    GROUP BY
        po_lnk.cli_num
)rep ON onb.po_num = rep.po_num --check for any repurchase on month 0-2, then exclude
LEFT JOIN onb_dpdnt_ids dpd ON onb.pol_num = dpd.pol_num AND onb.onb_mth_yr = dpd.onb_mth_yr --exclude policies whose servicing agent = policy owner/insured/dependent
WHERE onb.onb_mth_yr = CONCAT(YEAR('{onb_mth_end}'),LPAD(MONTH('{onb_mth_end}'),2,0))
    AND onb.purchase_order_base = 1
    AND onb.cvg_typ = 'B'
    AND onb.cvg_reasn = 'O'
""")

display(onb_cli_pol)

# COMMAND ----------

# DBTITLE 1,Step7 - Apply exclusions tagging
onb_cli_pol.createOrReplaceTempView("onb_cli_pol")

onb_cli_final = sqlContext.sql(f"""
SELECT
    po_num
   ,po_name
   ,po_id_num
   ,po_sex_code
   ,po_birth_dt
   ,po_age_curr
   ,po_age_curr_grp
   ,po_prim_phon_num
   ,po_mobl_phon_num
   ,po_othr_phon_num
   ,mobile_ind
   ,opt_sms_ind
   ,po_email_addr
   ,email_ind
   ,opt_email_ind
   ,po_full_addr
   ,customer_city
   ,cust_city_cat
   ,lifestage_segment
   ,max_no_dpnd
   ,occp_code
   ,mthly_incm
   ,mthly_incm_cat
   ,annual_incm
   ,frst_join_dt
   ,cus_type
   ,pol_cnt
   ,10yr_pol_cnt
   ,total_ape
   ,pol_cnt_cat
   ,total_ape_cat
   ,vip_cat
   ,near_vip_cat
   ,lead_segment
   ,hml_propensity_grp
   ,hl_propensity_grp
   ,no_high_prop
   ,hml_acc_med
   ,hml_inv
   ,hml_term
   ,hml_ul
   ,1h_high_prop
   ,in_onb_po
   ,in_onb_ins
   ,in_onb
   ,in_banca
   ,in_dmtm
   ,sagt_rltv
   ,repurchased
   ,unassigned
   ,pol_num
   ,plan_code
   ,vers_num
   ,insrd_num
   ,base_ape
   ,pol_iss_dt
   ,sagt_cd
   ,sagt_name
   ,sagt_status
   ,sagt_mobl_phon
   ,sagt_email_addr
   ,sagt_id_num
   ,sagt_comp_prvd_num
   ,sagt_city
   ,sagt_loc_code
   ,sagt_region
   ,sagt_mgr_cd
   ,sagt_mgr_nm
   ,cus_agt_rltnshp
   ,sagt_tier
   ,sagt_cluster
   ,sagt_rh
   ,CASE
        WHEN in_banca = 'Y'
            OR in_dmtm = 'Y'
            OR sagt_rltv = 'Y'
            OR in_onb = 'Y'
            OR repurchased = 'Y'
            OR unassigned = 'Y'
            THEN 'Y'
        ELSE 'N'
    END                                                 AS excluded
   ,CASE
        WHEN in_banca = 'Y'
            THEN '1) Joint Banca'
        WHEN in_dmtm = 'Y'
            THEN '2) Joint DMTM'
        WHEN sagt_rltv = 'Y'
            THEN '3) Agent is the Owner/Insured/Beneficiary'
        WHEN in_onb = 'Y'
            THEN '4) Past Campaign Customer'
        WHEN repurchased = 'Y'
            THEN '5) Repurchased 0-2 months from Onboarding'
        WHEN unassigned = 'Y'
            THEN '6) Unassigned'
        ELSE 'Not Excluded'
    END AS excluded_grp
   ,onb_mth_yr
FROM onb_cli_pol
WHERE onb_mth_yr = CONCAT(YEAR('{onb_mth_end}'),LPAD(MONTH('{onb_mth_end}'),2,0))
""")

display(onb_cli_final)

# COMMAND ----------

# DBTITLE 1,Step8 - Score ONB leads
onb_cli_final.createOrReplaceTempView("onb_cli_final")
vn_existing_customer_needs_model.createOrReplaceTempView("need")
onboarding_ci_invest.createOrReplaceTempView("new_need")

onb_cli_scoring = sqlContext.sql(f"""
SELECT
    onb.po_num
   ,onb.pol_num
   ,onb.sagt_cd
   ,onb.cus_type
   ,onb.near_vip_cat
   ,onb.vip_cat
   ,CASE
        WHEN onb.near_VIP_cat <> 'Not Near VIP'
            AND onb.VIP_cat <> 'Silver' THEN 'Near VIP' --If both Silver and Near Gold1/Gold2 then classified as Silver
        WHEN onb.VIP_cat <> 'Not VIP'   THEN 'VIP'
        ELSE 'Others'
    END                                             AS lead_segment
   ,CASE
        WHEN onb.near_VIP_cat <> 'Not Near VIP'
            AND onb.VIP_cat <> 'Silver' THEN 'Near VIP' --If both Silver and Near Gold1/Gold2 then classified as Silver
        WHEN onb.VIP_cat <> 'Not VIP'   THEN 'VIP'
        WHEN onb.cus_type = 'Existing'
            AND LEAST(need.decile_ci, need.decile_inv, need.decile_lt, need.decile_lp,need.decile_med) BETWEEN 1 AND 2 --high propensity based from the existing customers model (top 2 decile)
                THEN 'High Propensity'
        WHEN onb.cus_type = 'New'
			AND LEAST(new_need.ci_decile, new_need.invst_decile, new_need.lt_decile) BETWEEN 1 AND 3 --high propensity based from the new customers model (top 3 decile)
                THEN 'New'
        --Adding another lead segment if any of the decile is null
        WHEN need.cli_num IS NOT NULL
            AND (   
                need.decile_ci IS NULL
                OR need.decile_inv IS NULL
                OR need.decile_lt IS NULL
                OR need.decile_lp IS NULL
                OR need.decile_med IS NULL
            ) THEN 'Unknown'
        WHEN new_need.PO_NUM IS NOT NULL
            AND (
                new_need.ci_decile IS NULL  
                OR new_need.invst_decile IS NULL 
                OR new_need.lt_decile IS NULL 
            ) THEN 'Unknown'
        ELSE 'Others'
    END                                             AS final_lead_segment
   ,need.decile_acc
   ,need.decile_ci
   ,need.decile_med
   ,need.decile_inv
   ,need.decile_lp   
   ,need.decile_lt
   ,need.hml_acc
   ,need.hml_ci
   ,need.hml_med
   ,need.hml_inv
   ,need.hml_lp
   ,need.hml_lt
   ,CAST(NULL AS INT)                                AS offer_acc  --new customer Acc&Med: does not exist yet
   ,IF(new_need.ci_decile BETWEEN 1 AND 3, 1, 0)     AS offer_ci   --new customer CI: high propensity if Decile 1-3
   ,CAST(NULL AS INT)                                AS offer_edu  --new customer Educ: does not exist yet
   ,IF(new_need.invst_decile BETWEEN 1 AND 3, 1, 0)  AS offer_inv  --new customer Inv: high propensity if Decile 1-3
   ,CAST(NULL AS INT)                                AS offer_term --new customer Term: does not exist yet
   ,CAST(NULL AS INT)                                AS offer_ul   --new customer UL: does not exist yet
   ,CASE
        WHEN LEAST(need.decile_ci, need.decile_inv, need.decile_lt, need.decile_lp,need.decile_med) BETWEEN 1 AND 2
        	AND need.cli_num is not NULL
            THEN 1
        ELSE 0
    END                                             AS top2_HP_model --high propensity based from the existing customers model (top 2 decile)
   ,CASE
        WHEN LEAST(need.decile_ci, need.decile_inv, need.decile_lt, need.decile_lp,need.decile_med) BETWEEN 1 AND 3
        	AND need.cli_num is not NULL
            THEN 1
        ELSE 0
    END                                             AS HP_model --high propensity based from the existing customers model (top 3 decile)
    ,CASE
        WHEN LEAST(new_need.ci_decile, new_need.invst_decile, new_need.lt_decile) BETWEEN 1 AND 3
        	AND new_need.po_num is not NULL
           THEN 1
        ELSE 0
     END                                             AS HP_rules   --high propensity based from the new customers model (top 3 decile)
   ,IF(new_need.lt_decile BETWEEN 1 AND 3, 1, 0)     AS offer_lt   --new customer Inv: high propensity if Decile 1-3	
   ,onb.onb_mth_yr
FROM onb_cli_final onb LEFT JOIN 
     need ON onb.po_num = need.cli_num LEFT JOIN 
     new_need ON onb.po_num = new_need.po_num AND onb.onb_mth_yr = CONCAT(SUBSTR(new_need.month,1,4),SUBSTR(new_need.month,6,2))
WHERE onb.onb_mth_yr = CONCAT(YEAR('{onb_mth_end}'),LPAD(MONTH('{onb_mth_end}'),2,0))
""")

display(onb_cli_scoring)

# COMMAND ----------

# DBTITLE 1,Step9 - Final table
onb_cli_scoring.createOrReplaceTempView("onb_cli_scoring")

onb_leads = sqlContext.sql(f"""
SELECT
    onb.po_num
   ,onb.po_name
   ,onb.po_id_num
   ,onb.po_sex_code
   ,onb.po_birth_dt
   ,onb.po_age_curr
   ,onb.po_age_curr_grp
   ,onb.po_prim_phon_num
   ,onb.po_mobl_phon_num
   ,onb.po_othr_phon_num
   ,onb.mobile_ind
   ,onb.opt_sms_ind
   ,onb.po_email_addr
   ,onb.email_ind
   ,onb.opt_email_ind
   ,onb.po_full_addr
   ,onb.customer_city
   ,onb.cust_city_cat
   ,onb.lifestage_segment
   ,onb.max_no_dpnd
   ,onb.occp_code
   ,onb.mthly_incm
   ,onb.mthly_incm_cat
   ,onb.annual_incm
   ,onb.frst_join_dt
   ,onb.cus_type
   ,onb.pol_cnt
   ,onb.10yr_pol_cnt
   ,onb.total_ape
   ,onb.pol_cnt_cat
   ,onb.total_ape_cat
   ,onb.vip_cat
   ,onb.near_vip_cat
   ,CASE
        WHEN onb.lead_segment IN ('VIP', 'Near VIP')
            THEN onb.lead_segment
        WHEN sco.cus_type = 'Existing'
			AND sco.top2_HP_model = 1
            THEN 'High Propensity'
        WHEN sco.cus_type = 'New'
			AND sco.hp_rules = 1
            THEN 'New'
        --adding the unknown group
        WHEN sco.cus_type = 'Existing'
            AND sco.top2_HP_model IS NULL 
            THEN 'Unknown'
        WHEN sco.cus_type = 'New'
			AND sco.hp_rules IS NULL 
            THEN 'Unknown'
        ELSE 'Others'
    END                                 AS lead_segment
   ,CAST(NULL AS STRING)                AS hml_propensity_grp
   ,CAST(NULL AS STRING)                AS hl_propensity_grp
   ,CAST(NULL AS STRING)                AS no_high_prop
   ,sco.hml_acc
   ,sco.hml_ci
   ,sco.hml_med
   ,sco.hml_inv
   ,sco.hml_lt
   ,sco.hml_lp
   ,CAST(NULL AS STRING)                AS 1h_high_prop
   ,IF(sco.top2_HP_model = 1, 'Y', 'N') AS high_prop_model
   ,IF(sco.hp_rules = 1, 'Y', 'N')      AS high_prop_rules
   ,onb.in_onb_po
   ,onb.in_onb_ins
   ,onb.in_onb
   ,onb.in_banca
   ,onb.in_dmtm
   ,onb.sagt_rltv
   ,onb.repurchased
   ,onb.unassigned
   ,onb.pol_num
   ,onb.plan_code
   ,onb.vers_num
   ,onb.insrd_num
   ,onb.base_ape
   ,onb.pol_iss_dt
   ,onb.sagt_cd
   ,onb.sagt_name
   ,onb.sagt_status
   ,onb.sagt_mobl_phon
   ,onb.sagt_email_addr
   ,onb.sagt_id_num
   ,onb.sagt_comp_prvd_num
   ,onb.sagt_city
   ,onb.sagt_loc_code
   ,onb.sagt_region
   ,onb.sagt_mgr_cd
   ,onb.sagt_mgr_nm
   ,onb.cus_agt_rltnshp
   ,onb.sagt_tier
   ,onb.sagt_cluster
   ,onb.sagt_rh
   ,onb.excluded
   ,onb.excluded_grp
   ,onb.onb_mth_yr
FROM ONB_cli_final onb LEFT JOIN 
    ONB_cli_scoring sco
    ON onb.po_num = sco.po_num
        AND onb.pol_num = sco.pol_num
        AND onb.onb_mth_yr = sco.onb_mth_yr
WHERE onb.onb_mth_yr = CONCAT(YEAR('{onb_mth_end}'),LPAD(MONTH('{onb_mth_end}'),2,0))
""")

print(onb_leads.count())

# COMMAND ----------

# DBTITLE 1,Step10 - Write to file
onb_leads.write.mode("overwrite").parquet("abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/ONB_leads")

# COMMAND ----------


