# IMPORT GENERIC MODULES
import sys
import logging
import ConfigParser
import os
import json

# IMPORT SPARK MODULES
from pyspark.sql.functions import  col, when, max, sum, min, count, round, lit, concat, date_add
import pyspark.sql.functions
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, SQLContext


def generate_scecom_data_for_hbase_hist_load():
    # Create log handler
    logger = logging.getLogger('Generate scecom data for Hbase History Load')
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    ################################
    #   Example:
    #    tf_mtr_rdng_srce_tbl = "core.tf_mtr_rdng_scecom_intermediate"
    #    td_serv_acct_tbl = "core.td_service_acct"
    #    td_serv_plan_tbl = "core.td_service_plan"
    #    tl_serv_acct_serv_pln_tbl = "core.tl_serv_acct_serv_pln"
    #    td_sce_season_tbl = "core.td_sce_season"
    #    td_calendar_tbl = "core.td_calendar"
    #    td_billing_day_part_tbl = "core.td_billing_day_part"
    #    interval_meas=15
    #    start_dt='2017-03-01'
    #    end_dt='2017-03-01'
    #    hdfs_output='/data/core/scecom_hbase_hist_bulk_load/'
    ###################################

    tf_mtr_rdng_srce_tbl = sys.argv[1]
    logger.info("Meter Reading source table is: {0}".format(tf_mtr_rdng_srce_tbl))

    td_serv_acct_tbl = sys.argv[2]
    logger.info("Service Account table is: {0}".format(td_serv_acct_tbl))

    td_serv_plan_tbl = sys.argv[3]
    logger.info("Service Plan table is: {0}".format(td_serv_plan_tbl))

    tl_serv_acct_serv_pln_tbl = sys.argv[4]
    logger.info("Service Account Serv plan table is: {0}".format(tl_serv_acct_serv_pln_tbl))

    td_sce_season_tbl = sys.argv[5]
    logger.info("SCE SEASON table is: {0}".format(td_sce_season_tbl))

    td_calendar_tbl = sys.argv[6]
    logger.info("SCE CALENDAR table is: {0}".format(td_calendar_tbl))

    td_billing_day_part_tbl = sys.argv[7]
    logger.info("Billing DAY PART table is: {0}".format(td_billing_day_part_tbl))

    interval_meas = int(sys.argv[8])
    logger.info("Interval measure is: {0}".format(interval_meas))

    start_dt = sys.argv[9]
    logger.info("Start Date is: {0}".format(start_dt))

    end_dt = sys.argv[10]
    logger.info("End Date is: {0}".format(end_dt))

    hdfs_output = sys.argv[11]
    logger.info("HDFS OUTPUT PATH is: {0}".format(hdfs_output))

    # Initialize Spark Conf
    conf = SparkConf().setAppName('PySpark')
    sc = SparkContext(conf=conf)
    hiveContext = HiveContext(sc)
    sqlContext = SQLContext(sc)

    # Populate the Meter Reading Dataframe
    logger.info("Loading tf_mtr_rdng_df")

    tf_mtr_rdng_df = hiveContext.sql("SELECT cast(serv_pnt_id as string) sp, \
        cast(serv_acct_id as string) sa, \
        CASE WHEN ((periodcount % 23) = 0 or (periodcount % 24) = 0 or (periodcount % 25) = 0) THEN 'complete' ELSE 'partial' END rsc, \
        cast(interval_measure as string) im, \
        cast(rdng_meas as string) rm, \
        cast(rdng_chnl_num as string) chnl, \
        regexp_replace(pcfc_day_per_dt,'-','') as rd, \
        pcfc_day_per_dt pfd, \
        rdng_dttm, \
        CASE WHEN interval_measure=15 THEN regexp_replace(substr(pcfc_qtr_hr_per_strt_dttm,12,5),':','') \
		     WHEN interval_measure=60 THEN regexp_replace(substr(pcfc_hr_per_strt_dttm,12,5),':','') END as qtr_intr, \
        substr(pcfc_hr_per_strt_dttm,12,2) as hr_intr, \
        substr(pcfc_qtr_hr_per_strt_dttm,12,10) as qtr_intr_10, \
        cast(periodcount as string) pc, \
        CASE WHEN upper(trim(unit_of_meas_cd)) = 'KVARH' THEN 'kv' \
             WHEN (upper(trim(unit_of_meas_cd)) = 'KWH_DEL') or (upper(trim(unit_of_meas_cd)) = 'KWH') THEN 'kd' \
             WHEN upper(trim(unit_of_meas_cd)) = 'KWH_NET' THEN 'kn' \
             WHEN upper(trim(unit_of_meas_cd)) = 'KWH_REC' THEN 'kr' END uom, \
        CASE WHEN status = 'Estimated' THEN 'ESTIMATED' ELSE 'ACTUAL' END AS st, \
        outage_ind oi, if (interval_measure = 15,'4','1') as demand_factor \
    FROM {0} where pcfc_day_per_dt between '{1}' and '{2}' and interval_measure={3} and rdng_chnl_num IN (1101, 1102, 1103, 9104) order by serv_pnt_id,qtr_intr,unit_of_meas_cd".format(
        tf_mtr_rdng_srce_tbl, start_dt, end_dt, interval_meas))

    logger.info("Finished loading tf_mtr_rdng_df")

    # Populate the Service Account dataframe
    logger.info("Loading td_serv_act_df")

    td_serv_act_df = hiveContext.sql("SELECT serv_acct_id,serv_plan_cd,serv_plan_id as plan_id \
        from {0} where serv_plan_cd != 'ESP'".format(td_serv_acct_tbl))

    logger.info("Finished loading td_serv_act_df")

    # Populate the Service plan dataframe
    logger.info("Loading td_serv_plan_df")

    td_serv_plan_df = hiveContext.sql("SELECT trim(seasn_use_type_cd) use_type_cd,serv_plan_id \
        from {0}".format(td_serv_plan_tbl))

    logger.info("Finished Loading td_serv_plan_df")

    # Populate the Service Account Service plan dataframe
    logger.info("Loading tl_serv_acct_serv_pln_df")

    tl_serv_acct_serv_pln_df = hiveContext.sql("SELECT serv_acct_id acct_id,serv_plan_assign_strt_dt,serv_plan_assign_end_dt \
        from {0}".format(tl_serv_acct_serv_pln_tbl))

    logger.info("Finished loading tl_serv_acct_serv_pln_df")

    # Populate the SCE Season dataframe
    logger.info("Loading td_sce_season_df")

    td_sce_season_df = hiveContext.sql("SELECT trim(season_use_type_cd) season_use_type_cd,season_strt_dt,season_end_dt,trim(season_cd) as si \
        from {0}".format(td_sce_season_tbl))

    logger.info("Finished Loading td_sce_season_df")

    # Populate the Calendar dataframe
    logger.info("Loading td_calendar_df")

    td_calendar_df = hiveContext.sql("SELECT calendar_dt,trim(weekend_sce_holdy_ind) weekend_sce_holdy_ind \
        from {0} where to_date(calendar_dt) between '{1}' and '{2}'".format(td_calendar_tbl, start_dt, end_dt))

    logger.info("Finished loading td_calendar_df")

    # Populate the Billing Day Part Dataframe
    logger.info("Loading td_billing_day_part_df")

    td_billing_day_part_df = hiveContext.sql("SELECT COALESCE(TRIM(billg_daypart_type_cd),'') as pi, serv_plan_id as bill_day_part_plan_id,substring(to_utc_timestamp(billg_daypart_strt_pcfc_tm,'America/Los_Angeles'),12,10) billg_daypart_strt_pcfc_tm,substring(to_utc_timestamp(billg_daypart_end_pcfc_tm,'America/Los_Angeles'),12,10) billg_daypart_end_pcfc_tm,trim(billg_daypart_day_type_cd) billg_daypart_day_type_cd,trim(Seasn_cd) Seasn_cd \
    from {0} where TRIM(billg_daypart_type_cd) IN ('MID','OFF', 'ON', 'SOF') AND TRIM(serv_meas_type_cd)  IN ('KWHSFP', 'KWHSNP', 'KWHSMP', 'KWHWFP', 'KWHWNP', 'KWHWMP', 'KWHSSP','KWHWSP') AND to_date(current_timestamp()) >= billg_daypart_strt_dt AND to_date(current_timestamp()) <= date_sub(billg_daypart_end_dt, 1)".format(
        td_billing_day_part_tbl))

    logger.info("Finished Loading td_billing_day_part_df")

    # Join Meter Reading FACT with multiple dimension tables
    # This Dataframe which will be the base dataframe for the following use cases
    # READING MEASURE (0000)
    # DAILY SUM   (ds)
    # HOURLY SUM (hs)
    # DAILY MAX  (dm)
    # PEAK INDICATOR (pi)
    # STATUS INDICATOR (st)
    # Outage indicator (oi)
    # Season Indicator (si)

    logger.info("Generating joined_mtr_rdng_df")
    joined_mtr_rdng_df = tf_mtr_rdng_df.join(td_serv_act_df, tf_mtr_rdng_df.sa == td_serv_act_df.serv_acct_id, 'inner') \
        .join(td_serv_plan_df, td_serv_plan_df.serv_plan_id == td_serv_act_df.plan_id, 'inner') \
        .join(tl_serv_acct_serv_pln_df, ((tl_serv_acct_serv_pln_df.acct_id == td_serv_act_df.serv_acct_id) \
                                         & (tf_mtr_rdng_df.pfd >= tl_serv_acct_serv_pln_df.serv_plan_assign_strt_dt) \
                                         & (tf_mtr_rdng_df.pfd < tl_serv_acct_serv_pln_df.serv_plan_assign_end_dt)),
              'inner') \
        .join(td_sce_season_df, ((td_sce_season_df.season_use_type_cd == td_serv_plan_df.use_type_cd) \
                                 & (tf_mtr_rdng_df.pfd >= td_sce_season_df.season_strt_dt) \
                                 & (tf_mtr_rdng_df.pfd <= td_sce_season_df.season_end_dt)), 'inner') \
        .join(td_calendar_df, ((td_calendar_df.calendar_dt >= td_sce_season_df.season_strt_dt) \
                               & (td_calendar_df.calendar_dt <= td_sce_season_df.season_end_dt)), 'inner') \
        .join(td_billing_day_part_df, ((td_billing_day_part_df.bill_day_part_plan_id == td_serv_plan_df.serv_plan_id) \
                                       & (
                                                   tf_mtr_rdng_df.qtr_intr_10 >= td_billing_day_part_df.billg_daypart_strt_pcfc_tm) \
                                       & (
                                                   tf_mtr_rdng_df.qtr_intr_10 <= td_billing_day_part_df.billg_daypart_end_pcfc_tm) \
                                       & (
                                                   td_calendar_df.weekend_sce_holdy_ind == td_billing_day_part_df.billg_daypart_day_type_cd) \
                                       & (td_sce_season_df.si == td_billing_day_part_df.Seasn_cd)), 'left_outer')

    logger.info("Finished generating joined_mtr_rdng_df")
    logger.info("Pesist joined_mtr_rdng_df to memory")

    joined_mtr_rdng_df.persist()

    logger.info("Is joined_mtr_rdng_df cached? {0}".format(joined_mtr_rdng_df.is_cached))

    #######################################################################
    ################## READING MEASURE START ##############################
    # QTR Interval List
    qtr_intr_lst = sorted(joined_mtr_rdng_df.select("qtr_intr").distinct().map(lambda row: row[0]).collect())

    logger.info("Is joined_mtr_rdng_df cached? {0}".format(joined_mtr_rdng_df.is_cached))
    logger.info("Interval List is {0}".format(qtr_intr_lst))

    # Reading Measure List
    rdng_meas_15_cols = [when(col("qtr_intr") == m, col("rm")).otherwise(None).alias(m) for m in qtr_intr_lst]

    # max value for reading measures
    maxs = [max(col(m)).alias(m) for m in qtr_intr_lst]

    # Calculate the Reading measure for the 96 minute intervals (FOR INTEVAL QUERY)
    df_transposed_rdng_meas_15_min_mtr_rdng = (joined_mtr_rdng_df
                                               .select(col("sa"), col("sp"), col("im"), col("uom"), col("pc"),
                                                       col("pfd"), col("rd"), col("si"), col("rsc"), *rdng_meas_15_cols)
                                               .groupBy("sa", "sp", "im", "uom", "pc", "pfd", "rd", "si", "rsc")
                                               .agg(*maxs)
                                               .na.fill(0)
                                               .withColumn("rec_type", lit("rdng_meas")))

    logger.info("Is joined_mtr_rdng_df cached? {0}".format(joined_mtr_rdng_df.is_cached))
    logger.info("Finished generating df_transposed_rdng_meas_15_min_mtr_rdng")

    #######################################################################
    ################## READING MEASURE END ##############################

    ##################################################################
    ################## HOURLY SUM START ##############################

    # Calculate HOURLY SUM TMP (FOR INTEVAL QUERY)
    df_transposed_rdng_meas_hourly_sum_tmp = joined_mtr_rdng_df.groupBy(col("sa"), col("sp"), col("uom"), col("rd"),
                                                                        col("hr_intr"), col("im")) \
        .agg(round(sum(col("rm")), 4).cast("string") \
             .alias("hs")) \
        .orderBy(col("sa"), col("sp"), col("rd"), col("uom"), col("hr_intr"))

    logger.info("Finished generating df_transposed_rdng_meas_hourly_sum_tmp")

    # HOUR Interval List
    hr_intr_lst = sorted(
        df_transposed_rdng_meas_hourly_sum_tmp.select("hr_intr").distinct().map(lambda row: row[0]).collect())
    logger.info("Hourly interval List is {0}".format(hr_intr_lst))

    # Hourly Sum Column List
    hrly_sum_15_cols = [when(col("hr_intr") == n, col("hs")).otherwise(None).alias(n) for n in hr_intr_lst]

    # max value for hourly sum
    maxs_hr = [max(col(n)).alias(n) for n in hr_intr_lst]

    # Transpose the Hourly Sum
    df_transposed_rdng_meas_hourly_sum = (df_transposed_rdng_meas_hourly_sum_tmp
                                          .select(col("sa"), col("sp"), col("uom"), col("rd"), col("im"),
                                                  *hrly_sum_15_cols)
                                          .groupBy("sa", "sp", "uom", "rd", "im")
                                          .agg(*maxs_hr)
                                          .na.fill(0)
                                          .withColumn("rec_type", lit("hrly_sum")))

    logger.info("Finished generating df_transposed_rdng_meas_hourly_sum")

    ##################################################################
    ################## HOURLY SUM END ##############################

    ##################################################################
    ################## DAILY SUM START ##############################

    # Calculate Daily Sum (FOR USAGE QUERY -DAILY)
    df_transposed_rdng_meas_daily_sum = joined_mtr_rdng_df \
        .groupBy(col("sa"), col("sp"), col("rd"), col("im"), col("uom")) \
        .agg(round(sum(col("rm")), 4).cast("string").alias("ds")) \
        .orderBy(col("sa"), col("sp"), col("rd"), col("uom")) \
        .withColumn("rec_type", lit("daily_sum"))

    logger.info("Finished generating df_transposed_rdng_meas_daily_sum")

    ##################################################################
    ################## DAILY SUM END #################################

    ##################################################################
    ################## DAILY MAX START ###############################
    # Calculate DAILY MAX(FOR USAGE QUERY - DAILY)
    df_transposed_rdng_meas_daily_max = joined_mtr_rdng_df \
        .where(joined_mtr_rdng_df.uom == 'kd') \
        .groupBy(col("sa"), col("sp"), col("rd"), col("pfd"), col("im"), col("demand_factor")) \
        .agg((max(col("rm").cast("double")) * col("demand_factor")).cast("string").alias("dmkd")) \
        .orderBy(col("sa"), col("sp"), col("rd"), col("pfd")) \
        .withColumn("rec_type", lit("daily_max")) \
        .withColumn("mstkd", concat(col("pfd"), lit(" 00:00:00"))) \
        .withColumn("metkd", concat(date_add(col("pfd"), 1), lit(" 00:00:00")))

    logger.info("Finished generating df_transposed_rdng_meas_daily_max")

    ##################################################################
    ################## DAILY MAX END ###############################

    #######################################################################
    ################## PEAK INDICATOR START ##############################
    # PEAK INDICATOR List
    peak_indicator_cols = [when(col("qtr_intr") == o, col("pi")).otherwise(None).alias(o)
                           for o in qtr_intr_lst]

    # max value for peak indicator
    maxs_peak = [max(col(o)).alias(o) for o in qtr_intr_lst]

    # Calculate the Peak INDICATOR for the 96 minute intervals (FOR INTERVAL AND USAGE QUERY)
    df_transposed_peak_indicator = (joined_mtr_rdng_df
                                    .select(col("sa"), col("sp"), col("rd"), col("im"), *peak_indicator_cols)
                                    .groupBy("sa", "sp", "rd", "im")
                                    .agg(*maxs_peak)
                                    .na.fill(0)
                                    .withColumn("rec_type", lit("peak_ind")))

    logger.info("Finished generating df_transposed_peak_indicator")

    #######################################################################
    ################## PEAK INDICATOR END ##############################

    #######################################################################
    ################## OUTAGE INDICATOR START ##############################
    # OUTAGE INDICATOR LIST
    outage_indicator_cols = [when(col("qtr_intr") == p, col("oi")).otherwise(None).alias(p)
                             for p in qtr_intr_lst]

    # MAX VALUE FOR OUTAGE INDICATOR
    maxs_outage = [max(col(p)).alias(p) for p in qtr_intr_lst]

    # Calculate the OUTAGE INDICATOR for the 96 minute intervals (FOR INTERVAL AND USAGE QUERY)
    df_transposed_outage_indicator = (joined_mtr_rdng_df
                                      .select(col("sa"), col("sp"), col("rd"), col("im"), *outage_indicator_cols)
                                      .groupBy("sa", "sp", "rd", "im")
                                      .agg(*maxs_outage)
                                      .na.fill(0)
                                      .withColumn("rec_type", lit("outage_ind")))

    logger.info("Finished generating df_transposed_outage_indicator")

    #######################################################################
    ################## OUTAGE INDICATOR END ##############################

    #######################################################################
    ################## STATUS START ######################################
    # STATUS LIST
    status_cols = [when(col("qtr_intr") == q, col("st")).otherwise(None).alias(q)
                   for q in qtr_intr_lst]

    # MAX VALUE FOR STATUS
    maxs_status = [max(col(q)).alias(q) for q in qtr_intr_lst]

    # Calculate the Status for the 96 minute intervals (FOR INTEVAL QUERY)
    df_transposed_status_indicator = (joined_mtr_rdng_df
                                      .select(col("sa"), col("sp"), col("uom"), col("rd"), col("im"), *status_cols)
                                      .groupBy("sa", "sp", "uom", "rd", "im")
                                      .agg(*maxs_status)
                                      .na.fill(0)
                                      .withColumn("rec_type", lit("status")))

    logger.info("Finished generating df_transposed_status_indicator")

    #######################################################################
    ################## STATUS END #########################################

    #######################################################################
    ################# WRITE DATAFRAME OUTPUTS #############################

    # 1. READING MEASURE OUTPUT
    df_transposed_rdng_meas_15_min_mtr_rdng.coalesce(10).write.format('json').mode('overwrite').save(
        hdfs_output + "rec_type=rdng_meas/" + "dt={0}/im={1}/".format(start_dt, str(interval_meas)))

    # 2. HOURLY SUM OUTPUT
    df_transposed_rdng_meas_hourly_sum.coalesce(5).write.format('json').mode('overwrite').save(
        hdfs_output + "rec_type=hrly_sum/" + "dt={0}/im={1}/".format(start_dt, str(interval_meas)))

    # 3. DAILY SUM OUTPUT
    df_transposed_rdng_meas_daily_sum.coalesce(2).write.format('json').mode('overwrite').save(
        hdfs_output + "rec_type=daily_sum/" + "dt={0}/im={1}/".format(start_dt, str(interval_meas)))

    # 4. DAILY MAX OUTPUT
    df_transposed_rdng_meas_daily_max.coalesce(2).write.format('json').mode('overwrite').save(
        hdfs_output + "rec_type=daily_max/" + "dt={0}/im={1}/".format(start_dt, str(interval_meas)))

    # 5. PEAK INDICATOR OUTPUT
    df_transposed_peak_indicator.coalesce(2).write.format('json').mode('overwrite').save(
        hdfs_output + "rec_type=peak_ind/" + "dt={0}/im={1}/".format(start_dt, str(interval_meas)))

    # 6. OUTAGE INDICATOR OUTPUT
    df_transposed_outage_indicator.coalesce(4).write.format('json').mode('overwrite').save(
        hdfs_output + "rec_type=outage_ind/" + "dt={0}/im={1}/".format(start_dt, str(interval_meas)))

    # 7. STATUS OUTPUT
    df_transposed_status_indicator.coalesce(6).write.format('json').mode('overwrite').save(
        hdfs_output + "rec_type=status/" + "dt={0}/im={1}/".format(start_dt, str(interval_meas)))

    ##### CLEAR CACHE #######################
    joined_mtr_rdng_df.unpersist()
    logger.info("Finished Unpersisting joined meter reading")
    logger.info("Is joined_mtr_rdng_df cached? {0}".format(joined_mtr_rdng_df.is_cached))


if __name__ == "__main__":
    if len(sys.argv) != 12:
        raise Exception('Incorrect number of arguments passed')

    print
    'Number of arguments:', len(sys.argv), 'arguments.'
    print
    'Argument List:', str(sys.argv)
    generate_scecom_data_for_hbase_hist_load()
