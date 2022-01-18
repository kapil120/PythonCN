import os
import sys
import yaml
import time
import datetime
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from coretechs_data_engineering.utils import get_logger
logger = get_logger(__name__)

def get_id_ref(spark, input_path, filedate):
    mem_df = spark.read.option('mergeSchema', True).csv(input_path+filedate+'/CareFirst_Member_'+'*txt*.pgp', header='true',sep='|')
    logger.info('Distinct Medicaid_IDs in member data: {0}'.format(mem_df.select('RECIP_NO').distinct().count()))
    id_map = mem_df.select(f.col('RECIP_NO').alias('MedicaidID'),'MEM_NO').dropDuplicates()
    logger.info('{0} MedicaidIDs mapped to MEM_NOs'.format(id_map.count()))
    return id_map

def get_claims_by_condition(df, condition, col_dict, filename, out_path):
    logger.info('Processing {0} claims'.format(filename))
    start_time = time.time()
    PARTITION_FIELDS = ['updated_at', 'source_id', 'source_month']
    name_dict = {'file_name':'source_id', 'file_date':'source_date', 'file_month': 'source_month'}
    name_dict.update(col_dict)
    clm_df = df.where(condition).select([f.col(source_name).alias(sd_name) for source_name,sd_name in name_dict.items()])
    clm_df = clm_df.withColumn('updated_at', f.date_format(f.lit(datetime.datetime.utcnow()), format='yyyyMMddhhmmss'))
    clm_df = clm_df.repartition(*PARTITION_FIELDS)
    logger.info(clm_df.explain())
    clm_df.write.mode('append').parquet(out_path+filename+'.parquet',partitionBy=PARTITION_FIELDS)
    logger.info('Records count: {0}'.format(clm_df.count()))
    logger.info('Completed {0} processing in {1}'.format(filename, time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))))

def preprocess_medical_claim(df, filedate, in_path, out_path, disposition_data_path=None):
    logger.info('Working on MedicalClaim file')
    # suffix medical claim_id with M to distinguish beacon and medical claims
    df = df.withColumn('ct_DOCUMENT',f.concat(f.trim(f.col('DOCUMENT')),f.lit('M')))
    # get reversal claims
    rev_condition = f.col('DOCUMENT').rlike('^R')
    get_claims_by_condition(df, rev_condition, {'ct_DOCUMENT':'ct_claim_id'}, 'reversal_claim', out_path)
    # filter out reversal claims for claims sds
    df = df.where(~rev_condition)
    logger.info('Record count after filtering reversal claims: {0}'.format(df.count()))
    # get denial claims
    den_condition = f.col('DENY_FLAG')=='1'
    get_claims_by_condition(df, den_condition, {'ct_DOCUMENT':'ct_claim_id', 'DENY_FLAG': 'claim_status'}, 'denial_claim', out_path)
    # filter out denial claims for claims sds
    temp_df =  df.filter(f.col('DENY_FLAG').isNull())
    df =  df.filter(~den_condition)
    logger.info('Record count after filtering denial claims: {0}'.format(df.count()))
    df =  df.union(temp_df)
    logger.info('Record count after adding deny flag null claims: {0}'.format(df.count()))
    # Disposition codes
    if disposition_data_path:
        logger.info('Reading disposition file: {0}'.format(disposition_data_path))
        disposition_df = spark.read.csv(disposition_data_path, header=True)
        df = df.join(disposition_df, 'DOCUMENT', how='left')
        logger.info('Record count after joining Disposition data (ext): {0}'.format(df.count()))
        df = df.withColumn('DISCHARGE_STATUS_CODE',f.when(f.col('DISPOSITION').isNotNull(),f.col('DISPOSITION')).otherwise(f.col('DispositionCode')))
    else:
        df = df.withColumn('DISCHARGE_STATUS_CODE',f.col('DISPOSITION'))
    # correct discharge dates
    logger.info('Working on correcting disposition dates in medical claims')
    claimline = os.path.join(in_path+filedate+'/CareFirst_MedicalClaimsDetail_*txt*.pgp')
    claimlinedf = spark.read.option('mergeSchema', True).csv(claimline, header='true',sep='|')
    claimline1df = claimlinedf
    inpatient_rev_codes = ['0100', '0101', '0110', '0111', '0112', '0113', '0114', '0116', '0117', '0118', '0119', '0120', '0121', '0122', '0123', '0124', '0126', '0127', '0128', '0129', '0130', '0131', '0132', '0133', '0134', '0136', '0137', '0138', '0139', '0140', '0141', '0142', '0143', '0144', '0146', '0147', '0148', '0149', '0150', '0151', '0152', '0153', '0154', '0156', '0157', '0158', '0159', '0160', '0164', '0167', '0169', '0170', '0171', '0172', '0173', '0174', '0179', '0190', '0191', '0192', '0193', '0194', '0199', '0200', '0201', '0202', '0203', '0204', '0206', '0207', '0208', '0209', '0210', '0211', '0212', '0213', '0214', '0219', '1000', '1001', '1002']
    claimlinedf = claimlinedf.where(f.col('REVENUE_CODE').isin(inpatient_rev_codes))
    claimlinedf = claimlinedf.select('DOCUMENT','CLAIM_LINE_NUMBER','THRU_DT')
    temp_df = df.where((f.col('VENDOR_TYPE')=='HOSP') &
                       (f.col('CLAIM_FORM')=='U') &
                       (f.col('CLM_TYPE').isin(['30R','INA','IPI','ITI'])) &
                       (f.col('DISCH_DT').isNull())
                      ).select('DOCUMENT')
    disch_df = temp_df.join(claimlinedf,'DOCUMENT', how='left')
    disch_df= disch_df.groupBy(f.col('DOCUMENT'),f.col('THRU_DT')).agg(f.count('THRU_DT').alias('count_of_thru_dt'))
    w = Window().partitionBy('DOCUMENT').orderBy(f.desc('count_of_thru_dt'),f.desc('THRU_DT'))
    disch_df= disch_df.withColumn('rank', f.dense_rank().over(w)).where(f.col('rank')==1).drop('rank').drop('count_of_thru_dt')
    df = df.join(disch_df, 'DOCUMENT', how='left')
    logger.info('Record count after joining Discharge date (Thru date from claim line): {0}'.format(df.count()))
    # Identify ED and inpatient events
    logger.info('Working on identifying ED and inpatient claims')
    inpatient_rev = ["0100","0101", "0102", "0103", "0104", "0105", "0106", "0107", "0108", "0109", "0110", "0111", "0112", "0113", "0114", "0115", "0116", "0117", "0118", "0119", "0120", "0121", "0122", "0123", "0124", "0125", "0126", "0127", "0128", "0129", "0130", "0131", "0132", "0133", "0134", "0135", "0136", "0137", "0138", "0139", "0140", "0141", "0142", "0143", "0144", "0145", "0146", "0147", "0148", "0149", "0150", "0151", "0152", "0153", "0154", "0155", "0156", "0157", "0158", "0159", "0160", "0161", "0162", "0163", "0164", "0165", "0166", "0167", "0168", "0169", "0170", "0171", "0172", "0173", "0174", "0175", "0176", "0177", "0178", "0179", "0200", "0201", "0202", "0203", "0204", "0205", "0206", "0207", "0208", "0209", "0210", "0211", "0212", "0213", "0214", "0215", "0216", "0217", "0218", "0219"]
    ED_rev = ["0450","0451","0452","0453","0454","0455","0456","0457","0458","0459","0981"]
    ED_Procedure = ["99281","99282","99283","99284","99285"]
    claimline1df = claimline1df.join(df,'DOCUMENT',how = 'left')
    claimline1df = claimline1df.withColumn("Claim_event",f.when((f.col('REVENUE_CODE') .isin(inpatient_rev)) |
                                                               ((f.col('TYPE_BILL')=='011') | (f.col('TYPE_BILL')=='012')), 'IP')
                                                          .when((f.col('REVENUE_CODE') .isin(ED_rev)) |
                                                                (f.col('LINE_CODE') .isin(ED_Procedure)) | (f.col('PL_CODE') == '23'), 'ED').otherwise(None))
    claimline1df = claimline1df.groupBy('DOCUMENT').agg(f.collect_set('Claim_event').alias('Claim_events'))
    claimline2df = claimline1df.withColumn('Final_claim_event',f.when(f.array_contains('Claim_events','IP'), 'IP') .when(f.array_contains('Claim_events', 'ED'), 'ED') .otherwise(None))
    claimline2df = claimline2df.select('DOCUMENT','Final_claim_event')
    df = df.join(claimline2df, 'DOCUMENT', how='left')
    logger.info('Medical Claims preprocessed')
    return df

def preprocess_medical_claim_line(df):
    logger.info('Working on MedicalClaimsDetail file')
    # suffix medical claim_id with M to distinguish beacon and medical claims
    df = df.withColumn('ct_DOCUMENT',f.concat(f.trim(f.col('DOCUMENT')),f.lit('M')))
    logger.info('Record count: {0}'.format(df.count()))
    logger.info('Medical Claims Detail preprocessed')
    return df

def preprocess_beacon_claim(df, out_path, transaction_data_path=None):
    logger.info('Working on BeaconClaims file')
    # suffix beacon claim id with B to distinguish beacon and medical claims
    df = df.withColumn('ct_ClaimID',f.concat(f.trim(f.col('ClaimID')),f.lit('B')))
    # get reversal claims
    rev_condition = f.col('ClaimID').rlike('^R')
    get_claims_by_condition(df, rev_condition, {'ct_ClaimID':'ct_claim_id'}, 'reversal_claim', out_path)
    # filter out reversal claims for claims sds
    df = df.where(~rev_condition)
    logger.info('Record count after filtering denial claims: {0}'.format(df.count()))
    # get denial claims
    den_condition = f.col('ClaimLineStatus')=='Denied'
    get_claims_by_condition(df, den_condition, {'ct_ClaimID':'ct_claim_id', 'ClaimLineStatus': 'claim_status'}, 'denial_claim', out_path)
    # filter out denial claims for claims sds
    temp_df =  df.filter(f.col('ClaimLineStatus').isNull())
    df =  df.filter(~den_condition)
    logger.info('Record count after filtering denial claims: {0}'.format(df.count()))
    df =  df.union(temp_df)
    logger.info('Record count after adding claim line status null claims: {0}'.format(df.count()))
    # Remove duplicate Beacon inpatient claims
    logger.info('Removing the duplicate Beacon inpatient claims')
    if transaction_data_path:
        logger.info('Reading the data from transaction file :{0}'.format(transaction_data_path))
        transaction_df = spark.read.csv(transaction_data_path, header='true').select('ClaimID','TransactionCode')
        df = df.join(transaction_df, 'ClaimID', how='left')
        #get the duplicate beacon inpatient claims
        dup_condition = f.col('TransactionCode')=='2'
        get_claims_by_condition(df, dup_condition, {'ct_ClaimID':'ct_claim_id'}, 'duplicate_beacon_claim', out_path)
        #filter out the duplicate beacon inpatient claims
        df = df.where(~dup_condition)
        logger.info('Record count after filtering duplicate beacon inpatient claims: {0}'.format(df.count()))

    # format bill type code
    logger.info('Formatting bill type code for beacon claims')
    df = df.withColumn('BILL_TYPE_CD',f.concat(f.col('BillType1'),f.col('BillType2'),f.col('BillType3')))
    df = df.withColumn('Discharge Date',
                        f.when((f.col('ClaimType') == 'I') & 
                                (f.col('PlaceOfServiceCode') == '21') & 
                                (f.col('Discharge Date') .isNull()),
                                f.col('ThruDate'))
                        .otherwise(f.col('Discharge Date')))
    # correct revenue code
    logger.info('Correcting revenue code for beacon claims')
    df = df.withColumn("RevenueCode",
                        f.when((f.col('ClaimType') == 'I') &
                                (f.col('PlaceOfServiceCode') == '21') &
                                ((f.col('RevenueCode') == '001') | (f.col("RevenueCode").isNull())),
                                f.col('ProcedureCode'))
                        .otherwise(f.col('RevenueCode')))
    # format revenue code to 4 digits
    df = df.withColumn("RevenueCode", f.when(f.length(f.col('RevenueCode')) < 4, f.lpad(f.col('RevenueCode'), 4, '0')).otherwise(f.col('RevenueCode')))
    # Identify ED and inpatient claims
    logger.info('Identifying ED and inpatient claims')
    inpatient_rev = ["0100","0101", "0102", "0103", "0104", "0105", "0106", "0107", "0108", "0109", "0110", "0111", "0112", "0113", "0114", "0115", "0116", "0117", "0118", "0119", "0120", "0121", "0122", "0123", "0124", "0125", "0126", "0127", "0128", "0129", "0130", "0131", "0132", "0133", "0134", "0135", "0136", "0137", "0138", "0139", "0140", "0141", "0142", "0143", "0144", "0145", "0146", "0147", "0148", "0149", "0150", "0151", "0152", "0153", "0154", "0155", "0156", "0157", "0158", "0159", "0160", "0161", "0162", "0163", "0164", "0165", "0166", "0167", "0168", "0169", "0170", "0171", "0172", "0173", "0174", "0175", "0176", "0177", "0178", "0179", "0200", "0201", "0202", "0203", "0204", "0205", "0206", "0207", "0208", "0209", "0210", "0211", "0212", "0213", "0214", "0215", "0216", "0217", "0218", "0219"]
    ED_rev = ["0450","0451","0452","0453","0454","0455","0456","0457","0458","0459","0981"]
    ED_Procedure = ["99281","99282","99283","99284","99285"]
    df = df.withColumn("Claim_event",f.when((f.col('RevenueCode') .isin(inpatient_rev)) |
                                           ((f.col('BILL_TYPE_CD') == '011') | (f.col('BILL_TYPE_CD')=='012')),'IP')
                                      .when((f.col('RevenueCode') .isin(ED_rev)) |
                                            (f.col('ProcedureCode') .isin(ED_Procedure)) | (f.col('PlaceOfServiceCode') == '23'), 'ED').otherwise(None))
    df1 = df.groupBy('ClaimID').agg(f.collect_set('Claim_event').alias('Claim_events'))
    df1 = df1.withColumn('Final_claim_event',f.when(f.array_contains('Claim_events','IP'), 'IP') .when(f.array_contains('Claim_events', 'ED'), 'ED') .otherwise(None))
    df1 = df1.select('ClaimID','Final_claim_event')
    df = df.join(df1, 'ClaimID', how='left')
    logger.info('Record count: {0}'.format(df.count()))
    logger.info('Beacon Claims preprocessed')
    return df

def preprocess_provider(in_path, filedate, df, id_map):
    logger.info('Working on Provider file')
    # map provider data to claim line level
    claimline = os.path.join(in_path+filedate+'/CareFirst_MedicalClaimsDetail_*txt*.pgp')
    claimlinedf = spark.read.option('mergeSchema', True).csv(claimline, header='true',sep='|')
    logger.info('Retreiving claim line info for providers')
    claimlinedf = claimlinedf.join(id_map, claimlinedf.MEMBER==id_map.MEM_NO, how='left')
    claimlinedf = claimlinedf.select('MEMBER','MedicaidID','DOCUMENT','CLAIM_LINE_NUMBER','PROVIDER')
    claimlinedf = claimlinedf.withColumn('ct_DOCUMENT',f.concat(f.trim(f.col('DOCUMENT')),f.lit('M')))
    df = df.join(claimlinedf, df.PROVIDER_ID==claimlinedf.PROVIDER , how='left')
    logger.info('Record count: {0}'.format(df.count()))
    logger.info('Providers preprocessed')
    return df

def preprocess_beacon_provider(in_path, filedate, df, id_map):
    logger.info('Working on BeaconProvider file')
    # map provider data to claim line level
    bclaim = os.path.join(in_path+filedate+'/CareFirst_BeaconClaims_*txt*.pgp')
    bclaimdf = spark.read.option('mergeSchema', True).csv(bclaim, header='true',sep='|')
    logger.info('Retreiving claim line info for beacon providers')
    bclaimdf = bclaimdf.join(id_map, 'MedicaidID', how='left')
    bclaimdf = bclaimdf.select('MedicaidID','ClaimID','ClaimLine','ProviderID','ClinicianNPI')
    bclaimdf = bclaimdf.withColumn('ct_ClaimID',f.concat(f.trim(f.col('ClaimID')),f.lit('B')))
    df = df.join(bclaimdf, df.NPINo==bclaimdf.ClinicianNPI , how='left')
    logger.info('Record count: {0}'.format(df.count()))
    logger.info('Beacon Providers preprocessed')
    return df

def preprocess(in_path, filedate, stg_path, id_map_path, out_path, disposition_data_path=None, transaction_path=None):
    logger.info('Preprocessing source data')

    dfs = {}
    preprocessors = {
        'Member': None,
        'MedicalClaim': 'preprocess_medical_claim(df, filedate, in_path, out_path, disposition_data_path)',
        'MedicalClaimsDetail': 'preprocess_medical_claim_line(df)',
        'BeaconClaims': 'preprocess_beacon_claim(df, out_path, transaction_path)',
        'Provider': 'preprocess_provider(in_path, filedate, df, id_map)',
        'BeaconProvider': 'preprocess_beacon_provider(in_path, filedate, df, id_map)'
    }

    # Read MedicaidID-MemberID map for each member
    id_map = spark.read.parquet(id_map_path)
    logger.info('ID Map Record count: {0}'.format(id_map.count()))
    
    # Read and preprocess source data files
    for name in preprocessors.keys():
        df_path = os.path.join(in_path+filedate+'/CareFirst_'+name+'_*txt*.pgp')
        df = spark.read.option('mergeSchema', True).csv(df_path, header='true',sep='|')

        file_start_time = time.time()
        df = df.withColumn('file_name', f.lit(df_path))
        df = df.withColumn('file_date', f.to_date(f.unix_timestamp(f.lit(filedate), 'yyyy-MM-dd').cast("timestamp")))
        df = df.withColumn('file_month', f.lit('-'.join(filedate.split('-')[:2])))

        logger.info('{0} source file record count: {1}'.format(name, df.count()))
        if 'MEMBER' in df.columns:
            logger.info('Adding Medicaid ID to {0} data'.format(name))
            df=df.join(id_map, df.MEMBER==id_map.MEM_NO, how='left')
         
        if name!='Member':
            logger.info('==========Preprocess {0}=========='.format(name))
            df = eval(preprocessors.get(name))

        dfs[name] = df
        logger.info('Completed {0} preprocessing in {1}'.format(name, time.strftime("%H:%M:%S", time.gmtime(time.time() - file_start_time))))
        logger.info('writing preprocessed {0} data to staging'.format(name))
        logger.info('Record count: {0}'.format(df.count()))
        df.write.csv(stg_path+filedate+'/preprocessed_'+name+'.csv_stage', header='true', mode='overwrite')

    return dfs

def standardize(dfs, in_path, out_path, filedate, error_path=None):
    logger.info('Working on standardizing data')
    from coretechs_data_engineering.ingest.master.standardize import create_sds

    # Read standard data structure
    logger.info('Reading sds config')
    with open(os.path.expanduser('~/gma-coretechs/carefirst-data-engineering/ingest/carefirst_sds.yml'), 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    # Read transformation logic
    with open(os.path.expanduser('~/gma-coretechs/carefirst-data-engineering/ingest/carefirst_transformation.yml'), 'r') as ymlfile:
        tcfg = yaml.load(ymlfile) or {}

        # Consolidate data, map spec, and transform spec for each piece
    sds_map = {}
    for name, field_map in cfg.items():
        # The mapping has names like MedicalClaim_claim, so we split that name into the filename part and the sds piece part
        file_name =  name.split('_')[0]
        sds_piece_name = '_'.join(name.split('_')[1:])

        # Check the transformation spec for a corresponding name
        transform_spec = tcfg.get(name)

        # Combine the df, the map spec, and the transform spec into a dictionary, and add it to the sds map dict
        full_spec = {'data': dfs[file_name], 'map_spec': field_map, 'transform_spec': transform_spec}
        if sds_piece_name in sds_map.keys():
            sds_map[sds_piece_name].append(full_spec)
        else:
            sds_map[sds_piece_name] = [full_spec]

    logger.info('Standardize process started')
    create_sds(sds_map, out_path, mode='append', error_path=error_path)
    logger.info('Standardization completed')

def filter_sds(all_path, latest_path, filedate):
    # reversals and denials removal from all sds
    logger.info('Working on Filtering SDS ')
    rev_df = spark.read.parquet(all_path+'reversal_claim.parquet').persist()
    act_df = rev_df.select(f.col('ct_claim_id').substr(f.lit(2),f.length(f.col('ct_claim_id'))).alias('ct_claim_id'))
    den_df = spark.read.parquet(all_path+'denial_claim.parquet').persist()
    duplicate_df = spark.read.parquet(all_path+'duplicate_beacon_claim.parquet').persist()

    #rev_df = rev_df.filter(f.col('source_date')==f.lit(filedate))
    #den_df = den_df.filter(f.col('source_date')==f.lit(filedate))
    #duplicate_df = duplicate_df.filter(f.col('source_date')==f.lit(filedate))


    logger.info('Record count_reversal claims: {0}'.format(rev_df.count()))
    logger.info('Record count_claims corresponding reversal claims: {0}'.format(act_df.count()))
    logger.info('Record count_denied claims: {0}'.format(den_df.count()))
    logger.info('Record count_duplicate claims: {0}'.format(duplicate_df.count()))
    
    PARTITION_FIELDS = ['updated_at', 'source_id', 'source_month']

    # remove all records of reversal claims and denials
    sds_list = ['claim']
    for sds in sds_list:
        logger.info('removing reversal and denial claim records from {0}'.format(sds))
        df = spark.read.parquet(all_path+sds+'.parquet').repartition('ct_claim_id').persist()
        logger.info('{0} sds initial record count: {1}'.format(sds,df.count()))
        df = df.join(rev_df,'ct_claim_id',how='leftanti')
        logger.info('Record count after filtering reversal claims: {0}'.format(df.count()))
        df = df.join(act_df,'ct_claim_id',how='leftanti')
        logger.info('Record count after filtering claims corresponding to reversal claims: {0}'.format(df.count()))
        df = df.join(den_df,'ct_claim_id',how='leftanti')
        logger.info('Record count after filtering denial claims: {0}'.format(df.count()))
        df = df.join(duplicate_df,'ct_claim_id',how='leftanti')
        logger.info('Record count after filtering duplicate Beacon inpatient claims: {0}'.format(df.count()))
        df_new = df.withColumn("row_number",f.row_number().over(Window.partitionBy(df.ct_claim_id).orderBy((df.source_id).desc()))).filter(f.col("row_number")==1).drop('row_number')
        df_new.repartition(*PARTITION_FIELDS).write.mode('overwrite').parquet(latest_path+sds+'.parquet',partitionBy=PARTITION_FIELDS)

    sds_list = ['claim_line']
    for sds in sds_list:
        logger.info('removing reversal and denial claim records from {0}'.format(sds))
        df = spark.read.parquet(all_path+sds+'.parquet').repartition('ct_claim_id').persist()
        logger.info('{0} sds initial record count: {1}'.format(sds,df.count()))
        df = df.join(rev_df,'ct_claim_id',how='leftanti')
        logger.info('Record count after filtering reversal claims: {0}'.format(df.count()))
        df = df.join(act_df,'ct_claim_id',how='leftanti')
        logger.info('Record count after filtering claims corresponding to reversal claims: {0}'.format(df.count()))
        df = df.join(den_df,'ct_claim_id',how='leftanti')
        logger.info('Record count after filtering denial claims: {0}'.format(df.count()))
        df = df.join(duplicate_df,'ct_claim_id',how='leftanti')
        logger.info('Record count after filtering duplicate Beacon inpatient claims: {0}'.format(df.count()))
        df_new = df.withColumn("row_number",f.row_number().over(Window.partitionBy(df.ct_claim_id,df.ct_claim_line_id).orderBy((df.source_id).desc()))).filter(f.col("row_number")==1).drop('row_number')
        df_new.repartition(*PARTITION_FIELDS).write.mode('overwrite').parquet(latest_path+sds+'.parquet',partitionBy=PARTITION_FIELDS)

    sds_list = ['provider']
    for sds in sds_list:
        logger.info('removing reversal and denial claim records from {0}'.format(sds))
        df = spark.read.parquet(all_path+sds+'.parquet').repartition('ct_claim_id').persist()
        logger.info('{0} sds initial record count: {1}'.format(sds,df.count()))
        df = df.join(rev_df,'ct_claim_id',how='leftanti')
        logger.info('Record count after filtering reversal claims: {0}'.format(df.count()))
        df = df.join(act_df,'ct_claim_id',how='leftanti')
        logger.info('Record count after filtering claims corresponding to reversal claims: {0}'.format(df.count()))
        df = df.join(den_df,'ct_claim_id',how='leftanti')
        logger.info('Record count after filtering denial claims: {0}'.format(df.count()))
        df = df.join(duplicate_df,'ct_claim_id',how='leftanti')
        logger.info('Record count after filtering duplicate Beacon inpatient claims: {0}'.format(df.count()))
        df_new = df.withColumn("row_number",f.row_number().over(Window.partitionBy(df.ct_claim_id,df.ct_claim_line_id,df.ct_provider_id,df.specialty,df.group_name).orderBy((df.source_id).desc()))).filter(f.col("row_number")==1).drop('row_number')
        df_new.repartition(*PARTITION_FIELDS).write.mode('overwrite').parquet(latest_path+sds+'.parquet',partitionBy=PARTITION_FIELDS)
   
    sds_list = ['procedure']
    for sds in sds_list:
        logger.info('removing reversal and denial claim records from {0}'.format(sds))
        df = spark.read.parquet(all_path+sds+'.parquet').repartition('ct_claim_id').persist()
        logger.info('{0} sds initial record count: {1}'.format(sds,df.count()))
        df = df.join(rev_df,'ct_claim_id',how='leftanti')
        logger.info('Record count after filtering reversal claims: {0}'.format(df.count()))
        df = df.join(act_df,'ct_claim_id',how='leftanti')
        logger.info('Record count after filtering claims corresponding to reversal claims: {0}'.format(df.count()))
        df = df.join(den_df,'ct_claim_id',how='leftanti')
        logger.info('Record count after filtering denial claims: {0}'.format(df.count()))
        df = df.join(duplicate_df,'ct_claim_id',how='leftanti')
        logger.info('Record count after filtering duplicate Beacon inpatient claims: {0}'.format(df.count()))
        df_new = df.withColumn("row_number",f.row_number().over(Window.partitionBy(df.ct_claim_id,df.ct_claim_line_id,df.code,df.code_system).orderBy((df.source_id).desc()))).filter(f.col("row_number")==1).drop('row_number')
        df_new.repartition(*PARTITION_FIELDS).write.mode('overwrite').parquet(latest_path+sds+'.parquet',partitionBy=PARTITION_FIELDS)

    sds_list = ['diagnosis']
    for sds in sds_list:
        logger.info('removing reversal and denial claim records from {0}'.format(sds))
        df = spark.read.parquet(all_path+sds+'.parquet').repartition('ct_claim_id').persist()
        logger.info('{0} sds initial record count: {1}'.format(sds,df.count()))
        df = df.join(rev_df,'ct_claim_id',how='leftanti')
        logger.info('Record count after filtering reversal claims: {0}'.format(df.count()))
        df = df.join(act_df,'ct_claim_id',how='leftanti')
        logger.info('Record count after filtering claims corresponding to reversal claims: {0}'.format(df.count()))
        df = df.join(den_df,'ct_claim_id',how='leftanti')
        logger.info('Record count after filtering denial claims: {0}'.format(df.count()))
        df = df.join(duplicate_df,'ct_claim_id',how='leftanti')
        logger.info('Record count after filtering duplicate Beacon inpatient claims: {0}'.format(df.count()))
        df_new = df.withColumn("row_number",f.row_number().over(Window.partitionBy(df.ct_claim_id,df.code,df.code_system,df.primary).orderBy((df.source_id).desc()))).filter(f.col("row_number")==1).drop('row_number')
        df_new.repartition(*PARTITION_FIELDS).write.mode('overwrite').parquet(latest_path+sds+'.parquet',partitionBy=PARTITION_FIELDS)


    # take latest member and eligibility data
    for sds in ['member','eligibility']:
        logger.info('rewriting {0}'.format(sds))
        all_df =  spark.read.parquet(all_path+sds+'.parquet')
        max_ts = all_df.select(f.max("updated_at")).collect()[0][0]
        new_df = all_df.filter(f.col('updated_at')==max_ts)
        logger.info('Record count: {0}'.format(new_df.count()))
        new_df.repartition(*PARTITION_FIELDS).write.mode('overwrite').parquet(latest_path+sds+'.parquet',partitionBy=PARTITION_FIELDS)

    logger.info('Removed Denied,reversed and duplicate beacon inpatient claims from SDS')

if __name__ == '__main__':

    spark = SparkSession.builder.getOrCreate()
    if len(sys.argv) < 2:
        print("Please enter data file date in YYYY-mm-dd format(required), disposition data path(optional) and transaction code data path(optional) if any")
        exit(1)

    filedate = sys.argv[1]

    if len(sys.argv) == 3:
        disposition_ext_data_path = sys.argv[2]
    else:
        disposition_ext_data_path = None
        
    if len(sys.argv) == 4:
        transaction_ext_data_path = sys.argv[3]
    else:
        transaction_ext_data_path = None

    logger.info("SDS Load for files dated: {0}".format(filedate))

    #filedate ='2021-08-20'
    #disposition_ext_data_path = '/data/supp_data/CF Disposition.csv' # ---for historical data---
    #disposition_ext_data_path = '/data/supp_data/CF Disposition_r2.csv' #---for incremental data---
    #transaction_ext_data_path = '/data/supp_data/CF Transaction Code.csv' #---for transaction data---

    input_data_path = 'wasbs://uploads@coretechscfdev.blob.core.windows.net/decrypted/'
    staging_data_path = '/data/staging/'
    output_data_path = '/data/sds/all/'
    latest_data_path = '/data/sds/filtered/'
    error_path = '/data/errors/'+filedate+'/'
    id_map_path = '/data/id_map/'+filedate+'_beacon_id_map.parquet'
    

    start_time = time.time()
    logger.info('Creating ID map for all members in data')
    ids = get_id_ref(spark, input_data_path, filedate)
    ids.write.parquet(id_map_path, mode='overwrite')
    logger.info('ID map ready')

    logger.info('----------PREPROCESSING----------')
    preprocess_start_time = time.time()
    dfs = preprocess(input_data_path, filedate, staging_data_path, id_map_path, output_data_path, disposition_ext_data_path, transaction_ext_data_path)
    logger.info('Completed data processing and writing to staging in {0}'.format(time.strftime("%H:%M:%S", time.gmtime(time.time() - preprocess_start_time))))

    logger.info('----------STANDARDIZING----------')
    std_start_time = time.time()
    standardize(dfs, input_data_path, output_data_path, filedate, error_path)
    logger.info('Completed data standardizing and writing to SDS in {0}'.format(time.strftime("%H:%M:%S", time.gmtime(time.time() - std_start_time))))

    logger.info('----------FILTERING----------')
    filter_start_time = time.time()
    filter_sds(output_data_path, latest_data_path, filedate)
    logger.info('Completed data filtering and writing to filtered SDS in {0}'.format(time.strftime("%H:%M:%S", time.gmtime(time.time() - filter_start_time))))

    logger.info("SDS Load completed for the {0} data in --- {1} ---" .format(filedate, time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))))