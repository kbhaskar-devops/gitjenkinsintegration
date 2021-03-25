[hadoop@ip-172-31-34-67 ~]$ vi temp_final.py
    master_query = '(select File_ID, File_Name, Delimiter, File_status_flag from {}.File_Master where File_status_flag="N") as table3'.format(config['mysql']['database'])
    # jdbc_url = "jdbc:mysql://{host}:{port}/mysql?user={user}&password={password}".format(**config["mysql"])
    master_df = spark.read.format('jdbc').options(driver = 'com.mysql.jdbc.Driver',url=jdbc_url, dbtable=master_query).load()
    file_names = master_df.select('File_Name').rdd.flatMap(lambda x: x).collect()
    print(file_names)
    for i in file_names:
        job_start_time = datetime.datetime.now()
        fileMD = read_metadata_from_mysql_RDS(config,i,jdbc_url)
        delim = fileMD.collect()[0]['Delimiter']
        s3_loc = 's3a://{0}/{1}'.format(config['s3']['File_Bucket'], i)
        if delim == 'fixedwidth':
            filefroms3 = read_fW_file_from_s3(s3_loc)
        else:
            filefroms3 = spark.read.csv('s3://inputfilebucketck/{}'.format(i), inferSchema = True, header = True, sep = delim)

        source=filefroms3
        source_count = filefroms3.count()
        filefroms3 = data_cleansing(filefroms3,i,config,jdbc_url)
        eliminated_recs_count = source_count-filefroms3.count()
        print(eliminated_recs_count)
        eliminated_recs = source.subtract(filefroms3)
        Primarykeyquery = "(select b.Column_name, b.Key_IND from {0}.File_Master a inner join {0}.File_Columns b on a.File_ID = b.File_ID \
                    where a.File_Name = '{1}' and b.Key_IND = 'PK') as table2".format(config["mysql"]["database"],i)
        Primarykeydf = spark.read.format('jdbc').options(driver = 'com.mysql.jdbc.Driver',url=jdbc_url, dbtable=Primarykeyquery).load()
        Primarykeys = list(map(lambda x : x[0],Primarykeydf.select("Column_name").collect()))
        eliminated_recs_pks = eliminated_recs.select(Primarykeys).rdd.map(tuple).collect()
        eliminated_records = json.dumps(eliminated_recs_pks)
        if len(eliminated_records) != 0:
                audit_dq_reject_params = {
                        'file_name':i,\
                        'dq_rule_violated':"Data cleansing",\
                        'rejected_records':eliminated_records,\
                        'job_run_date':job_start_time,\
                        'job_start_time':job_start_time,\
                        'job_name':app_name
                }
                audit_updation(audit_dq_reject_params,config,dbtype,audit_reject,jdbc_url)



        is_cnames_valid,col_name_status = column_names_validation(filefroms3,fileMD)
        is_dtypes_valid,dtype_status = data_types_validation(filefroms3,fileMD)
"temp_final.py" 225L, 12796C  