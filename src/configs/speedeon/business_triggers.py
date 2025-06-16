def config(env: str, partition_value: str) -> dict:
    """
    Description : Configuration file for Speedeon
    :param env: environment
    :param partition_value: data processing date
    :return:
    """
    if env in ['dev', 'qa']:
        sftp_source = '/Inbox/Sample/speedeon/business_triggers/'
        ncoa_request_dir = '/Inbox/Sample/speedeon/business_triggers/request/'
        ncoa_response_dir = '/Inbox/Sample/speedeon/business_triggers/request/'
    elif env == 'preprod':
        sftp_source = '/Inbox/speedeon/business_triggers/'
        ncoa_request_dir = '/Inbox/speedeon/business_triggers/request/'
        ncoa_response_dir = '/Inbox/speedeon/business_triggers/request/'
    elif env == 'prod':
        sftp_source = '/Speedeon/'
        ncoa_request_dir = '/'
        ncoa_response_dir = '/Jobs_Completed/'
    else:
        print ("Incorrect Environment")
        raise Exception
    return {
    'ncoa_calculated_fields': [
        ('company_st', 'F.col("company_name")'),
        ('firstname_st', 'F.upper(F.trim(F.col("first_name")))'),
        ('lastname_st', 'F.upper(F.trim(F.col("last_name")))'),
        ('address_st',  'F.col("address_1")'),
        ('city_st', 'F.col("city")'),
        ('state_st', 'F.col("state")'),
        ('zip5_st', 'F.lpad(F.col("zip"), 5, "0")'),
        ('zip4_st', 'F.lpad(F.col("zip_4"), 4, "0")'),
        ('cass_addr_err', 'F.lit(None).cast(StringType())'),
        ('mail_score', 'F.lit(None).cast(StringType())'),
        ('ncoa_addr_err', 'F.lit(None).cast(StringType())'),
        ('return_code', 'F.lit(None).cast(StringType())'),
        ('move_type', 'F.lit(None).cast(StringType())'),
        ('move_date', 'F.lit(None).cast(StringType())'),
        ('dpc', 'F.lit(None).cast(StringType())'),
        ('crrt', 'F.lit(None).cast(StringType())')
    ],

    'calculated_fields': [
        ('fmcgid', f'F.concat(F.lit("SPDN"), F.lit(datetime.strptime("{partition_value}","%Y%m%d").strftime("%m%d%y")), F.lpad(F.row_number().over(Window.orderBy(F.monotonically_increasing_id())), 6, "0"))'),
        ('dlx_recordid', f'F.concat(F.lit("SPDN"), F.lit(datetime.strptime("{partition_value}","%Y%m%d").strftime("%m%d%y")), F.lpad(F.row_number().over(Window.orderBy(F.monotonically_increasing_id())), 6, "0"))'),
        ('companyname', 'F.upper(F.trim(F.col("company_name")))'),
        ('contactname', 'F.concat_ws(" ", F.upper(F.trim(F.col("first_name"))), F.upper(F.trim(F.col("last_name"))))'),
        ('middle_initial', 'F.lit(None).cast(StringType())'),
        ('contact_suffix', 'F.lit(None).cast(StringType())'),
        ('sic_code', 'parse_sic(F.col("sic_code"))'),
        ('phone', 'F.col("business_phone")'),
        ('source', 'F.lit("SPDN")'),
        ('trigger_type', 'F.lit("NEW & EXP")'),
        ('work_at_home','F.when((F.col("worksite_type") == "HOME"), "Y").when((F.col("worksite_type") == "COMMERCIAL"), "N").otherwise("0")'),
        ('url', 'F.lit(None).cast(StringType())'),
        ('email', 'F.lit(None).cast(StringType())'),
        ('file_type', 'F.lit(None).cast(StringType())'),
        ('week_st', 'F.substring(F.col("fmcgid"), 5, 6)'),
        ('contact_title', 'F.lit(None).cast(StringType())'),
        ('ra_flag', 'F.lit(None).cast(StringType())'),
        ('database_id', 'F.col("speedeonid")'),
        ('employees', 'F.lit(None).cast(StringType())'),
        ('has_databaseid', 'F.when((F.col("speedeonid")== ""), "0").otherwise("1")'),
        ('has_url', 'F.lit("0")'),
        ('has_email', 'F.lit("0")'),
        ('has_filetype', 'F.lit("0")'),
        ('provider_rank', 'F.lit("15")'),
        ('sic_desc', 'F.lit(None).cast(StringType())'),
        ('record_type', 'F.lit(None).cast(StringType())'),
        ('sic2', 'F.lit(None).cast(StringType())'),
        ('sic3', 'F.lit(None).cast(StringType())'),
        ('sic4', 'F.lit(None).cast(StringType())'),
        ('sic5', 'F.lit(None).cast(StringType())'),
        ('sic6', 'F.lit(None).cast(StringType())'),
        ('sic7', 'F.lit(None).cast(StringType())'),
        ('sic8', 'F.lit(None).cast(StringType())'),
        ('title', 'F.lit(None).cast(StringType())'),
        ('title_code', 'F.lit(None).cast(StringType())'),
        ('corporation_date', 'F.lit(None).cast(StringType())'),
        ('sales_string', 'F.lit(None).cast(StringType())'),
        ('sales_cd', 'F.lit(None).cast(StringType())'),
        ('salesvolume_num', 'F.lit(None).cast(StringType())'),
        ('preopen', 'F.lit(None).cast(StringType())'),
        ('franchise', 'F.lit(None).cast(StringType())'),
        ('filing_type', 'F.lit(None).cast(StringType())'),
        ('hotline_verified_flag', 'F.lit(None).cast(StringType())'),
        ('hotline_business_type', 'F.lit(None).cast(StringType())'),
        ('production_date_v2', 'F.lit(None).cast(StringType())'),
        ('multi_sourced', 'F.lit(None).cast(StringType())'),
        ('industry_code', 'F.lit(None).cast(StringType())'),
        ('pre_opening', 'F.lit(None).cast(StringType())'),
        ('tradename', 'F.lit(None).cast(StringType())'),
        ('single_hq_branch', 'F.lit(None).cast(StringType())'),
        ('lob', 'F.lit(None).cast(StringType())'),
        ('bizowner_name', 'F.lit(None).cast(StringType())'),
        ('bizowner_address', 'F.lit(None).cast(StringType())'),
        ('bizowner_city', 'F.lit(None).cast(StringType())'),
        ('bizowner_state', 'F.lit(None).cast(StringType())'),
        ('bizowner_zip5', 'F.lit(None).cast(StringType())'),
        ('bizowner_zip4', 'F.lit(None).cast(StringType())'),
        ('bizowner_phone', 'F.lit(None).cast(StringType())'),
        ('emp_total_reliability', 'F.lit(None).cast(StringType())'),
        ('emp_reliability', 'F.lit(None).cast(StringType())'),
        ('emp_total', 'F.lit(None).cast(StringType())'),
        ('emp_tot_cd', 'F.lit(None).cast(StringType())'),
        ('user_area', 'F.lit(None).cast(StringType())'),
        ('ult_duns', 'F.lit(None).cast(StringType())'),
        ('headquarter_duns', 'F.lit(None).cast(StringType())'),
        ('parentcompany_duns', 'F.lit(None).cast(StringType())'),
        ('emp_cd', 'F.lit(None).cast(StringType())'),
        ('bricategory16', 'F.lit(None).cast(StringType())'),
        ('brirawscore_num', 'F.lit(None).cast(StringType())'),
        ('vendor', 'F.lit("SPDN")'),
        ('source_type', 'F.lit("BUSINESS_TRIGGERS")'),
    ],

    'data_category': 'business',
    'core_table_name': 'business_speedeon_business_triggers',
    'check_names':True,
    'parameter_store':'dlx-ddm-sterling-sftp-creds',
    'melissa_parameter_store':'dlx-ddm-melissa-sftp-creds',
    'source_filename': 'FMCG_SpeedeonNewBusiness_DDMMMYY.csv',
    'source_extract_filename': 'FMCG_SpeedeonNewBusiness_DDMMMYY.csv',
    'source_count_filename'  : 'FMCG_SpeedeonNewBusiness_DDMMMYY_count.txt',
    'file_arrival_day':'MON',
    'sftp_transfer':'push',
    'source_dir': sftp_source,
    'ncoa_request_dir': ncoa_request_dir,
    'ncoa_response_dir': ncoa_response_dir,
    'file_checks':{'extension': '.csv', 'encoding': 'ascii', 'type': 'delimited'},
    'quality_checks':{'first_name':'Completeness'},
    'non_ascii_chars_threshold' : '0.02'
    }