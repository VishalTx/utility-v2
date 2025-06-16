def config(env: str, partition_value: str) -> dict:
    """
    Description : Configuration file for Speedon New Mover
    :param env: environment
    :param partition_value: data processing date
    :return:
    """
    if env in ['dev', 'qa']:
        sftp_source = '/Inbox/Sample/speedeon/'
        ncoa_request_dir = '/Inbox/Sample/speedeon/request/'
        ncoa_response_dir = '/Inbox/Sample/speedeon/request/'
    elif env == 'preprod':
        sftp_source = '/Inbox/speedeon/'
        ncoa_request_dir = '/Inbox/speedeon/request/'
        ncoa_response_dir = '/Inbox/speedeon/request/'
    elif env == 'prod':
        sftp_source = '/Speedeon/'
        ncoa_request_dir = '/'
        ncoa_response_dir = '/Jobs_Completed/'
    else:
        print ("Incorrect Environment")
        raise Exception
    return {
    'ncoa_calculated_fields': [
        ('company_st', 'F.concat_ws(" ",F.col("fname"), F.col("lname"))'),
        ('firstname_st', 'F.col("fname")'),
        ('lastname_st', 'F.col("lname")'),
        ('address_st', 'F.col("address")'),
        ('city_st', 'F.col("city")'),
        ('state_st', 'F.col("state")'),
        ('zip5_st', 'F.lpad(F.col("zip"), 5, "0")'),
        ('zip4_st', 'F.lpad(F.col("zip4"), 4, "0")'),
        ('cass_addr_err', 'F.lit(None).cast(StringType())'),
        ('mail_score', 'F.lit(None).cast(StringType())'),
        ('ncoa_addr_err', 'F.lit(None).cast(StringType())'),
        ('return_code', 'F.lit(None).cast(StringType())'),
        ('move_type', 'F.lit(None).cast(StringType())'),
        ('move_date', 'F.col("move_date")'),
        ('dpc', 'F.lit(None).cast(StringType())'),
        ('crrt', 'F.lit(None).cast(StringType())')
    ],
    'life_events_calculated_fields': [
        ('fmcgid', f'F.concat(F.lit("SPD"), F.lit(datetime.strptime("{partition_value}","%Y%m%d").strftime("%m%d%y")), F.lpad(F.row_number().over(Window.orderBy(F.monotonically_increasing_id())), 7, "0"))'),
        ('dlx_recordid', f'F.concat(F.lit("SPD"), F.lit(datetime.strptime("{partition_value}","%Y%m%d").strftime("%m%d%y")), F.lpad(F.row_number().over(Window.orderBy(F.monotonically_increasing_id())), 7, "0"))'),
        ('prev_address', 'F.col("o_addr")'),
        ('prev_city', 'F.col("o_city")'),
        ('prev_state', 'F.col("o_state")'),
        ('prev_zip', 'F.when((F.col("o_zip5")).isNotNull() & (F.trim(F.col("o_zip5")) != ""), F.lpad(F.trim(F.col("o_zip5")), 5, "0")).otherwise(F.lit(None).cast(StringType()))'),
        ('prev_flag', 'F.when((F.col("prev_address").isNotNull()) & (F.trim(F.col("prev_address")) != ""), "1").otherwise("0")'),
        ('phone', 'F.substring(F.regexp_replace(F.col("phone"), r"[^0-9]", ""), -10, 10)'),
        ('phone_flag', 'F.when((F.col("phone").isNotNull()) & (F.col("phone") != ""), "1").otherwise("0")'),
        ('age', 'F.col("age_z4")'),
        ('vendorid', 'F.col("speedeonid")'),
        ('source', 'F.lit("SPEEDEON")'),
        ('vendor', 'F.lit("SPEEDEON")'),
        ('source_type', 'F.lit("New Mover")'),
        ('listing_price', 'F.lit(None).cast(StringType())'),
        ('dwelling', 'F.lit(None).cast(StringType())'),
        ('geostatuscode', 'F.lit(None).cast(StringType())'),
        ('latitude', 'F.lit(None).cast(StringType())'),
        ('longitude', 'F.lit(None).cast(StringType())'),
        ('listing_date', 'F.lit(None).cast(StringType())'),
        ('dwelling_type', 'F.lit(None).cast(StringType())'),
        ('median_household_income_included_feature_v12', 'F.lit(None).cast(StringType())'),
        ('rpi_current_address_included_feature_v12', 'F.lit(None).cast(StringType())'),
        ('rpi_previous_address_included_feature_v12', 'F.lit(None).cast(StringType())'),
        ('ehi_included_feature_infutor', 'F.lit(None).cast(StringType())'),
        ('notmover_included_feature_infutor', 'F.lit(None).cast(StringType())'),
        ('data_type_deed_included_feature_focus', 'F.lit(None).cast(StringType())'),
        ('source_code_included_feature_ach', 'F.lit(None).cast(StringType())'),
        ('household_flag_included_feature_ach', 'F.lit(None).cast(StringType())'),
        ('listing_date_included_feature_focus_premoverlist', 'F.lit(None).cast(StringType())'),
        ('file_code_included_feature_v12_premover', 'F.lit(None).cast(StringType())'),
        ('activity_date_included_feature_v12_premover', 'F.lit(None).cast(StringType())'),
        ('list_date_included_feature_v12_premover', 'F.lit(None).cast(StringType())'),
        ('listing_date_included_feature_v12_premover', 'F.lit(None).cast(StringType())'),
        ('pm_tr_date_included_feature_avrick_premovercontract', 'F.lit(None).cast(StringType())'),
        ('event_type_included_feature_sms_le', 'F.lit(None).cast(StringType())'),
        ('program_name_included_feature_sms_premover', 'F.lit(None).cast(StringType())'),
        ('listing_date_included_feature_focus_premovercontract', 'F.lit(None).cast(StringType())'),
        ('gender_included_feature_crosslists_newparent', 'F.lit(None).cast(StringType())'),
        ('child_age_code_included_feature_crosslists_newparent', 'F.lit(None).cast(StringType())'),
        ('child_0_18_included_feature_experian_newparent', 'F.lit(None).cast(StringType())'),
        ('child_0_3_included_feature_experian_newparent', 'F.lit(None).cast(StringType())'),
        ('date_on_file_included_feature_experian_newparent', 'F.lit(None).cast(StringType())'),
        ('pm_tr_date_included_feature_avrick_premoverlist', 'F.lit(None).cast(StringType())'),
        ('homeowner_renter_flag', 'F.lit(None).cast(StringType())'),
        ('buildingareasqft', 'F.lit(None).cast(StringType())'),
        ('childpresence', 'F.lit(None).cast(StringType())'),
        ('hh_age', 'F.lit(None).cast(StringType())'),
        ('lead_sourcing_intel', 'F.lit(None).cast(StringType())'),
        ('sale_date', 'F.lit(None).cast(StringType())'),
        ('sale_amount', 'F.lit(None).cast(StringType())'),
        ('landusecode', 'F.lit(None).cast(StringType())'),
        ('mortgage_lender_name', 'F.lit(None).cast(StringType())'),
        ('mortgage_amount', 'F.lit(None).cast(StringType())'),
        ('mortgage_term', 'F.lit(None).cast(StringType())'),
        ('mortgage_interest_rate', 'F.lit(None).cast(StringType())'),
        ('mortgage_type', 'F.lit(None).cast(StringType())'),
        ('second_home_flag', 'F.lit(None).cast(StringType())'),
        ('namesuffix', 'F.lit(None).cast(StringType())'),
        ('middle_initial_name', 'F.lit(None).cast(StringType())'),
        ('homeowner_corp_indentifier', 'F.lit(None).cast(StringType())'),
        ('mobile_home_indicator', 'F.lit(None).cast(StringType())'),
        ('timeshare_indicator', 'F.lit(None).cast(StringType())'),
        ('ownership_rights', 'F.lit(None).cast(StringType())'),
        ('relationship_type', 'F.lit(None).cast(StringType())'),
        ('new_construction_flag', 'F.lit(None).cast(StringType())'),
        ('inter_family_sale', 'F.lit(None).cast(StringType())'),
        ('email', 'F.lit(None).cast(StringType())'),
        ('occupant_type', 'F.lit(None).cast(StringType())'),
        ('move_type_vendor', 'F.lit(None).cast(StringType())'),
        ('move_date_vendor', 'F.lit(None).cast(StringType())'),
        ('child_dob', 'F.lit(None).cast(StringType())'),
        ('weddingdate', 'F.lit(None).cast(StringType())'),
    ],

    'data_category': 'consumer',
    'core_table_name': 'consumer_speedeon_new_mover',
    'check_names':True,
    'parameter_store':'dlx-ddm-sterling-sftp-creds',
    'melissa_parameter_store':'dlx-ddm-melissa-sftp-creds',
    'source_filename': 'FMCG_TD_SantanderBank_MMDDYY.*.txt',
    'source_extract_filename': 'FMCG_TD_SantanderBank_MMDDYY.*.txt',
    'source_count_filename':'FMCG_TD_SantanderBank_MMDDYY_count.*.txt',
    'bypass_file':'N',
    'bypass_source_filename':'',
    'bypass_source_extract_filename':'',
    'file_arrival_day':'THU',
    'sftp_transfer':'push',
    'source_dir': sftp_source,
    'ncoa_request_dir': ncoa_request_dir,
    'ncoa_response_dir': ncoa_response_dir,
    'file_checks':{'extension': '.txt', 'encoding': 'ascii', 'type': 'delimited'},
    'quality_checks':{'fname':'Completeness'},
    'non_ascii_chars_threshold' : '0.02'
    }