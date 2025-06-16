from utils.aws_wrapper import AWSDynamoDB, AWSS3
from utils.bitbucket_wrapper import BitbucketWrapper
from utils.extract_script_part import extract_list

class Job:
    def __init__(self):
        self.BASE_CONSUMER_TRIGGER = None
        self.s3 = None
        self.campaign_wednesday = None
        self.stack_file_path = None
        self.dynamodb = None

    def run(self):
        print("Job Started")
        # Initiating the resources
        self.dynamodb = AWSDynamoDB()
        self.s3 = AWSS3()
        # fetching updating the repository
        repos = BitbucketWrapper()
        op_type, status, message = repos.clone_repo()
        if status:
            '''Started Job Logic here'''
            self.prerequisites()
        else:
            print(f'Error in fetching repository. Check environment details Error: [{message}]')
            '''Exnd Job Logic here'''
            self.done()

    def done(self, error=False, error_message=''):
        if error:
            print("Job Ended with ERROR")
        else:
            print("Job Ended with SUCCESS")

    def raise_error_and_stop(self, job_error):
        print(f"Error occurred: {job_error}")
        self.done()

    def table_exists(self, table_name):
        if self.dynamodb.table_exists('ddm_client_trigger_config'):
            print('`ddm_client_trigger_config` table exists')
        else:
            self.raise_error_and_stop(f'[Pre-requisites FAILED] Table `{table_name}` does not exist')

    def prerequisites(self):
        print("Started Pre-requisites")
        self.table_exists('ddm_client_trigger_config')
        self.table_exists('ddm_client_trigger_waterfall')
        self.table_exists('ddm_client_trigger_step')

        data = self.dynamodb.get_item('ddm_client_trigger_config', 'travis-consumer_trigger')
        # get `stack_file_path
        self.stack_file_path = data['general']['stack_file_path']
        self.campaign_wednesday = data['eligibility']['campaign_wednesday']
        bucket_name = "dlx-ddm-consume-qa"
        prefix = f"{self.stack_file_path}/dt={self.campaign_wednesday}/"
        # print("1. >>>> ", prefix)
        # adding this for demo purpose
        prefix = f"{self.stack_file_path}/dt=20230125/"
        # print("2. >>>> ", prefix)
        # bucket_contents = self.s3.list_bucket_objects(bucket_name, prefix)
        # print("Input Bucket Contents", bucket_contents)
        self.load_column_schema()

    def load_column_schema(self):
        # Path to BASE_CONSUMER
        # i.e. 'd3_ingest/src/dev/glue-files/site-packages/standardized_packages/source_code/load_in/trigger_load_in/trigger_load_in.py'
        bucket_local_path = self.s3.getenv('Bitbucket_Local_Path')
        script_path = bucket_local_path + '/glue-files/site-packages/standardized_packages/source_code/load_in/trigger_load_in/trigger_load_in.py'
        list_name = 'BASE_CONSUMER_TRIGGER'
        self.BASE_CONSUMER_TRIGGER = extract_list(script_path, list_name)
        print("Base Consumer Trigger", self.BASE_CONSUMER_TRIGGER)
        self.previous_logic_check()

    def previous_logic_check(self):
        '''
        Previous Logic
        --------------
        Load previous run logic for both Input and Output files AWS S3 Bucket path/URI
         - if no JSON Object present for campaign_wednesday on Input files and no
        JSON Object present for current_week on Output files
        '''
        self.load_glue_job()

    def load_glue_job(self):
        '''
        Load Glue Job
        -------------
        Load In Glue Job to construct from the derived logic from config table of
        Dynamo DB as per point#2 & #3 to have fully qualified Input and Output
        AWS S3 bucket file paths
        '''
        self.apply_state()

    def apply_state(self):
        '''
        Apply State
        -----------
        Apply State footprint cut data filtering logic based on the JSON Object
        present for state_footprint from Dynamo DB table 'ddm_client_trigger_config'
        '''
        self.apply_zip5_zip9()

    def apply_zip5_zip9(self):
        '''
        Apply Zip5 & Zip9 footprint cut data
        ------------------------------------
        Apply Zip5 & Zip9 footprint cut data filtering on US Zip Codelogic based on
        the JSON Objects fields presence zip5_footprint & zip9_footprint to get the
        S3 path location for the zip codes outlined in the file as details having
        dependency to filter Zip codes from the lookup file (Zip Txt or CSV) S3
        path as provided from Dynamo DB table 'ddm_client_trigger_config'
        '''
        self.write_pre_glue_job_output()

    def write_pre_glue_job_output(self):
        '''
        Write Pre Glue job output (Reliability Test)
        --------------------------------------------
        Write Pre Glue job output (Reliability Test) of our Schema validation reports
        on Source Input files ensuring that all 33 Mandatory Columns are present if not
         present log a defect in Jira upon post actual Glue job execution of Load In
        '''
        self.execute_glue_job()

    def execute_glue_job(self):
        '''
        Execution of glue job `dlx-ddm-trigger-load-in`
        -----------------------------------------------
        Run the Glue Job via python script to execute the Glue Job named
        - Glue Job Name: dlx-ddm-trigger-load-in
        Note: only if job failed,  this job get failed then extract logs from cloudwatch.
        print it.
        '''
        self.waterfall_data_logs()

    def waterfall_data_logs(self):
        '''
        Waterfall data logs
        -------------------
        Connect to Dynamo DB. Waterfall data logs updates in "ddm_client_trigger_waterfall"
        Table to get column filtering for columns Details , High & Qty upon applying filter
        with parameters config_id & run_dt
        these logs have Input files Data Suppressed and Drop% details for all the executions
        handled by the AWS Glue job
        '''
        self.step_logs_update()

    def step_logs_update(self):
        '''
        Step logs updates in "ddm_client_trigger_step" Table
        ----------------------------------------------------
        Connect to Dynamo DB. Step logs updates in "ddm_client_trigger_step" Table to get
        column filtering for column message upon applying filter with parameters config_id
        & run_dt these logs add messages from logs for all the executions handled by the
        AWS Glue job
        '''
        self.validate_bucket_output()

    def validate_bucket_output(self):
        '''
        Validate the output AWS S3
        --------------------------
        Validate the Output file at the designated AWS S3 bucket output file location
        Sample Output file
        Path: s3://useast1-dlx-{env}-ddm-client-process/ddm_client/{client}/data_processing/consumer_triggers_{year}/weekly_processing/week_number/intermediate_files/ 00
        - In Footprint.parquet
        '''
        self.validate_scenarios()

    def validate_scenarios(self):
        '''
        Cover Load Scenarios In Glue Job
        --------------------------------
        Cover Load In Glue Job positive and negative Scenarios as in this  Confluence
        Page: Consumer Trigger - Loadin Test steps - Deluxe Data Discovery
         - Confluence for all negative scenarios it must log a defect in Jira upon
         post actual Glue job execution of Load In
        '''
        self.write_schema_validation()

    def write_schema_validation(self):
        '''
        Write out schema validation report
        ----------------------------------
        Write output of our Schema validation reports on Output file ensuring that
        all 33 Mandatory Columns are present if not present log a defect in Jira
        upon post actual Glue job execution of Load In
        '''
        self.write_output_parquet_as_csv()

    def write_output_parquet_as_csv(self):
        '''
        Write output of Input Parquet file converted as S3Source.csv
        ------------------------------------------------------------
        Write output of Input Parquet file converted as S3Source.csv & Output
        Parquet file converted as S3Target.csv in parquet folder in project
        root folder for QS to pic these output files from parquet to do
        end to end data validations by QuerySurge when run with CI Tool
        Harness (Deluxe DevOps Tool) headless setup
        '''
        self.done()



if __name__ == "__main__":
    glue_job = Job()
    glue_job.run()
