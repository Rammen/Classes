from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowSkipException

"""
This operator aims to ensure the quality of the data. 
It takes a table and checks if it is empty.
No table should be empty at this point. Thus if one table is empty there's an error somewhere
"""

class EmptyQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 tables = [],
                 *args, **kwargs):

        super(EmptyQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):

        # Set redshift credentials
        self.log.info("----> Setting Redshift credentials")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # If there is no tables to ckeck the quality of the data: this task will be marked as skipped in airflow
        if len(self.tables) == 0:
            self.log.info('No tables were given to check the data quality. This step will be skipped')
            raise AirflowSkipException
            
        self.log.info('Processing the tables')
        
        # Check all the tables one by one to see if it is empty
        for table in self.tables:
            self.log.info(f"Checking if table {table} is empty")
            number_rows = redshift.get_records(f"SELECT COUNT(*) FROM {table}")[0][0]
            
            # If there is more than 0 row, there is data in the table
            if number_rows > 0:
                self.log.info(f"Table {table} contains {number_rows} rows of data")
            
            else:
                self.log.info(f"This table is EMPTY. Marking this test as FAILED")
                raise ValueError()

         
        self.log.info('Empty data quality checks: succeed')
        
        return