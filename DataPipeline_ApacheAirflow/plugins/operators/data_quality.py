from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowSkipException


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):

        # Set redshift credentials
        self.log.info("----> Setting Redshift credentials")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # If there is no querries to ckeck the quality of the data: this task will be marked as skipped in airflow
        if len(self.checks) == 0:
            self.log.info('No querry were given to check the data quality. This step will be skipped')
            raise AirflowSkipException
            
        self.log.info('Processing the querries')
        
        for check in self.checks:
            querry=check.get('sql_check')
            expected_value=check.get('expect_value')
             
            self.log.info(f"Trying the following querry: {querry}")
            actual_value = redshift.get_records(querry)[0][0]
            self.log.info(f"Expected value of {expected_value} --> Value returned from the querry is of {actual_value}")
            
            if actual_value == expected_value:
                self.log.info("This querry returned the appropriate value")
            
            else:
                self.log.info(f"This querry DID NOT returned the appropriate value. Marking this test as FAILED")
                raise ValueError()

         
        self.log.info('Data quality checks: succeed')
        
        return
    
