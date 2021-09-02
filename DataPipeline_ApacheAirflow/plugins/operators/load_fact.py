from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


"""
This operator:
1. Takes data from the staging tables in redshift
2. Apply an SQL statement to create the fact table
3. Save the new dimension table in redshift

Requires the empty table in redshift to be created prior

"""

class LoadFactOperator(BaseOperator):
    
    # This is the base for the SQL statement
    insert_fact_table_sql = """
        INSERT INTO {}
        {} """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 fact_table="",
                 sql_querry="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_table = fact_table
        self.sql_querry = sql_querry

    def execute(self, context):
        # Set credentials
        self.log.info("----> Setting Redshift credentials")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Selecting and transforming data, then loading into fact table
        self.log.info(f"----> Collecting data and creating the fact table {self.fact_table}")
        formatted_table = LoadFactOperator.insert_fact_table_sql.format(
            self.fact_table,
            self.sql_querry)
        
        redshift.run(formatted_table)
        
