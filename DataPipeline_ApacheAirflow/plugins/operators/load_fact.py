from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Take data from redshift and apply a SQL statement to create a new fact table (in redshift)
    """
    
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
        
