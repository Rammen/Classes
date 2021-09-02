from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Take data from redshift and apply a SQL statement to create a dimension table (in redshift)
    """
    
    insert_dimension_table_sql = """
        INSERT INTO {}
        {} """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 dimension_table="",
                 sql_querry="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dimension_table = dimension_table
        self.sql_querry = sql_querry

    def execute(self, context):
        # Set credentials
        self.log.info("----> Setting Redshift credentials")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Selecting and transforming data, then loading into dimension table
        self.log.info(f"----> Collecting data and creating the dimension table {self.dimension_table}")
        formatted_table = LoadDimensionOperator.insert_dimension_table_sql.format(
            self.dimension_table,
            self.sql_querry)
        
        redshift.run(formatted_table)
