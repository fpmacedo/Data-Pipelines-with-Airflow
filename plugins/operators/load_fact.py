# pylint: disable=import-error
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_sql = """
        INSERT INTO {}
        {}
        ;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_query="",
                 *args, **kwargs):

        """
        :param redshift_conn_id: The redshift connection id. The name or identifier for
        establishing a connection to Redshift.
        :type redshift_conn_id: str
        :param table: The name of the table where the data should be loaded.
        :type table: str
        :param insert_query: The SQL statment that is used to retrieve the fact table data.
        :type insert_query: str
        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.insert_query = insert_query
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        #fact table just append the rows, without previous clearing
        #self.log.info("Clearing data from destination Redshift table")
        #redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Inserting data in destination Redshift {}".format(self.table))
        formatted_sql = LoadFactOperator.insert_sql.format(self.table, self.insert_query)
        redshift.run(formatted_sql)