# pylint: disable=import-error
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
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
                 truncate_insert=True,
                 *args, **kwargs):

        """
        :param redshift_conn_id: The postgres connection id. The name or identifier for
        establishing a connection to the Postgrea database.
        :type redshift_conn_id: str
        :param table: A table to insert the data.
        :type table: str
        :param insert_query: The SQL statment that is used to retrieve the fact table data.
        :type insert_query: str
        :param truncate_insert: Wheter to trucate the target table before insertion or not.
        :type truncate_insert: bool
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.insert_query = insert_query
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.truncate_insert = truncate_insert

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_insert:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Inserting data in destination Redshift {}".format(self.table))
        formatted_sql = LoadDimensionOperator.insert_sql.format(self.table, self.insert_query)
        redshift.run(formatted_sql)