from dataclasses import dataclass
import json
from typing import List, Iterator, Tuple, TypeVar
import os

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
import ulid

from bigquery.helpers import exec_shell_command, write_json_string

T = TypeVar("T")

@dataclass
class TableAccess:
    project_id: str
    dataset_id: str
    table_id: str


class BigQueryTableRelease:
    OPERATION_TYPE = "bigquery_table_release"
    SCOPE = (
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/cloud-platform",
    )

    def __init__(self, project_id: str = "spdb-dev"):
        super(BigQueryTableRelease, self).__init__(self.OPERATION_TYPE)
        self.credentials = service_account.Credentials.from_service_account_file(
            filename=os.getenv("SERVICE_ACCOUNT_CREDENTIAL_PATH_DEFAULT"),
            scopes=self.SCOPE
        )
        self.project_id = project_id
        self.client = bigquery.Client(
            credentials=self.credentials
        )

    @staticmethod
    def _create_full_path_table_id(table_access: TableAccess):
        return f"{table_access.project_id}.{table_access.dataset_id}.{table_access.table_id}"

    def execute_legacy_sql(self, query: str,  dry_run: bool = False):
        """Execute SQL query.

        :param query:
        :param dry_run:
        :return:
        """
        try:
            job_config = bigquery.QueryJobConfig(dry_run=dry_run, use_query_cache=False)
            query_job = self.client.query(query, job_config=job_config)
            # waiting for the result
            query_job.result()
        except Exception as exception:
            raise exception

    def exist_table(self, table_access: TableAccess) -> Tuple[bool, T]:
        """Check if a table exists or not
        :param table_access: access properties of a table
        :return: boolean
        """
        project_id = table_access.project_id
        dataset_id = table_access.dataset_id
        table_id = table_access.table_id

        try:
            # dataset = client.dataset(dataset_id)
            # table_ref = dataset.table(table_id)
            table = self.client.get_table(f'{project_id}.{dataset_id}.{table_id}')
            return True, table.schema
        except NotFound:
            return False, None

    def get_table_schema(self,  table_access: TableAccess) -> T:
        """Get schema of an existing table.

        :param table_access: access properties of a table
        :return: table schema
        """
        _, schema = self.exist_table(table_access)
        return schema

    def create_table(self, table_access: TableAccess, schema: T) -> bool:
        """create a table on BigQuery with access path is: project_id.dataset_id.table_id

        :param table_access: access properties of a table
        :param schema: table schema
        :return: True if this new table can be created, False otherwise
        """
        exist, _ = self.exist_table(table_access)
        if exist:
            return False
        try:
            new_table_id = self._create_full_path_table_id(table_access)
            new_table = bigquery.Table(table_ref=new_table_id, schema=schema)
            self.client.create_table(new_table)
        except Exception as exception:
            raise exception

    def copy_table(self, src_table_access: TableAccess, dest_table_access: TableAccess):
        """Copy a table from src_table_access to dest_table_access

        :param src_table_access: source table
        :param dest_table_access: destination table
        :return: True if the copy job run successfully, False otherwise
        """
        # check if src_table_access exists or not
        exists, src_table_schema = self.exist_table(src_table_access)
        if not exists:
            return

        # Check if dest_table_access already exists
        dest_table_exists, dest_table_schema = self.exist_table(dest_table_access)
        if dest_table_exists:
            return

        # if dest_table does not exist
        src_table_id = self._create_full_path_table_id(src_table_access)
        dest_table_id = self._create_full_path_table_id(dest_table_access)

        try:
            # self.__create_table(dest_table_access, src_table_schema)
            job = self.client.copy_table(src_table_id, dest_table_id)
            job.result()  # Wait for the job to complete.
            # run the copy job to copy from
        except Exception as exception:
            raise exception

    def drop_table(self, table_access: TableAccess, backup: bool = True):
        """drop a table

        :param table_access: TableAccess
        :param backup: backup the table if it's True
        :return:
        """
        table_id = self._create_full_path_table_id(table_access)
        try:
            if not backup:
                self.client.delete_table(table_id, not_found_ok=False)
                return
            else:
                u = ulid.new()
                unique_id = u.timestamp().str
                backup_table_access: TableAccess = TableAccess(
                    project_id=table_access.project_id,
                    dataset_id=table_access.dataset_id,
                    table_id=table_access.table_id + unique_id
                )
                self.copy_table(table_access, backup_table_access)
                self.client.delete_table(table_id, not_found_ok=False)
        except Exception as exception:
            raise exception

    def delete_all_rows(self, table_access: TableAccess):
        """Delete all records of a table.

        :param table_access: Table Access
        :return:
        """
        table_id = self._create_full_path_table_id(table_access)
        try:
            query = f"""
                       DELETE FROM `{table_id}` WHERE TRUE
                   """
            self.execute_legacy_sql(query)
        except Exception as e:
            raise e

    def list_tables(self, dataset_id: str) -> Iterator:
        """List all tables of a dataset given this dataset_id.

        :param dataset_id: It should have the format of `project_id.dataset_id`
        :return:
        """
        try:
            tables = self.client.list_tables(dataset_id)
            return tables
        except Exception as exception:
            raise exception

    def create_table_revision(self, table_access: TableAccess):
        """Create a revision of a table for tracking changes history purpose.

        :param table_access:
        :return:
        """
        try:
            u = ulid.new()
            unique_id = str(u.timestamp().int)
            revision_table_access: TableAccess = TableAccess(
                project_id=table_access.project_id,
                dataset_id=table_access.dataset_id,
                table_id=table_access.table_id + unique_id
            )
            self.copy_table(table_access, revision_table_access)
        except Exception as exception:
            raise exception

    def modify_table_schema(
            self, table_access: TableAccess,
            columns_to_drop: List[bigquery.SchemaField],
            new_columns_to_add: List[bigquery.SchemaField],
            backup: bool = True
    ):
        """

        :param table_access:
        :param columns_to_drop
        :param new_columns_to_add
        :param backup
        :return:
        """
        table_id = self._create_full_path_table_id(table_access)
        try:
            table = self.client.get_table(table_id)
            if not table.schema:
                return

            # backup table
            if backup:
                u = ulid.new()
                unique_id = str(u.timestamp().int)
                backup_table_access: TableAccess = TableAccess(
                    project_id=table_access.project_id,
                    dataset_id=table_access.dataset_id,
                    table_id=table_access.table_id + unique_id
                )
                self.copy_table(table_access, backup_table_access)

            # Modify schema
            original_schema = table.schema
            new_schema = original_schema[:]

            # Adding new columns
            for new_col in new_columns_to_add:
                new_schema.append(new_col)

            # Remove existing columns
            for col in columns_to_drop:
                new_schema.remove(col)

            table.schema = new_schema
            self.client.update_table(table, ["schema"])
        except Exception as exception:
            raise exception

    @staticmethod
    def backup_table_to_gcs(dataset, table_name, to_dir, schema_only):
        """Store schema & data in table to GCS.

        :param dataset: BigQuery dataset name
        :param table_name: BigQuery table name
        :param to_dir: GCS output prefix
        :param schema_only: don't export data, just the schema
        :return: None
        """

        full_table_name = f'{dataset}.{table_name}'

        # write schema to GCS
        schema = exec_shell_command(['bq', 'show', '--schema', full_table_name])
        write_json_string(
            schema,
            os.path.join(to_dir, dataset, table_name, 'schema.json')
        )

        if not schema_only:
            # back up the table definition
            tbldef = exec_shell_command(['bq', '--format=json', 'show', full_table_name])
            write_json_string(
                tbldef,
                os.path.join(to_dir, dataset, table_name, 'tbldef.json')
            )

            tbldef = json.loads(tbldef)  # array of dicts
            if tbldef['type'] == 'VIEW':
                return  # no need to extract data

            # read the data
            output_data_name = os.path.join(to_dir, dataset, table_name, 'data_*.avro')
            _ = exec_shell_command([
                'bq', 'extract',
                '--destination_format=AVRO',
                '--use_avro_logical_types',  # for DATE, TIME, NUMERIC
                '{}.{}'.format(dataset, table_name),
                output_data_name
            ])

