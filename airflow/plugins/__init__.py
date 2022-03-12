from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

class IndexTrackerPlugin(AirflowPlugin):
    name = "indextracker_plugin"
    operators = [
        operators.BranchS3KeyOperator,
        operators.DownloadToS3Operator
    ]
    helpers = [
        helpers.create_staging_table,
        helpers.create_staging_table_index,
        helpers.create_staging_table_etf,
        helpers.create_staging_table_futures,
        helpers.create_staging_table_options,
    ]
