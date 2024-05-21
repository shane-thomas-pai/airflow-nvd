from airflow.models import BaseOperator

from training.utils.nifi_connection.pipeline_steps import trigger_nifi_flow


class NifiOperator(BaseOperator):
    def __init__(self, name, **kwargs):
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        trigger_nifi_flow()
