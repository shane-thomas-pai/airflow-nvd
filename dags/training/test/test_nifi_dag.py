from airflow.utils.trigger_rule import TriggerRule

from training.custom_dags.nifi_dag import dag


def test_nifi_dag_has_correct_task_order():
    nifi_dag = dag()
    nifi_flow_task = nifi_dag.get_task("schedule-nifi-flow")
    quality_check_task = nifi_dag.get_task("quality-check")

    assert nifi_flow_task.downstream_list == [quality_check_task]


def test_quality_check_only_happens_when_nifi_flow_successful():
    nifi_dag = dag
    nifi_flow_task = nifi_dag.get_task("schedule-nifi-flow")

    assert nifi_flow_task.trigger_rule == TriggerRule.ALL_SUCCESS
