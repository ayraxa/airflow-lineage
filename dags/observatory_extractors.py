from __future__ import annotations

from airflow.providers.openlineage.extractors.base import BaseExtractor, OperatorLineage
from openlineage.client.event_v2 import Dataset


class ObservatoryPythonExtractor(BaseExtractor):
    """
    custom extractor for demo DAG.
    it returns fixed input/output datasets per task_id so Marquez can draw lineage.
    """

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        # Apply to PythonOperator tasks (we’ll filter by dag_id inside)
        return ["PythonOperator"]

    def _execute_extraction(self) -> OperatorLineage | None:
        op = self.operator

        # Only affect our demo DAG; don’t mess with other PythonOperators
        if getattr(op, "dag_id", None) != "pipeline_observatory":
            return OperatorLineage()

        raw = Dataset(namespace="pipeline_observatory", name="file:///opt/airflow/data/raw/raw.csv")
        clean = Dataset(namespace="pipeline_observatory", name="file:///opt/airflow/data/clean/clean.parquet")
        report = Dataset(namespace="pipeline_observatory", name="file:///opt/airflow/data/reports/quality_report.json")
        summary = Dataset(namespace="pipeline_observatory", name="file:///opt/airflow/data/summary/summary.csv")

        mapping = {
            "ingest_raw": ([], [raw]),
            "clean_transform": ([raw], [clean]),
            "quality_report": ([clean], [report]),
            "publish_summary": ([clean], [summary]),
        }

        ins, outs = mapping.get(op.task_id, ([], []))
        return OperatorLineage(inputs=ins, outputs=outs)
