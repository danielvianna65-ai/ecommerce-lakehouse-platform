# =====================================================
# IMPORTS
# =====================================================
import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta

# =========================================================
# Tabelas do domínio ecommerce
# =========================================================
TRUSTED_JOBS = [
    "clientes",
    "categorias",
    "produtos",
    "pedidos",
    "itens_pedido",
    "estoque",
    "pagamentos",
    "enderecos",
]

# =========================================================
# SPARK CONFIGS
# =========================================================
SPARK_CONF = {
    "spark.executor.instances": "2",
    "spark.executor.memory": "2g",
    "spark.executor.cores": "1",
    "spark.cores.max": "2",
    "spark.driver.memory": "1g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.shuffle.partitions": "2",
    "spark.sql.shuffle.partitions": "2",
    "spark.sql.shuffle.partitions": "2",
    "spark.hadoop.dfs.replication": "1",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.jars": (
        "/opt/spark/external-jars/delta-spark_2.12-3.2.0.jar,"
        "/opt/spark/external-jars/delta-storage-3.2.0.jar"
    )
}

# =========================================================
# Default args
# =========================================================
DEFAULT_ARGS = {
    "owner": "DlV",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# =========================================================
# DAG
# =========================================================
with DAG(
    dag_id="03_trusted_validation",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    description="Trusted layer - validação e garantia da qualidade dos dados.",
    schedule_interval=None,
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
    default_args=DEFAULT_ARGS ,
        tags=[
            "trusted",
            "reference-data",
            "validation",
            "data-quality",
            "spark",
        ],
) as dag:

    trusted_tasks = []

    # =====================================================
    # Loop de criação das tasks (1 job por tabela)
    # =====================================================
    for tabela in TRUSTED_JOBS:
        task = SparkSubmitOperator(
            task_id=f"trusted_{tabela}",
            application=f"/opt/spark/jobs/03_trusted/trusted_{tabela}.py",
            conn_id="spark_standalone",
            deploy_mode="client",
            name=f"trusted-ecommerce-{tabela}",
            # ==========================
            # Configurações Spark / Delta
            # ==========================
            conf=SPARK_CONF,
            verbose=True,
        )

        trusted_tasks.append(task)

    # =====================================================
    # TASK ENRICHMENT
    # =====================================================

    trusted_clientes_enrichment = SparkSubmitOperator(
        task_id="trusted_clientes_enrichment",
        application="/opt/spark/jobs/03_trusted/trusted_clientes_enrichment.py",
        conn_id="spark_standalone",
        deploy_mode="client",
        name="trusted_clientes_enrichment",
        conf=SPARK_CONF,
        verbose=True,
    )

    trusted_tasks.append(trusted_clientes_enrichment)

    # ===============================================
    # Trigger próxima DAG (Refined)
    # ===============================================
    trigger_refined = TriggerDagRunOperator(
        task_id="trigger_refined_dimensional_modeling",
        trigger_dag_id="04_refined_dimensional_modeling",
        wait_for_completion=False
    )

    # Todas as tabelas precisam terminar
    trusted_tasks >> trigger_refined