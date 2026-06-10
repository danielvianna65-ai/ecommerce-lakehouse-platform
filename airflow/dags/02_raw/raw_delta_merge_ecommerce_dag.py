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
TABLES_ECOMMERCE = [
    "pagamentos",
    "pedidos",
    "clientes",
    "produtos",
    "itens_pedido",
    "categorias",
    "estoque",
    "enderecos",
]

# =========================================================
# SPARK CONFIGS
# =========================================================
SPARK_CONF = {
    "spark.executor.instances": "1",
    "spark.executor.memory": "3g",
    "spark.executor.cores": "2",
    "spark.cores.max": "2",
    "spark.driver.memory": "1g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.shuffle.partitions": "4",
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
    dag_id="02_raw_standardization",
    description="Raw layer - padronização, deduplicação e merge incremental em Delta Lake.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
        tags=[
            "raw",
            "reference-data",
            "standardization",
            "delta",
            "merge",
            "spark",
        ],
) as dag:

    raw_tasks = []

    # =====================================================
    # Loop de criação das tasks (1 job por tabela)
    # =====================================================
    for table in TABLES_ECOMMERCE:

        task = SparkSubmitOperator(
            task_id=f"raw_{table}",
            application=f"/opt/spark/jobs/02_raw/raw_{table}.py",
            conn_id="spark_standalone",
            deploy_mode="client",
            name=f"raw-ecommerce-{table}",
            # ==========================
            # Configurações Spark / Delta
            # ==========================
            conf=SPARK_CONF,
            verbose=True,
        )

        raw_tasks.append(task)

    # =====================================================
    # TASK ENRICHMENT
    # =====================================================

    raw_clientes_enrichment = SparkSubmitOperator(
        task_id="raw_clientes_enrichment",
        application="/opt/spark/jobs/02_raw/raw_clientes_enrichment.py",
        conn_id="spark_standalone",
        deploy_mode="client",
        name="raw_clientes_enrichment",
        conf=SPARK_CONF,
        verbose=True,
    )

    raw_tasks.append(raw_clientes_enrichment)

    # ===============================================
    # Trigger próxima DAG (RAW)
    # ===============================================
    trigger_trusted = TriggerDagRunOperator(
        task_id="trigger_trusted_validation",
        trigger_dag_id="03_trusted_validation",
        wait_for_completion=False
    )

    # ===============================================
    # Todas as tabelas precisam terminar
    # ===============================================
    raw_tasks >> trigger_trusted