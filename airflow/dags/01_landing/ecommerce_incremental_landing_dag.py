# ======================================================
# IMPORTS
# ======================================================
import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta

# =========================================================
# tables ecommerce
# =========================================================
TABLES_ECOMMERCE = [
    "categorias",
    "clientes",
    "enderecos",
    "estoque",
    "itens_pedido",
    "pagamentos",
    "pedidos",
    "produtos",
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
    dag_id="01_landing_ingestion",
        description="""
    Landing layer - ingestão incremental de dados transacionais via JDBC
    e dados de referência utilizados para enriquecimento analítico.
    """,
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    max_active_runs=1,
        tags=[
            "landing",
            "ingestion",
            "reference-data",
            "spark"
        ],
) as dag:

    landing_tasks = []

    # =====================================================
    # Loop tasks (1 job por tabela)
    # =====================================================
    for table in TABLES_ECOMMERCE:

        task = SparkSubmitOperator(
            task_id=f"landing_{table}",
            application="/opt/spark/jobs/01_landing/landing_incremental.py",
            conn_id="spark_standalone",
            deploy_mode="client",
            name=f"landing-ecommerce-{table}",

            # ==========================
            # Args para o job Spark
            # ==========================
            application_args=[
                "--table", table,
                "--landing_base", "hdfs://namenode:8020/data/01_landing/ecommerce",
                "--jdbc_url", "{{ var.value.ecommerce_jdbc_url }}",
                "--jdbc_user", "{{ var.value.ecommerce_jdbc_user }}",
                "--jdbc_password", "{{ var.value.ecommerce_jdbc_password }}",
                "--execution_date",
                "{{ data_interval_start.in_timezone('America/Sao_Paulo').to_date_string() }}"
            ],
            # ==========================
            # Configurações Spark / MySQL
            # ==========================
            conf={
                **SPARK_CONF,
                "spark.jars": "/opt/spark/external-jars/mysql-connector-j-8.3.0.jar",
            },
            verbose=False,
        )

        landing_tasks.append(task)

    # =====================================================
    # TASK ENRICHMENT
    # =====================================================

    landing_clientes_enrichment = SparkSubmitOperator(
        task_id="landing_clientes_enrichment",
        application="/opt/spark/jobs/01_landing/landing_clientes_enrichment.py",
        conn_id="spark_standalone",
        deploy_mode="client",
        name="landing_clientes_enrichment",
        conf=SPARK_CONF,
        verbose=True,
    )

    landing_tasks.append(landing_clientes_enrichment)

    # ===============================================
    # Trigger próxima DAG (RAW)
    # ===============================================
    trigger_raw = TriggerDagRunOperator(
        task_id="trigger_raw_standardization",
        trigger_dag_id="02_raw_standardization",
        wait_for_completion=False
    )

    # ===============================================
    # Todas as tabelas precisam terminar
    # ===============================================
    landing_tasks >> trigger_raw
