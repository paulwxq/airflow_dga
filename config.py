#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
配置模块: 存储所有配置参数
"""

# Neo4j 连接配置
NEO4J_URI = "bolt://192.168.3.156:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "Passw0rd"  # 请修改为您的实际密码

# 脚本目录配置
SCRIPTS_DIR = "/opt/airflow/scripts"  # ETL脚本存放目录

# Airflow DAG目录
AIRFLOW_DAGS_DIR = "/opt/airflow/dags"  # Airflow DAG目录
# AIRFLOW_DAGS_DIR = "./dags"  # 临时：输出到项目目录下的dags文件夹

# 日志配置
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# 调度配置
SCHEDULE_INTERVAL = "0 1 * * *"  # 每天凌晨1点执行

# DAG配置
DAG_OWNER = "airflow"
DAG_EMAIL = []  # 报警邮件接收地址
DAG_RETRIES = 1
DAG_RETRY_DELAY_MINUTES = 5