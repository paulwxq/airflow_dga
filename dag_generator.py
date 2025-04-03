#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DAG生成器：连接Neo4j，分析表依赖关系，动态生成Airflow DAG

此模块负责:
1. 连接Neo4j数据库
2. 查询和分析表之间的依赖关系
3. 构建依赖图并执行拓扑排序
4. 生成优化的执行计划
5. 创建Airflow DAG定义文件
"""

import os
import time
import logging
from datetime import datetime
from pathlib import Path
from neo4j import GraphDatabase
import networkx as nx

from config import (
    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD,
    SCRIPTS_DIR, AIRFLOW_DAGS_DIR,
    LOG_LEVEL, LOG_FORMAT,
    SCHEDULE_INTERVAL
)

# 配置日志
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT
)
logger = logging.getLogger(__name__)

# DAG模板
DAG_TEMPLATE = """
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
动态生成的ETL流程
生成时间: {timestamp_human}
'''

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {{
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}

dag = DAG(
    'etl_pipeline_{date_suffix}',
    default_args=default_args,
    description='自动生成的ETL流程',
    schedule_interval='{schedule}',
    catchup=False,
    is_paused_upon_creation=False
)

{tasks}

{dependencies}
"""

# 任务模板
TASK_TEMPLATE = """
task_{task_id} = BashOperator(
    task_id='{task_id}',
    bash_command='python {script_path}',
    dag=dag
)
"""


class DAGGenerator:
    """DAG生成器类：负责连接Neo4j，分析依赖关系，生成Airflow DAG"""
    
    def __init__(self):
        """初始化DAG生成器"""
        self.driver = GraphDatabase.driver(
            NEO4J_URI, 
            auth=(NEO4J_USER, NEO4J_PASSWORD)
        )
        self.scripts_dir = Path(SCRIPTS_DIR)
        self.airflow_dags_dir = Path(AIRFLOW_DAGS_DIR)
        
    def close(self):
        """关闭Neo4j连接"""
        self.driver.close()
        
    def get_table_dependencies(self):
        """从Neo4j获取表依赖关系
        
        Returns:
            list: 包含依赖关系信息的字典列表
        """
        with self.driver.session() as session:
            result = session.run("""
            MATCH (source:Table)<-[r:DERIVED_FROM]-(target:Table)
            RETURN source.name AS source_table, 
                   target.name AS target_table, 
                   r.script_name AS script_name,
                   r.schedule AS schedule
            """)
            
            dependencies = [
                {
                    'source_table': record['source_table'],
                    'target_table': record['target_table'],
                    'script_name': record['script_name'],
                    'schedule': record['schedule']
                }
                for record in result
            ]
            
            return dependencies
    
    def build_dependency_graph(self, dependencies):
        """构建依赖图并进行拓扑排序
        
        Args:
            dependencies (list): 依赖关系列表
            
        Returns:
            tuple: (排序后的表列表, 依赖图)
        """
        G = nx.DiGraph()
        
        # 添加所有表作为节点
        all_tables = set()
        for dep in dependencies:
            all_tables.add(dep['source_table'])
            all_tables.add(dep['target_table'])
            
        for table in all_tables:
            G.add_node(table)
            
        # 添加依赖边 (目标表依赖于源表)
        for dep in dependencies:
            G.add_edge(dep['target_table'], dep['source_table'])
            
        # 执行拓扑排序
        try:
            sorted_tables = list(nx.topological_sort(G))
            sorted_tables.reverse()  # 反转，使源表在前
            return sorted_tables, G
        except nx.NetworkXUnfeasible:
            logger.error("错误: 依赖图中存在循环!")
            return None, G
    
    def optimize_execution_plan(self, dependencies, sorted_tables):
        """优化执行计划，避免重复执行脚本
        
        Args:
            dependencies (list): 依赖关系列表
            sorted_tables (list): 拓扑排序后的表列表
            
        Returns:
            list: 优化后的执行步骤列表
        """
        execution_plan = []
        executed_targets = set()
        
        # 按照拓扑排序顺序处理表
        for table in sorted_tables:
            # 找到以这个表为目标表的所有依赖
            target_deps = [dep for dep in dependencies if dep['target_table'] == table]
            
            # 如果这个表已经作为目标被处理过，跳过
            if table in executed_targets or not target_deps:
                continue
                
            # 按脚本名分组，合并使用相同脚本的依赖
            scripts = {}
            for dep in target_deps:
                script_name = dep['script_name']
                if script_name not in scripts:
                    scripts[script_name] = {
                        'script_name': script_name,
                        'target_table': dep['target_table'],
                        'source_tables': [dep['source_table']],
                        'schedule': dep['schedule']
                    }
                else:
                    scripts[script_name]['source_tables'].append(dep['source_table'])
            
            # 将合并后的执行步骤添加到计划中
            for script_info in scripts.values():
                execution_plan.append(script_info)
                
            executed_targets.add(table)
            
        return execution_plan
    
    def generate_airflow_dag(self, execution_plan):
        """生成Airflow DAG文件
        
        Args:
            execution_plan (list): 执行计划
            
        Returns:
            Path: 生成的DAG文件路径
        """
        # 生成唯一的时间戳作为DAG ID的一部分
        timestamp = int(time.time())
        timestamp_human = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        date_suffix = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
        # 确定调度频率 (使用最频繁的那个)
        schedules = [step['schedule'] for step in execution_plan]
        schedule_counts = {}
        for s in schedules:
            schedule_counts[s] = schedule_counts.get(s, 0) + 1
        
        schedule = max(schedule_counts, key=schedule_counts.get)
        if schedule == "DAILY":
            cron_schedule = "0 1 * * *"  # 每天凌晨1点
        elif schedule == "MONTHLY":
            cron_schedule = "0 0 1 * *"  # 每月1号零点
        else:
            cron_schedule = SCHEDULE_INTERVAL  # 使用默认配置
            
        # 生成任务定义
        tasks = []
        task_ids = {}
        script_to_tables = {}  # 记录每个脚本对应的表
        
        for i, step in enumerate(execution_plan):
            task_id = f"process_{step['target_table']}_{i}"
            task_ids[step['script_name']] = task_id
            script_to_tables[step['script_name']] = {
                'target': step['target_table'],
                'sources': step['source_tables']
            }
            
            script_path = os.path.join(SCRIPTS_DIR, step['script_name'])
            
            task = TASK_TEMPLATE.format(
                task_id=task_id,
                script_path=script_path
            )
            tasks.append(task)
            
        # 生成任务依赖关系
        dependencies = []
        # 遍历每个脚本的依赖关系
        for script_name, tables in script_to_tables.items():
            target_task = task_ids[script_name]
            # 查找该目标表依赖的源表对应的任务
            for source_table in tables['sources']:
                # 找到生成这个源表的脚本（如果有）
                for other_script, other_tables in script_to_tables.items():
                    if other_tables['target'] == source_table:
                        source_task = task_ids[other_script]
                        dependencies.append(f"task_{source_task} >> task_{target_task}")
                        
        # 生成完整的DAG文件内容
        dag_content = DAG_TEMPLATE.format(
            timestamp_human=timestamp_human,
            date_suffix=date_suffix,
            schedule=cron_schedule,
            tasks="".join(tasks),
            dependencies="\n".join(dependencies)
        )
        
        # 写入到Airflow DAG目录
        dag_file_path = self.airflow_dags_dir / f"dynamic_etl_dag_{date_suffix}.py"
        with open(dag_file_path, 'w') as f:
            f.write(dag_content)
            
        logger.info(f"生成DAG文件: {dag_file_path}")
        return dag_file_path
    
    def run(self):
        """执行整个DAG生成流程"""
        try:
            # 1. 获取依赖关系
            dependencies = self.get_table_dependencies()
            if not dependencies:
                logger.warning("Neo4j中未找到依赖关系。")
                return
                
            logger.info(f"查询到 {len(dependencies)} 个依赖关系")
            
            # 2. 构建依赖图并排序
            sorted_tables, graph = self.build_dependency_graph(dependencies)
            if not sorted_tables:
                return
                
            logger.info(f"拓扑排序结果: {sorted_tables}")
            
            # 3. 优化执行计划
            execution_plan = self.optimize_execution_plan(dependencies, sorted_tables)
            logger.info(f"生成执行计划，共 {len(execution_plan)} 个步骤")
            
            # 4. 生成Airflow DAG
            dag_file = self.generate_airflow_dag(execution_plan)
            logger.info(f"成功生成DAG文件: {dag_file}")
            
        except Exception as e:
            logger.error(f"DAG生成过程中出错: {str(e)}", exc_info=True)
        finally:
            self.close()


if __name__ == "__main__":
    """直接运行此模块时，执行DAG生成"""
    logger.info("开始运行DAG生成器")
    generator = DAGGenerator()
    generator.run()
    logger.info("DAG生成器运行完成")