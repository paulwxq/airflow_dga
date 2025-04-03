#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 此文件放在与config.py同一目录下，不要放在dags目录下
# 此方法用于手动触发指定表的工作流

import os
import sys
import logging
import networkx as nx
from datetime import datetime
from neo4j import GraphDatabase

from config import (
    NEO4J_URI, 
    NEO4J_USER, 
    NEO4J_PASSWORD, 
    SCRIPTS_DIR,
    LOG_LEVEL,
    LOG_FORMAT
)


# 配置日志
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT
)
logger = logging.getLogger(__name__)

class WorkflowOrchestrator:
    def __init__(self, neo4j_uri=NEO4J_URI, 
                 neo4j_user=NEO4J_USER, 
                 neo4j_password=NEO4J_PASSWORD, 
                 scripts_base_path=SCRIPTS_DIR):
        """
        初始化工作流编排器
        
        :param neo4j_uri: Neo4j连接地址
        :param neo4j_user: Neo4j用户名
        :param neo4j_password: Neo4j密码
        :param scripts_base_path: 脚本存储的基础路径
        """
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.scripts_base_path = scripts_base_path

    def get_workflow_dependencies(self, start_node_name):
        """
        获取从指定节点开始的工作流依赖关系
        
        :param start_node_name: 起始节点名称
        :return: 依赖图和脚本信息
        """
        with self.driver.session() as session:
            # Cypher查询获取节点及其依赖
            query = """
            MATCH (start {name: $start_node_name})
            CALL apoc.path.subgraphNodes(start, {
                relationshipFilter: 'DERIVED_FROM>',
                labelFilter: '+Table'
            }) YIELD node
            WITH start, collect(distinct node) as nodes
            UNWIND nodes as n
            OPTIONAL MATCH (n)<-[r:DERIVED_FROM]-(parent)
            RETURN n.name as node_name, 
                   parent.name as parent_name, 
                   r.script_name as script_name,
                   r.schedule as schedule
            """
            
            result = session.run(query, start_node_name=start_node_name)
            
            # 构建依赖图
            G = nx.DiGraph()
            script_map = {}
            schedule_map = {}
            
            for record in result:
                node = record['node_name']
                parent = record['parent_name']
                script = record['script_name']
                schedule = record['schedule']
                
                G.add_node(node)
                if parent:
                    G.add_edge(parent, node)
                
                if script:
                    script_map[node] = script
                if schedule:
                    schedule_map[node] = schedule
            
            return G, script_map, schedule_map

    def execute_workflow(self, start_node_name):
        """
        执行从指定节点开始的工作流
        
        :param start_node_name: 起始节点名称
        :return: 执行结果
        """
        # 获取依赖图和脚本映射
        dependency_graph, script_map, schedule_map = self.get_workflow_dependencies(start_node_name)
        
        # 拓扑排序确定执行顺序
        execution_order = list(nx.topological_sort(dependency_graph))
        
        # 执行结果记录
        execution_results = []
        
        # 按顺序执行脚本
        for node in execution_order:
            if node in script_map:
                script_path = os.path.join(self.scripts_base_path, script_map[node])
                
                try:
                    # 执行脚本
                    logger.info(f"执行节点 {node} 的脚本: {script_path}")
                    
                    # 创建一个局部命名空间，传入必要的上下文信息
                    exec_context = {
                        'node_name': node,
                        'script_name': script_map[node],
                        'schedule': schedule_map.get(node, 'N/A')
                    }
                    
                    exec(open(script_path).read(), exec_context)
                    
                    execution_results.append({
                        'node': node,
                        'script': script_map[node],
                        'schedule': schedule_map.get(node, 'N/A'),
                        'status': 'success'
                    })
                except Exception as e:
                    logger.error(f"节点 {node} 执行失败: {e}")
                    execution_results.append({
                        'node': node,
                        'script': script_map[node],
                        'schedule': schedule_map.get(node, 'N/A'),
                        'status': 'failed',
                        'error': str(e)
                    })
                    # 如果需要在失败时中断整个工作流，可以取消注释下面一行
                    # break
        
        return execution_results

    def close(self):
        """关闭数据库连接"""
        if self.driver:
            self.driver.close()

def main():
    """
    命令行入口
    使用方式: python workflow_orchestrator.py <start_node_name>
    """
    if len(sys.argv) < 2:
        logger.error("请提供起始节点名称")
        sys.exit(1)
    
    start_node_name = sys.argv[1]
    
    # 创建编排器
    orchestrator = WorkflowOrchestrator()
    
    try:
        # 执行工作流
        results = orchestrator.execute_workflow(start_node_name)
        
        # 输出执行结果
        logger.info("工作流执行结果:")
        for result in results:
            logger.info(f"节点: {result['node']}, 脚本: {result['script']}, 状态: {result['status']}")
    
    except Exception as e:
        logger.error(f"工作流执行失败: {e}")
    
    finally:
        orchestrator.close()

if __name__ == "__main__":
    main()