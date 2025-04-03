# 此文件放在应用端(客户端)，用于远程调用Airflow的API
# 方法2：手动触发指定表的工作流


import requests
import logging
from datetime import datetime


class AirflowDagTriggerClient:
    def __init__(self, airflow_url, username, password):
        """
        初始化Airflow DAG触发客户端
        
        :param airflow_url: Airflow服务器的基础URL
        :param username: Airflow用户名
        :param password: Airflow密码
        """
        self.base_url = airflow_url.rstrip('/')
        self.username = username
        self.password = password
        self.logger = logging.getLogger(__name__)
        
        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    def trigger_workflow(self, node_name):
        """
        触发工作流
        
        :param node_name: 起始节点名称
        :return: DAG运行信息
        """
        try:
            # DAG触发端点
            endpoint = f"{self.base_url}/api/v1/dags/dynamic_workflow_trigger/dagRuns"
            
            # 请求体
            payload = {
                "conf": {
                    "node_name": node_name
                },
                "dag_run_id": f"manual__{node_name}__{datetime.now().isoformat()}"
            }
            
            # 发送POST请求
            response = requests.post(
                endpoint, 
                json=payload,
                auth=(self.username, self.password),
                headers={
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                }
            )
            
            # 检查响应
            response.raise_for_status()
            
            # 解析响应
            dag_run_info = response.json()
            
            self.logger.info(f"成功触发工作流，节点: {node_name}")
            
            return {
                'status': 'success',
                'dag_run_id': dag_run_info.get('dag_run_id'),
                'state': dag_run_info.get('state')
            }
        
        except requests.RequestException as e:
            self.logger.error(f"触发工作流失败: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }

# 使用示例
def main():
    client = AirflowDagTriggerClient(
        airflow_url='http://localhost:8080',
        username='admin',
        password='admin'
    )
    
    result = client.trigger_workflow('region_month_sales_table')
    print(result)

if __name__ == "__main__":
    main()