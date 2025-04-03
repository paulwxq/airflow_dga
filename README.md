# Neo4j ETL 调度器

该项目实现了一个基于Neo4j表依赖关系的动态ETL调度系统。它能够自动分析Neo4j中定义的表依赖关系，并生成优化的Airflow DAG执行计划。

## 功能特点

- 自动连接Neo4j数据库并分析表依赖关系
- 通过拓扑排序确定正确的执行顺序
- 优化执行计划，避免脚本重复执行
- 自动生成Airflow DAG定义文件
- 定期更新执行计划，适应Neo4j中依赖关系的变化

## 项目结构

```
neo4j_etl_scheduler/
│
├── src/
│   ├── __init__.py          # 包初始化文件
│   ├── dag_generator.py     # DAG生成器核心逻辑
│   └── config.py            # 配置信息
│
├── dags/
│   └── dag_generator_dag.py # Airflow DAG定义
│
├── requirements.txt         # 依赖包列表
├── README.md                # 项目说明
└── .gitignore               # Git忽略文件
```

## 脚本的调用关系
这些脚本的调用关系确实是：
1. dag_generator_dag.py 是一个固定的Airflow DAG文件，它会被放在~/airflow/dags/目录下
2. 这个DAG会按照设定的时间计划（例如每天午夜）自动运行
3. 当它运行时，会调用dag_generator.py中的逻辑
4. dag_generator.py会连接Neo4j，分析表依赖关系
5. 然后它会生成一个新的动态DAG文件，名称类似dynamic_etl_dag_1712345678.py（带有时间戳后缀）
6. 这个生成的动态DAG文件会被保存到~/airflow/dags/目录下
7. Airflow会自动检测到新的DAG文件并加载它
8. 这个动态生成的DAG包含了根据Neo4j依赖关系优化后的ETL执行计划


## 安装配置

1. 克隆项目

```bash
git clone <仓库URL>
cd neo4j_etl_scheduler
```

2. 安装依赖

```bash
pip install -r requirements.txt
```

3. 配置

编辑 `src/config.py` 文件，设置Neo4j连接信息和其他配置参数。

4. 部署到Airflow

确保项目目录可被Airflow访问：

```bash
# 假设您的Airflow在Docker容器中运行
docker cp neo4j_etl_scheduler <container_id>:/opt/airflow/
```

## 使用方法

1. 在Neo4j中定义表依赖关系

```cypher
MATCH (d:Table {name: "day_sales_product_table"})
MATCH (m:Table {name: "month_sales_product_table"})
CREATE (m)-[r:DERIVED_FROM { 
    script_name: "day_2_month_agg.py", 
    schedule: "DAILY" 
}]->(d)
RETURN r
```

2. 放置您的ETL脚本

将ETL脚本放在配置的`SCRIPTS_DIR`目录中，脚本名称应与Neo4j关系中定义的`script_name`一致。

3. 启用DAG生成器

在Airflow界面中启用`etl_dag_generator` DAG。它将按计划自动运行并生成动态ETL DAG。

4. 观察生成的DAG

生成的DAG将自动出现在Airflow界面中，名称类似于`etl_pipeline_<timestamp>`。

## 依赖关系

- Python 3.12+
- Neo4j 5.x
- Apache Airflow 2.7+
- NetworkX 3.2+

## 注意事项

- 确保Neo4j中没有循环依赖，否则拓扑排序将失败
- 所有ETL脚本应该是自包含的，不需要传入额外参数
- 生成的DAG文件将保留最新的5个版本，旧版本会被自动清理