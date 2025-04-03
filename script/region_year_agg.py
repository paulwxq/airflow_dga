import psycopg2
from psycopg2 import sql
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log

# 数据库连接配置
DB_CONFIG = {
    'host': '192.168.3.156',
    'database': 'testdb',
    'user': 'postgres',
    'password': 'postgres',
    'port': '5432'
}

def generate_region_year_summary():
    """
    从区域月销售表统计生成区域年度销售汇总数据
    """
    conn = None
    try:
        # 连接数据库
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # # 检查年度汇总表中是否有数据，如果有则先清空
        # cursor.execute("SELECT COUNT(*) FROM region_year_sum_table")
        # count = cursor.fetchone()[0]
        # if count > 0:
        #     logger.info("年度汇总表中已有数据，将先清空现有数据...")
        #     cursor.execute("TRUNCATE TABLE region_year_sum_table")
        #     conn.commit()
        
        # # 从月度表统计生成年度汇总数据
        # insert_query = sql.SQL("""
        #     INSERT INTO region_year_sum_table (province_name, year_name, sales_amount)
        #     SELECT 
        #         province_name,
        #         SUBSTRING(month_name, 1, 4) AS year_name,
        #         SUM(sales_amount) AS sales_amount
        #     FROM 
        #         region_month_sales_table
        #     GROUP BY 
        #         province_name, SUBSTRING(month_name, 1, 4)
        #     ORDER BY 
        #         province_name, year_name
        # """)
        
        # cursor.execute(insert_query)
        # conn.commit()
        
        # 获取处理的记录数
        cursor.execute("SELECT COUNT(*) FROM region_year_sum_table")
        count = cursor.fetchone()[0]
        
        logger.info(f"成功生成区域年度汇总数据，共 {count} 条记录")
        
    except Exception as e:
        logger.info(f"发生错误: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def main():
    logger.info("开始从区域月销售表生成年度汇总数据...")
    generate_region_year_summary()
    logger.info("处理完成")

if __name__ == "__main__":
    main() 