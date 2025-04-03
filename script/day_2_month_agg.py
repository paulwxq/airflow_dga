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

def generate_monthly_sales():
    """
    从日销售表统计生成月销售表数据
    """
    conn = None
    try:
        # 连接数据库
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # # 检查月表中是否有数据，如果有则先清空
        # cursor.execute("SELECT COUNT(*) FROM month_sales_product_table")
        # count = cursor.fetchone()[0]
        # if count > 0:
        #     print("月表中已有数据，将先清空现有数据...")
        #     cursor.execute("TRUNCATE TABLE month_sales_product_table RESTART IDENTITY")
        
        # # 从日表统计生成月表数据
        # insert_query = sql.SQL("""
        #     INSERT INTO month_sales_product_table (sale_month, city_id, product_id, sales_amount)
        #     SELECT 
        #         TO_CHAR(sale_date, 'YYYY-MM') AS sale_month,
        #         city_id,
        #         product_id,
        #         SUM(sales_amount) AS sales_amount
        #     FROM 
        #         day_sales_product_table
        #     GROUP BY 
        #         TO_CHAR(sale_date, 'YYYY-MM'), city_id, product_id
        #     ORDER BY 
        #         sale_month, city_id, product_id
        # """)
        
        # cursor.execute(insert_query)
        # conn.commit()
        
        # 获取处理的记录数
        cursor.execute("SELECT COUNT(*) FROM month_sales_product_table")
        count = cursor.fetchone()[0]
        
        logger.info(f"成功生成月表数据，共 {count} 条记录")
        
    except Exception as e:
        logger.info(f"发生错误: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def main():
    logger.info("开始从日表生成月表数据...")
    generate_monthly_sales()
    logger.info("处理完成")

if __name__ == "__main__":
    main()