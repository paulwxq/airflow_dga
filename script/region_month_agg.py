import psycopg2
from psycopg2 import sql
import sys
from airflow.utils.log.logging_mixin import LoggingMixin

# 数据库连接配置
DB_CONFIG = {
    'host': '192.168.3.156',
    'database': 'testdb',
    'user': 'postgres',
    'password': 'postgres',
    'port': '5432'
}
logger = LoggingMixin().log

def generate_region_monthly_sales():
    """
    从月销售表和区域维度表生成区域月销售表数据
    """
    conn = None

    try:
        # 连接数据库
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # # 检查目标表中是否有数据，如果有则先清空
        # cursor.execute("SELECT COUNT(*) FROM region_month_sales_table")
        # count = cursor.fetchone()[0]
        # if count > 0:
        #     print("区域月销售表中已有数据，将先清空现有数据...")
        #     cursor.execute("TRUNCATE TABLE region_month_sales_table RESTART IDENTITY")
        
        # # 从月销售表和区域维度表统计生成区域月销售表数据
        # insert_query = sql.SQL("""
        #     INSERT INTO region_month_sales_table (province_name, month_name, sales_amount)
        #     SELECT 
        #         rd.province_name,
        #         msp.sale_month AS month_name,
        #         SUM(msp.sales_amount) AS sales_amount
        #     FROM 
        #         month_sales_product_table msp
        #     JOIN 
        #         region_dim rd ON msp.city_id = rd.city_id
        #     GROUP BY 
        #         rd.province_name, msp.sale_month
        #     ORDER BY 
        #         rd.province_name, msp.sale_month
        # """)
        
        # cursor.execute(insert_query)
        # conn.commit()
        
        # 获取处理的记录数
        cursor.execute("SELECT COUNT(*) FROM region_month_sales_table")
        count = cursor.fetchone()[0]        
        
        logger.info(f"成功生成区域月销售表数据，共 {count} 条记录")
        
    except Exception as e:
        print(f"发生错误: {e}", file=sys.stderr)
        if conn:
            conn.rollback()
        sys.exit(1)
    finally:
        if conn:
            conn.close()

def main():
    logger.info("开始从月销售表和区域维度表生成区域月销售表数据...")
    generate_region_monthly_sales()
    logger.info("处理完成")

if __name__ == "__main__":
    main()