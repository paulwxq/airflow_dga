import psycopg2
from psycopg2 import sql
import sys
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

def generate_catalog_monthly_sales():
    """
    从月销售表和产品目录维度表生成分类月销售表数据
    """
    conn = None
    try:
        # 连接数据库
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 记录开始时间
        start_time = datetime.now()
        # print(f"开始处理数据，时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # # 检查目标表中是否有数据，如果有则先清空
        # cursor.execute("SELECT COUNT(*) FROM catalog_month_sales_table")
        # count = cursor.fetchone()[0]
        # if count > 0:
        #     print("分类月销售表中已有数据，将先清空现有数据...")
        #     cursor.execute("TRUNCATE TABLE catalog_month_sales_table RESTART IDENTITY")
        #     conn.commit()
        
        # # 从月销售表和产品目录维度表统计生成分类月销售表数据
        # insert_query = sql.SQL("""
        #     INSERT INTO catalog_month_sales_table (product_type_name, month_name, sales_amount)
        #     SELECT 
        #         cd.product_type_name,
        #         msp.sale_month AS month_name,
        #         SUM(msp.sales_amount) AS sales_amount
        #     FROM 
        #         month_sales_product_table msp
        #     JOIN 
        #         catalog_dim cd ON msp.product_id = cd.product_id
        #     GROUP BY 
        #         cd.product_type_name, msp.sale_month
        #     ORDER BY 
        #         cd.product_type_name, msp.sale_month
        # """)
        
        # print("正在生成分类月销售数据...")
        # cursor.execute(insert_query)
        # conn.commit()
        
        # 获取处理的记录数
        cursor.execute("SELECT COUNT(*) FROM catalog_month_sales_table")
        count = cursor.fetchone()[0]
        
        # 记录结束时间
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"数据处理完成，时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"成功生成分类月销售表数据，共 {count} 条记录")
        logger.info(f"总耗时: {duration.total_seconds():.2f} 秒")
        
    except psycopg2.Error as e:
        logger.info(f"数据库错误发生: {e.pgerror}", file=sys.stderr)
        if conn:
            conn.rollback()
        sys.exit(1)
    except Exception as e:
        logger.info(f"发生错误: {str(e)}", file=sys.stderr)
        if conn:
            conn.rollback()
        sys.exit(1)
    finally:
        if conn:
            conn.close()

def main():
    logger.info("="*50)
    logger.info("开始从月销售表和产品目录维度表生成分类月销售表数据")
    logger.info("="*50)
    
    generate_catalog_monthly_sales()
    
    logger.info("="*50)
    logger.info("处理完成")
    logger.info("="*50)

if __name__ == "__main__":
    main()