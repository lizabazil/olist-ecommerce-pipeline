from utils import run_and_save_query



def run_all_queries():
    run_and_save_query(
        sql_query="""
                WITH monthly_revenue AS 
                (
                SELECT o.purchase_year, 
                    o.purchase_month,
                        ROUND(SUM(op.payment_value), 2) AS monthly_revenue
                FROM olist_processed_db.orders o 
                LEFT JOIN olist_processed_db.order_payments op 
                ON o.order_id = op.order_id
                GROUP BY o.purchase_year, o.purchase_month
                ORDER BY o.purchase_year, o.purchase_month
                )

                SELECT purchase_year, purchase_month, monthly_revenue,
                        ROUND(SUM(monthly_revenue) OVER (ORDER BY purchase_year, purchase_month), 2) AS running_total
                FROM monthly_revenue
                """,
                output_folder="montly_revenue_and_running_total"
    )

    run_and_save_query(
        sql_query="""
                    WITH first_stats AS (

                        SELECT op.payment_type,
                                COUNT(*) AS total_orders,
                                ROUND(SUM(op.payment_value), 2) AS total_revenue,
                                ROUND(AVG(op.payment_value), 2) AS avg_order_value
                        FROM olist_processed_db.order_payments op 
                        INNER JOIN olist_processed_db.orders o 
                        ON o.order_id = op.order_id
                        GROUP BY op.payment_type
                        ORDER BY total_revenue DESC
                        )

                    SELECT *, 
                        ROUND(total_revenue / SUM(total_revenue) OVER () * 100, 2) AS pct_of_total_revenue
                    FROM first_stats
                  """,
                  output_folder="payment_method_breakdown_by_revenue"
    )
    return None
