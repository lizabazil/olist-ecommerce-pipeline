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
                INNER JOIN olist_processed_db.order_payments op 
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

    run_and_save_query(
        sql_query="""
                    WITH first_stats AS (

                    SELECT o.purchase_year, o.purchase_month,
                            ROUND(AVG(op.payment_value), 2) AS avg_order_value
                    FROM olist_processed_db.order_payments op 
                    INNER JOIN olist_processed_db.orders o 
                    ON o.order_id = op.order_id
                    GROUP BY o.purchase_year, o.purchase_month
                    )

                    SELECT purchase_year, 
                            purchase_month,
                            avg_order_value,
                            LAG(avg_order_value, 1, NULL) OVER(ORDER BY purchase_year, purchase_month) AS prev_month_avg,
                            ROUND((avg_order_value - LAG(avg_order_value, 1, NULL) OVER(ORDER BY purchase_year, purchase_month)) / LAG(avg_order_value, 1, NULL) OVER(ORDER BY purchase_year, purchase_month) * 100, 2) AS mom_change_pct
                    FROM first_stats
                    """,
                    output_folder="average_order_value_over_time"
    )

    run_and_save_query(
        sql_query="""
                    SELECT op.payment_installments,
                    COUNT(*) AS total_orders,
                    ROUND(SUM(payment_value), 2) AS total_revenue,
                    ROUND(AVG(payment_value), 2) AS avg_order_value
            FROM olist_processed_db.order_payments op 
            INNER JOIN olist_processed_db.orders o 
            ON o.order_id = op.order_id
            GROUP BY op.payment_installments
            ORDER BY op.payment_installments
                    """,
                    output_folder="revenue_by_installments"
    )

        # customer segmentation by their total spend: high_value > 1000; mid_value > 500; else low_value
    run_and_save_query(
        sql_query="""
                WITH customer_metrics AS (
                        SELECT o.customer_id, SUM(op.payment_value) AS total_spent,
                                COUNT(DISTINCT o.order_id) AS total_orders
                        FROM olist_processed_db.customers c
                        INNER JOIN olist_processed_db.orders o ON c.customer_id = o.customer_id
                        INNER JOIN olist_processed_db.order_payments op ON o.order_id = op.order_id
                        GROUP BY o.customer_id
                        ),
                
                segmented_by_total_spent AS (    
                SELECT *,
                CASE WHEN total_spent > 1000 THEN 'high_value'
                        WHEN total_spent > 500 THEN 'mid_value'
                        ELSE 'low_value'
                        END AS customer_segment
                FROM customer_metrics
                )

                SELECT customer_segment, COUNT(customer_segment) AS total_customers,
                        ROUND(AVG(total_spent), 2) AS avg_lifetime_revenue,
                        AVG(total_orders) AS avg_orders
                FROM segmented_by_total_spent
                GROUP BY customer_segment
                """,
                output_folder="customer_lifetime_value_segmentation"
        )   
    run_and_save_query(
                    sql_query="""
                                WITH customer_first_purchase AS (
                        SELECT c.customer_unique_id,
                        MIN(purchase_year) AS first_purchase_year,
                        MIN(purchase_month) AS first_purchase_month
                        FROM olist_processed_db.orders o
                        INNER JOIN olist_processed_db.customers c ON o.customer_id = c.customer_id
        
                        GROUP BY c.customer_unique_id
                        ),
                        classified_orders AS (
                                        SELECT o.purchase_year,
                                        o.purchase_month, 
                                        c.customer_unique_id,
                                        CASE WHEN o.purchase_year = cfp.first_purchase_year 
                                        AND o.purchase_month = cfp.first_purchase_month 
                                        THEN 'new'
                                        ELSE 'returning'
                                        END AS new_or_returning_customer
                                        FROM olist_processed_db.orders o
                                        INNER JOIN olist_processed_db.customers c ON o.customer_id = c.customer_id
                                        INNER JOIN customer_first_purchase cfp ON cfp.customer_unique_id = c.customer_unique_id
                                        )
                                        
                        SELECT purchase_year, purchase_month, 
                                COUNT(DISTINCT CASE WHEN new_or_returning_customer = 'new' THEN customer_unique_id END) AS new_customers,
                                COUNT(DISTINCT CASE WHEN new_or_returning_customer = 'returning' THEN customer_unique_id END) AS returning_customers
                        FROM classified_orders
                        GROUP BY purchase_year, purchase_month
                        ORDER BY purchase_year, purchase_month
                                """,
                                output_folder="customer_acquistion_by_month"
                )

        # shows which states drive the most customers and revenue
    run_and_save_query(
        sql_query="""
                        WITH customer_states AS (

                        SELECT c.customer_state, 
                                COUNT(c.customer_unique_id) AS total_customers,
                                ROUND(SUM(op.payment_value), 2) AS total_revenue,
                                ROUND(AVG(op.payment_value), 2) AS avg_order_value
                        FROM olist_processed_db.customers c 
                        INNER JOIN olist_processed_db.orders o ON c.customer_id = o.customer_id
                        INNER JOIN olist_processed_db.order_payments op ON op.order_id = o.order_id
                        GROUP BY c.customer_state
                        )

                        SELECT *,
                                ROUND(total_customers * 100.0 / SUM(total_customers) OVER(), 2) AS pct_of_customers
                        FROM customer_states
                        ORDER BY pct_of_customers DESC
                        """, 
                        output_folder="geographic_customer_distribution"
    )

    run_and_save_query(
        sql_query="""
                        WITH total_orders_per_status AS (
                        SELECT order_status, 
                                COUNT(*) AS total_orders 
                        FROM olist_processed_db.orders 
                        GROUP BY order_status
                        )

                        SELECT *, 
                                ROUND(total_orders * 100.0 / SUM(total_orders) OVER(), 2) AS pct_of_total
                        FROM total_orders_per_status
                        ORDER BY total_orders DESC
                """, 
                output_folder="order_status_funnel"
    )

        # the orders with unknown estimated or actual delivery date were removed from this analysis
    run_and_save_query(
        sql_query="""
                        WITH date_diffs AS (
                                SELECT order_delivered_customer_date, order_estimated_delivery_date,
                                DATE_DIFF('day', CAST(order_estimated_delivery_date AS DATE), 
                                CAST(order_delivered_customer_date AS DATE)) AS days_diff
                                FROM olist_processed_db.orders
                                WHERE order_delivered_customer_date IS NOT NULL AND order_estimated_delivery_date IS NOT NULL
                        ), 
                        segmented_deliveries AS 
                        (SELECT *, 
                                CASE WHEN days_diff < 0 THEN 'early'
                                WHEN days_diff = 0 THEN 'on_time'
                                WHEN days_diff <= 3 THEN '1_to_3_days_late'
                                WHEN days_diff <= 7 THEN '4_to_7_days_late'
                                ELSE '7_plus_days_late'
                                END AS delivery_performance
                                
                        FROM date_diffs
                        ), 

                        grouped_del_perf AS (
                        SELECT delivery_performance, 
                                COUNT(*) AS total_orders 
                        FROM segmented_deliveries
                        GROUP BY delivery_performance
                        )

                        SELECT * , 
                                ROUND(total_orders * 100.0 / SUM(total_orders) OVER(), 2) AS pct_of_orders
                        FROM grouped_del_perf
                        ORDER BY CASE delivery_performance 
                                WHEN 'early' THEN 1
                                WHEN 'on_time' THEN 2 
                                WHEN '1_to_3_days_late' THEN 3 
                                WHEN '4_to_7_days_late' THEN 4 
                                WHEN '7_plus_days_late' THEN 5
                                END 
                        """, 
                        output_folder="estimated_vs_actual_delivery_performance"
    )
    

        # TODO: filter out orders with status 'unavailable' or 'cancelled' to get the correct calculations about revenue

    return None
