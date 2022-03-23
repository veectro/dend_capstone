class SqlQueries:
    articles_table_insert = ("""
    SELECT CONVERT(INTEGER, sa.article_id)                   AS article_id,
       prod_name                                             AS product_name,
       product_type_name,
       product_group_name,
       graphical_appearance_name,
       color_group_name,
       perceived_color_value_name,
       perceived_color_master_name,
       department_name,
       index_name,
       index_group_name,
       section_name,
       garment_group_name,
       detail_desc,
       CASE WHEN si.article_id IS NULL THEN false ELSE true END AS has_image
    FROM staging_articles sa
             LEFT JOIN staging_images si ON CONVERT(INTEGER, sa.article_id) = si.article_id;
    """)

    customers_table_insert = ("""
    SELECT customer_id,
       CASE FN WHEN 1.0 THEN true ELSE false END                      AS fn,
       CASE Active WHEN 1.0 THEN true ELSE false END                  AS is_active,
       CASE club_member_status WHEN 'ACTIVE' THEN true ELSE false END AS is_active_club_member_status,
       fashions_news_frequency,
       age,
       postal_code
    FROM staging_customers;
    """)

    transactions_table_insert = ("""
    SELECT MD5(customer_id || article_id || t_dat)          AS transaction_id,
       t_dat                                                AS date_of_purchase,
       customer_id,
       CAST(article_id AS INTEGER)                          AS article_id,
       sales_channel_id
    FROM staging_transactions;
    """)
