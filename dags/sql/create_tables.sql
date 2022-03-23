CREATE TABLE IF NOT EXISTS public.staging_articles
(
    article_id                  VARCHAR(255) UNIQUE NOT NULL,
    product_code                VARCHAR(255)        NOT NULL,
    prod_name                   VARCHAR(255)        NOT NULL,
    product_type_no             VARCHAR(255)        NOT NULL,
    product_type_name           VARCHAR(255)        NOT NULL,
    product_group_name          VARCHAR(255)        NOT NULL,
    graphical_appearance_no     VARCHAR(255)        NOT NULL,
    graphical_appearance_name   VARCHAR(255)        NOT NULL,
    colour_group_code           VARCHAR(255)        NOT NULL,
    color_group_name            VARCHAR(255)        NOT NULL,
    perceived_colour_value_id   VARCHAR(255)        NOT NULL,
    perceived_color_value_name  VARCHAR(255)        NOT NULL,
    perceived_colour_master_id  VARCHAR(255)        NOT NULL,
    perceived_color_master_name VARCHAR(255)        NOT NULL,
    department_no               VARCHAR(255)        NOT NULL,
    department_name             VARCHAR(255)        NOT NULL,
    index_code                  VARCHAR(255)        NOT NULL,
    index_name                  VARCHAR(255)        NOT NULL,
    index_group_no              VARCHAR(255)        NOT NULL,
    index_group_name            VARCHAR(255)        NOT NULL,
    section_no                  VARCHAR(255)        NOT NULL,
    section_name                VARCHAR(255)        NOT NULL,
    garment_group_no            VARCHAR(255)        NOT NULL,
    garment_group_name          VARCHAR(255)        NOT NULL,
    detail_desc                 VARCHAR(2500)
);

CREATE TABLE IF NOT EXISTS public.staging_customers
(
    customer_id             VARCHAR(255) UNIQUE NOT NULL,
    FN                      FLOAT,
    Active                  FLOAT,
    club_member_status      VARCHAR(255),
    fashions_news_frequency VARCHAR(255),
    age                     FLOAT,
    postal_code             VARCHAR(255)        NOT NULL
);

CREATE TABLE IF NOT EXISTS public.staging_transactions
(
    t_dat            DATE            NOT NULL,
    customer_id      VARCHAR(255)    NOT NULL,
    article_id       VARCHAR(255)    NOT NULL,
    price            DECIMAL(19, 18) NOT NULL,
    sales_channel_id SMALLINT        NOT NULL
);

CREATE TABLE IF NOT EXISTS public.staging_images
(
    article_id INTEGER UNIQUE NOT NULL
);


CREATE TABLE IF NOT EXISTS public.dim_articles
(
    article_id                  INTEGER UNIQUE        NOT NULL,
    product_name                VARCHAR(255)          NOT NULL,
    product_type_name           VARCHAR(255)          NOT NULL,
    product_group_name          VARCHAR(255)          NOT NULL,
    graphical_appearance_name   VARCHAR(255)          NOT NULL,
    color_group_name            VARCHAR(255)          NOT NULL,
    perceived_color_value_name  VARCHAR(255)          NOT NULL,
    perceived_color_master_name VARCHAR(255)          NOT NULL,
    department_name             VARCHAR(255)          NOT NULL,
    index_name                  VARCHAR(255)          NOT NULL,
    index_group_name            VARCHAR(255)          NOT NULL,
    section_name                VARCHAR(255)          NOT NULL,
    garment_group_name          VARCHAR(255)          NOT NULL,
    detail_desc                 VARCHAR(2500),
    has_image                   BOOLEAN DEFAULT FALSE NOT NULL
);

CREATE TABLE IF NOT EXISTS public.dim_customers
(
    customer_id                  VARCHAR(255) UNIQUE NOT NULL,
    fn                           BOOLEAN,
    is_active                    BOOLEAN,
    is_active_club_member_status BOOLEAN,
    fashions_news_frequency      VARCHAR(255),
    age                          INTEGER,
    postal_code                  VARCHAR(255)        NOT NULL
);

CREATE TABLE IF NOT EXISTS public.fact_transactions
(
    transaction_id   VARCHAR(255)    NOT NULL,
    date_of_purchase DATE            NOT NULL,
    customer_id      VARCHAR(255)    NOT NULL,
    article_id       INTEGER         NOT NULL,
    price            DECIMAL(19, 18) NOT NULL,
    sales_channel_id SMALLINT
);