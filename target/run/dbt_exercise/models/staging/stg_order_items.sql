

  create or replace view `data-bootcamp-477503`.`dbt_hayashi`.`stg_order_items`
  OPTIONS()
  as select * from `bigquery-public-data`.`thelook_ecommerce`.`order_items`;

