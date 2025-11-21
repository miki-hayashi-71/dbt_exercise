

  create or replace view `data-bootcamp-477503`.`dbt_hayashi`.`stg__orders`
  OPTIONS()
  as select * from `bigquery-public-data`.`thelook_ecommerce`.`orders`;

