connection: "snowflake_se_demo"

include: "../views/*.view.lkml"

explore: f_point_of_sale {
  label: "Plugs Electronics"
  description: "Star schema explore for Plugs Electronics point of sale data."

  join: f_sales {
    type: left_outer
    sql_on: ${f_point_of_sale.order_number} = ${f_sales.order_number} ;;
    relationship: many_to_one
  }

  join: d_product {
    type: left_outer
    sql_on: ${f_point_of_sale.product_key} = ${d_product.product_key} ;;
    relationship: many_to_one
  }

  join: d_store {
    type: left_outer
    sql_on: ${f_sales.store_key} = ${d_store.store_key} ;;
    relationship: many_to_one
  }

  join: d_customer {
    type: left_outer
    sql_on: ${f_sales.cust_key} = ${d_customer.cust_key} ;;
    relationship: many_to_one
  }
}
