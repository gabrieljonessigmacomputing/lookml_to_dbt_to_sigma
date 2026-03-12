view: f_point_of_sale {
  sql_table_name: RETAIL.PLUGS_ELECTRONICS.F_POINT_OF_SALE ;;

  dimension: order_number { type: string sql: ${TABLE}."Order Number" ;; }
  dimension: product_key { type: string sql: ${TABLE}."Product Key" ;; }
  dimension: sales_quantity { type: number sql: ${TABLE}."Sales Quantity" ;; }
  dimension: sales_amount { type: number sql: ${TABLE}."Sales Amount" ;; }
  dimension: cost_amount { type: number sql: ${TABLE}."Cost Amount" ;; }

  measure: row_count {
    type: count
    drill_fields: [order_number, product_key]
  }

  measure: total_sales_amount {
    type: sum
    sql: ${sales_amount} * ${sales_quantity} ;;
    value_format_name: usd
  }

  measure: total_cost_amount {
    type: sum
    sql: ${cost_amount} * ${sales_quantity} ;;
    value_format_name: usd
  }

  measure: gross_profit_amount {
    type: number
    sql: ${total_sales_amount} - ${total_cost_amount} ;;
    value_format_name: usd
  }

  measure: gross_profit_margin {
    type: number
    sql: 1.0 * ${gross_profit_amount} / NULLIF(${total_sales_amount}, 0) ;;
    value_format_name: percent_2
  }
}
