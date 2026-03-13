view: f_sales {
  sql_table_name: RETAIL.PLUGS_ELECTRONICS.F_SALES ;;

  dimension: order_number { type: string sql: ${TABLE}."Order Number" ;; }
  dimension: cust_key { type: string sql: ${TABLE}."Cust Key" ;; }
  dimension: store_key { type: string sql: ${TABLE}."Store Key" ;; }
  dimension: transaction_type { type: string sql: ${TABLE}."Transaction Type" ;; }
  dimension: purchase_method { type: string sql: ${TABLE}."Purchase Method" ;; }

  dimension_group: transaction {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    datatype: date
    sql: ${TABLE}."Date" ;;
  }
}
