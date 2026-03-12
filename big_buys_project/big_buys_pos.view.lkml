view: big_buys_pos {
  sql_table_name: SE_DEMO_DB.BIG_BUYS.BIG_BUYS_POS ;;

  # --- DIMENSIONS ---

  dimension: order_number {
    type: string
    primary_key: yes
    sql: ${TABLE}."Order Number" ;;
  }

  dimension_group: order {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    datatype: date
    sql: ${TABLE}."Date" ;;
  }

  dimension: sku_number {
    type: string
    sql: ${TABLE}."Sku Number" ;;
  }

  dimension: quantity {
    type: number
    sql: ${TABLE}."Quantity" ;;
  }

  dimension: cost {
    type: number
    sql: ${TABLE}."Cost" ;;
  }

  dimension: price {
    type: number
    sql: ${TABLE}."Price" ;;
  }

  # Product Info
  dimension: product_type { type: string sql: ${TABLE}."Product Type" ;; }
  dimension: product_family { type: string sql: ${TABLE}."Product Family" ;; }
  dimension: product_line { type: string sql: ${TABLE}."Product Line" ;; }
  dimension: brand { type: string sql: ${TABLE}."Brand" ;; }
  dimension: product_name { type: string sql: ${TABLE}."Product Name" ;; }
  dimension: product_tier { type: string sql: ${TABLE}."Product Tier" ;; }
  dimension: merchant_id { type: string sql: ${TABLE}."Merchant Id" ;; }

  # Store Info
  dimension: store_key { type: string sql: ${TABLE}."Store Key" ;; }
  dimension: store_name { type: string sql: ${TABLE}."Store Name" ;; }
  dimension: store_tier { type: string sql: ${TABLE}."Store Tier" ;; }
  dimension: store_region { type: string sql: ${TABLE}."Store Region" ;; }
  dimension: store_state { type: string sql: ${TABLE}."Store State" ;; }
  dimension: store_city { type: string sql: ${TABLE}."Store City" ;; }
  dimension: store_zip_code { type: string sql: ${TABLE}."Store Zip Code" ;; }
  dimension: county_name { type: string sql: ${TABLE}."County Name" ;; }
  dimension: store_latitude { type: number sql: ${TABLE}."Store Latitude" ;; }
  dimension: store_longitude { type: number sql: ${TABLE}."Store Longitude" ;; }
  
  # Customer Info
  dimension: cust_key { type: string sql: ${TABLE}."Cust Key" ;; }
  dimension: customer_name { type: string sql: ${TABLE}."Customer Name" ;; }
  dimension: cust_json { type: string sql: ${TABLE}."Cust Json" ;; }

  # --- MEASURES ---

  measure: order_volume {
    label: "Order Volume"
    type: count_distinct
    sql: ${order_number} ;;
  }

  measure: revenue {
    label: "Revenue"
    type: sum
    sql: ${quantity} * ${price} ;;
    value_format_name: usd
  }

  measure: cogs {
    label: "COGS"
    type: sum
    sql: ${quantity} * ${cost} ;;
    value_format_name: usd
  }

  measure: gross_profit {
    label: "Gross Profit"
    type: number
    sql: ${revenue} - ${cogs} ;;
    value_format_name: usd
  }

  measure: profit_margin {
    label: "Profit Margin"
    description: "Total profit margin"
    type: number
    sql: 1.0 * ${gross_profit} / NULLIF(${revenue}, 0) ;;
    value_format_name: percent_2
  }

  measure: average_units_per_order {
    label: "Average Units per Order"
    type: number
    sql: 1.0 * sum(${quantity}) / NULLIF(${order_volume}, 0) ;;
    value_format_name: decimal_2
  }

  measure: asp {
    label: "ASP"
    description: "Average total sales price for each order"
    type: number
    sql: 1.0 * ${revenue} / NULLIF(${order_volume}, 0) ;;
    value_format_name: usd
  }
}
