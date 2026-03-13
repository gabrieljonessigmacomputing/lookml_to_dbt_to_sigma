#!/bin/bash
set -euo pipefail
# Purpose: Create sample LookML project under looker_files/plugs/ (model + views for Plugs Electronics).
# Usually run by setup.sh; can be run standalone to (re)generate the sample.
# Usage: ./create_lookml.sh
#
# Directories: only create if missing (never remove or replace). Files: create or overwrite.
# NAMING: The sample LookML project is "plugs" — folder looker_files/plugs/, model file plugs.model.lkml.
# Use this name everywhere (CONNECTIONS.md, .env CONNECTION_ID_PLUGS, make_readme, README). Do not rename.

echo "Creating LookML project directories..."

mkdir -p looker_files/plugs/views looker_files/plugs/models

# ==========================================
# 1. MODEL FILE (Explore) — in models/
# ==========================================
echo "Generating Plugs model..."

cat << 'EOF' > looker_files/plugs/models/plugs.model.lkml
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
EOF

# ==========================================
# 2. VIEW FILES
# ==========================================
echo "Generating view files..."

# --- FACT: POINT OF SALE ---
cat << 'EOF' > looker_files/plugs/views/f_point_of_sale.view.lkml
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
EOF

# --- FACT: SALES ---
cat << 'EOF' > looker_files/plugs/views/f_sales.view.lkml
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
EOF

# --- DIM: PRODUCT ---
cat << 'EOF' > looker_files/plugs/views/d_product.view.lkml
view: d_product {
  sql_table_name: RETAIL.PLUGS_ELECTRONICS.D_PRODUCT ;;

  dimension: product_key { type: string primary_key: yes sql: ${TABLE}."Product Key" ;; }
  dimension: product_name { type: string sql: ${TABLE}."Product Name" ;; }
  dimension: product_type { type: string sql: ${TABLE}."Product Type" ;; }
  dimension: product_family { type: string sql: ${TABLE}."Product Family" ;; }
  dimension: product_line { type: string sql: ${TABLE}."Product Line" ;; }
  dimension: product_group { type: string sql: ${TABLE}."Product Group" ;; }
  dimension: product_description { type: string sql: ${TABLE}."Product Description" ;; }
  dimension: sku_number { type: string sql: ${TABLE}."Sku Number" ;; }
  dimension: price { type: number sql: ${TABLE}."Price" ;; }
  dimension: product_status { type: string sql: ${TABLE}."Product Status" ;; }
  dimension: product_notes { type: string sql: ${TABLE}."Product Notes" ;; }
  dimension: proposed_price { type: number sql: ${TABLE}."Proposed Price" ;; }
}
EOF

# --- DIM: STORE ---
cat << 'EOF' > looker_files/plugs/views/d_store.view.lkml
view: d_store {
  sql_table_name: RETAIL.PLUGS_ELECTRONICS.D_STORE ;;

  dimension: store_key { type: string primary_key: yes sql: ${TABLE}."Store Key" ;; }
  dimension: store_name { type: string sql: ${TABLE}."Store Name" ;; }
  dimension: store_address { type: string sql: ${TABLE}."Store Address" ;; }
  dimension: store_city { type: string sql: ${TABLE}."Store City" ;; }
  dimension: store_state { type: string map_layer_name: us_states sql: ${TABLE}."Store State" ;; }
  dimension: store_zip_code { type: zipcode sql: ${TABLE}."Store Zip Code" ;; }
  dimension: store_county { type: string sql: ${TABLE}."Store County" ;; }
  dimension: store_region { type: string sql: ${TABLE}."Store Region" ;; }
  dimension: store_size { type: string sql: ${TABLE}."Store Size" ;; }
  dimension: monthly_rent_cost { type: number sql: ${TABLE}."Monthly Rent Cost" ;; }
  dimension: number_of_employees { type: number sql: ${TABLE}."Number of Employees" ;; }
  dimension: online_ordering { type: string sql: ${TABLE}."Online Ordering" ;; }

  dimension: location {
    type: location
    sql_latitude: ${TABLE}."Latitude" ;;
    sql_longitude: ${TABLE}."Longitude" ;;
  }
}
EOF

# --- DIM: CUSTOMER ---
cat << 'EOF' > looker_files/plugs/views/d_customer.view.lkml
view: d_customer {
  sql_table_name: RETAIL.PLUGS_ELECTRONICS.D_CUSTOMER ;;

  dimension: cust_key { type: string primary_key: yes sql: ${TABLE}."Cust Key" ;; }
  dimension: cust_name { type: string sql: ${TABLE}."Cust Name" ;; }
  dimension: cust_address { type: string sql: ${TABLE}."Cust Address" ;; }
  dimension: cust_city { type: string sql: ${TABLE}."Cust City" ;; }
  dimension: cust_state { type: string sql: ${TABLE}."Cust State" ;; }
  dimension: cust_zip_code { type: zipcode sql: ${TABLE}."Cust Zip Code" ;; }
  dimension: cust_county { type: string sql: ${TABLE}."Cust County" ;; }
  dimension: cust_region { type: string sql: ${TABLE}."Cust Region" ;; }
  dimension: cust_type { type: string sql: ${TABLE}."Cust Type" ;; }
  dimension: cust_gender { type: string sql: ${TABLE}."Cust Gender" ;; }
  dimension: cust_age { type: number sql: ${TABLE}."Cust Age" ;; }
  dimension: age_group { type: string sql: ${TABLE}."Age Group" ;; }
  dimension: civil_status { type: string sql: ${TABLE}."Civil Status" ;; }
  dimension: loyalty_program { type: string sql: ${TABLE}."Loyalty Program" ;; }
}
EOF

echo "Done! LookML files written to looker_files/plugs/ (views/ and models/)."