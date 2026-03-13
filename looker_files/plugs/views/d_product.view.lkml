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
