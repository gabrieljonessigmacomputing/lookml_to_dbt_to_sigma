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
