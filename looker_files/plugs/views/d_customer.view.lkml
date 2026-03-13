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
