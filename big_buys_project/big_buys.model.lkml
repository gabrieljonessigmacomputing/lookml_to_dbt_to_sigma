connection: "snowflake_se_demo" # Replace with your actual Looker connection name

include: "*.view.lkml"

explore: big_buys_pos {
  label: "Big Buys POS"
  description: "Explore for Big Buys flattened point of sale data."
}
