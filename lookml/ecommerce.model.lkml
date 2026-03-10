connection: "your_connection_name"

include: "*.view.lkml"

explore: orders {
  from: orders
  label: "Orders"
  description: "Orders explore generated from Sigma pipeline test"
}

explore: customers {
  from: customers
  label: "Customers"
  description: "Customers explore generated from Sigma pipeline test"
}
