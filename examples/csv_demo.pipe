@batch
read csv "examples/in.csv"
  | filter status == "paid"
  | select id, amount, label = if(amount is null, "n/a", concat("$", amount)), note
  | limit 2
  | write csv "examples/out.csv"
