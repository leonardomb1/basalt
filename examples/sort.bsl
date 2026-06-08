@batch
read csv "examples/in.csv"
  | filter amount is not null
  | select id, amt = cast(amount as int)
  | sort amt desc
  | write csv "examples/out.csv"
