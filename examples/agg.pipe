@batch
read csv "examples/in.csv"
  | aggregate n = count(), total = sum(cast(amount as int)) by status
  | write csv "examples/out.csv"
