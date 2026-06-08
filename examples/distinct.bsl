@batch
read csv "examples/in.csv"
  | select status
  | distinct on status
  | write csv "examples/out.csv"
