-- PIVOT: each order priority's total price per order status, as one column per status.
SELECT * FROM (SELECT o_orderpriority, o_orderstatus, o_totalprice FROM {orders}) t
PIVOT (sum(o_totalprice) FOR o_orderstatus IN ('F', 'O', 'P'))
ORDER BY o_orderpriority;
