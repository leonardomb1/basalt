-- String-heavy: a LIKE over the long comment column.
SELECT count(*) AS n FROM {lineitem} WHERE l_comment LIKE '%special%' OR l_comment LIKE '%final%accounts%';
