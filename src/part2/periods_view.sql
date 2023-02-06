SET datestyle TO ISO, DMY;
DROP VIEW IF EXISTS Periods_View CASCADE ;
CREATE VIEW Periods_View
    (
        Customer_ID,
        Group_ID,
        First_Group_Purchase_Date,
        Last_Group_Purchase_Date,
        Group_Purchase,
        Group_Frequency,
        Group_Min_Discount
        )
    AS
SELECT customer_id,
       group_id,
       MIN(transaction_datetime)::timestamp First_Group_Purchase_Date,
       MAX(transaction_datetime)::timestamp Last_Group_Purchase_Date,
       COUNT(*)                  Group_Purchase,
       (((TO_CHAR((MAX(transaction_datetime)::timestamp - MIN(transaction_datetime)::timestamp), 'DD'))::int + 1)*1.0) / count(*)*1.0 as Group_Frequency,
      COALESCE((SELECT MIN(sku_discount / sku_summ)
                                    FROM checks c1
                                             JOIN purchase_history_view ph2 ON ph2.transaction_id = c1.transaction_id
                                    WHERE (sku_discount / sku_summ) > 0
                                      AND ph2.customer_id = t1.customer_id
                                      AND ph2.group_id = t1.group_id),
                                   0)                                AS Group_Minimum_Discount
  FROM (SELECT DISTINCT customer_id, t.transaction_datetime, c.sku_discount, pg.group_id, c.sku_summ
          FROM cards
                JOIN transactions t ON cards.customer_card_id = t.customer_card_id
                 JOIN checks c ON t.transaction_id = c.transaction_id
                  JOIN product_grid pg ON pg.sku_id = c.sku_id) AS t1
 GROUP BY group_id,customer_id
;