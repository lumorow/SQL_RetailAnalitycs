CREATE OR REPLACE VIEW Purchase_history_view AS
WITH purchases AS (SELECT c.customer_id,
                          t.transaction_id,
                          transaction_datetime,
                          group_id,
                          sku_purchase_price,
                          sku_amount,
                          sku_summ,
                          sku_summ_paid
                   FROM transactions AS t
                            JOIN cards c ON c.customer_card_id = t.customer_card_id
                            JOIN personal_information pi ON c.customer_id = pi.customer_id
                            JOIN checks c2 on t.transaction_id = c2.transaction_id
                            JOIN product_grid pg on pg.sku_id = c2.sku_id
                            JOIN stores s2 on pg.sku_id = s2.sku_id AND t.transaction_store_id = s2.transaction_store_id)
SELECT DISTINCT customer_id,
                transaction_id,
                transaction_datetime,
                group_id,
                SUM(sku_purchase_price * SKU_Amount)
                OVER (PARTITION BY customer_id, group_id, transaction_id, transaction_datetime) AS Group_Cost,
                SUM(sku_summ)
                OVER (PARTITION BY customer_id, group_id, transaction_id, transaction_datetime) AS Group_Summ,
                SUM(sku_summ_paid)
                OVER (PARTITION BY customer_id, group_id, transaction_id, transaction_datetime) AS Group_Summ_Paid
FROM purchases;
