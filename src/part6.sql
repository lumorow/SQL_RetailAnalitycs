CREATE OR REPLACE FUNCTION defining_offer_increasing_margin(
    cnt_group int,
    max_churn_rate numeric,
    max_stability_index numeric,
    max_index_sku numeric,
    margin_share numeric
)
    RETURNS TABLE
            (
                Customer_ID          int,
                SKU_Name             varchar,
                Offer_Discount_Depth numeric
            )
AS
$$

WITH cte_preparing_metrics AS
         (SELECT DISTINCT g.customer_id::int,
                 pg.sku_name,
                 g.group_churn_rate,
                 g.group_stability_index,
                 MAX(s4.sku_retail_price - s4.sku_purchase_price)
                 OVER (PARTITION BY g.customer_id, g.group_id, pg.sku_id),
                 (COUNT(s4.transaction_store_id) OVER (PARTITION BY pg.sku_id))::float
                     / (COUNT(s4.transaction_store_id) OVER (PARTITION BY g.group_id))::float     AS share_sku_group,
                 CASE WHEN ((SKU_Retail_Price - SKU_Purchase_Price)::float * (margin_share/ 100) /
                 SKU_Retail_Price) <= ceil(group_minimum_discount * 100/ 5.0) * 5 THEN ceil(group_minimum_discount * 100 / 5.0) * 5  END                                                           AS Offer_Discount_Depth,
                 DENSE_RANK() OVER (PARTITION BY g.customer_id ORDER BY g.group_id)               AS Ranks
          FROM groups_view AS g
                   JOIN customers_view c2 ON g.customer_id = c2.customer_id
                   JOIN cards c4 ON c2.customer_id = c4.customer_id
                   JOIN product_grid pg ON g.group_id = pg.group_id
                   JOIN stores s4 ON pg.sku_id = s4.sku_id
                   JOIN transactions t2 ON c4.customer_card_id = t2.customer_card_id
                   JOIN sku_group sg ON pg.group_id = sg.group_id)
SELECT DISTINCT customer_id, sku_name, Offer_Discount_Depth
FROM cte_preparing_metrics
WHERE max_churn_rate >= group_churn_rate
  AND max_stability_index >= group_stability_index
  AND cnt_group >= Ranks
  AND max_index_sku <= share_sku_group * 100
  AND Offer_Discount_Depth IS NOT NULL
$$ LANGUAGE SQL;

SELECT *
FROM defining_offer_increasing_margin(5, 3, 0.5, 100, 30)
