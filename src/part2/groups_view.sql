DROP VIEW IF EXISTS Groups_View;
DROP FUNCTION IF EXISTS INPUT_DATA(mode int, option int);
CREATE FUNCTION INPUT_DATA(mode int DEFAULT 2, option int DEFAULT 5000)
    RETURNS table
            (
                customer_id_new varchar,
                group_id_new    varchar,
                Marg            FLOAT
            )
AS
$$
DECLARE
    count_day             int = 0;
    Transaction_date_time timestamp;
BEGIN
    SELECT INTO Transaction_date_time (analysis_formation::DATE - option) FROM date_of_analysis_formation;
    SELECT INTO count_day COUNT(transaction_datetime)
      FROM purchase_history_view
     WHERE transaction_datetime >= Transaction_date_time;
    IF (mode = 1)
    THEN
        RETURN QUERY SELECT customer_id, group_id, SUM(Margin)
                       FROM (SELECT group_summ_paid - group_cost Margin, customer_id, group_id
                               FROM purchase_history_view AS phv,
                                    date_of_analysis_formation
                              ORDER BY transaction_datetime DESC
                              LIMIT option) AS mode1
                      GROUP BY group_id, customer_id
                      ORDER BY customer_id;
    ELSE
        RETURN QUERY SELECT customer_id, group_id, SUM(Margin)
                       FROM (SELECT group_summ_paid - group_cost Margin, customer_id, group_id
                               FROM purchase_history_view AS phv
                              ORDER BY transaction_datetime DESC
                              LIMIT option) AS mode2
                      GROUP BY group_id, customer_id;
    END IF;
END;
$$ LANGUAGE 'plpgsql';


CREATE OR REPLACE VIEW Groups_view AS
WITH cte_group AS
         (SELECT DISTINCT ph.customer_id,
                          ph.group_id,
                          ph.group_summ_paid,
                          ph.transaction_datetime,
                          group_cost,
                          ROW_NUMBER()
                          OVER (PARTITION BY ph.customer_id, ph.group_id ORDER BY transaction_datetime DESC)                                     AS row_day,
                          p.group_purchase /
                          (COUNT(ph.transaction_id) OVER (PARTITION BY p.customer_id, p.group_id) + (SELECT COUNT(*)
                                                                                                     FROM purchase_history_view AS ph1
                                                                                                     WHERE ph1.customer_id = ph.customer_id
                                                                                                       AND ph1.group_id != ph.group_id
                                                                                                       AND ph1.transaction_datetime
                                                                                                         BETWEEN p.first_group_purchase_date
                                                                                                         AND p.last_group_purchase_date))::float AS Group_Affinity_Index,
                          EXTRACT(EPOCH FROM (SELECT * FROM date_of_analysis_formation) -
                                             (MAX(transaction_datetime)
                                              OVER (PARTITION BY p.customer_id, p.group_id))) / 86400.0 /
                          Group_Frequency                                                                                                        AS Group_Churn_Rate,
                          ABS(EXTRACT(EPOCH FROM transaction_datetime - LAG(transaction_datetime, 1)
                                                                        OVER (PARTITION BY p.customer_id, p.group_id ORDER BY transaction_datetime))::float /
                              86400.0 - Group_Frequency) /
                          Group_Frequency                                                                                                        AS Group_Stability_Index,
                          COUNT(c.transaction_id)
                          FILTER (WHERE c.sku_discount > 0) OVER (PARTITION BY p.customer_id, p.group_id)::float /
                          group_purchase                                                                                                         AS Group_Discount_Share,
                          COALESCE((SELECT MIN(sku_discount / sku_summ)
                                    FROM checks c1
                                             JOIN purchase_history_view ph2 ON ph2.transaction_id = c1.transaction_id
                                    WHERE (sku_discount / sku_summ) > 0
                                      AND ph2.customer_id = ph.customer_id
                                      AND ph2.group_id = ph.group_id),
                                   0)                                                                                                            AS Group_Minimum_Discount,
                          AVG(group_summ_paid) OVER (PARTITION BY p.customer_id, p.group_id) /
                          AVG(group_summ) OVER (PARTITION BY p.customer_id, p.group_id)                                                          AS Group_Average_Discount
          FROM purchase_history_view AS ph
                   JOIN periods_view p
                        on ph.customer_id = p.customer_id and ph.group_id = p.group_id
                   JOIN checks c on ph.transaction_id = c.transaction_id)
SELECT DISTINCT g.customer_id,
                g.group_id,
                Group_Affinity_Index,
                Group_Churn_Rate,
                COALESCE(AVG(Group_Stability_Index), 0) AS Group_Stability_Index,
                gm.marg Group_Margin,
                Group_Discount_Share,
                Group_Minimum_Discount,
                Group_Average_Discount
FROM cte_group AS g
    JOIN
    (SELECT *
     FROM INPUT_DATA()) AS gm
       ON gm.customer_id_new = g.customer_id AND gm.group_id_new = g.group_id
WHERE Group_Minimum_Discount IS NOT NULL
GROUP BY g.customer_id, g.group_id, Group_Affinity_Index, Group_Churn_Rate,  gm.marg, Group_Discount_Share,
         Group_Minimum_Discount, Group_Average_Discount;

SELECT customer_id, group_id, COALESCE((SELECT MIN(sku_discount / sku_summ)
                                    FROM checks c1
                                             JOIN purchase_history_view ph2 ON ph2.transaction_id = c1.transaction_id
                                    WHERE (sku_discount / sku_summ) > 0
                                      AND ph2.customer_id = ph.customer_id
                                      AND ph2.group_id = ph.group_id),
                                   0) FROM purchase_history_view ph