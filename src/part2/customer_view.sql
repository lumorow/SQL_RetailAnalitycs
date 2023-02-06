DROP TABLE IF EXISTS segment_table CASCADE;
CREATE TABLE IF NOT EXISTS segment_table
(
    Segment                bigint,
    Average_check          varchar,
    Frequency_of_purchases varchar,
    Churn_probability      varchar
);

COPY segment_table (segment, average_check, frequency_of_purchases, churn_probability)
    FROM '/Users/mzoraida/SQL3_RetailAnalitycs_v1.0-0/src/import/segment_table.csv' DELIMITER ',' CSV HEADER;

DROP VIEW IF EXISTS Customers_View;
CREATE VIEW Customers_View
            (
             Customer_ID,
             Customer_Average_Check,
             Customer_Average_Check_Segment,
             Customer_Frequency,
             Customer_Frequency_Segment,
             Customer_Inactive_Period,
             Customer_Churn_Rate,
             Customer_Churn_Segment,
             Customer_Segment,
             Customer_Primary_Store
                )
AS
SELECT DISTINCT *
  FROM (WITH table_1 AS (SELECT t1.customer_id                                    customer_id,
                                t1.Customer_Average_Check,
                                CASE
                                    WHEN (((ROW_NUMBER() OVER (ORDER BY Customer_Average_Check DESC) * 1.0) /
                                           ((SELECT COUNT (*) FROM(SELECT DISTINCT c.customer_id
  FROM personal_information
           JOIN cards c ON personal_information.customer_id = c.customer_id
           JOIN transactions t ON c.customer_card_id = t.customer_card_id) as t1) * 1.0) *
                                           100.00) <= 10.00)
                                        THEN 'High'
                                    WHEN (((ROW_NUMBER() OVER (ORDER BY Customer_Average_Check DESC) * 1.0) /
                                           ((SELECT COUNT (*) FROM(SELECT DISTINCT c.customer_id
  FROM personal_information
           JOIN cards c ON personal_information.customer_id = c.customer_id
           JOIN transactions t ON c.customer_card_id = t.customer_card_id) as t1) * 1.0) *
                                           100.00) <= 35.00) THEN 'Medium'
                                    WHEN (((ROW_NUMBER() OVER (ORDER BY Customer_Average_Check DESC) * 1.0) /
                                           ((SELECT COUNT (*) FROM(SELECT DISTINCT c.customer_id
  FROM personal_information
           JOIN cards c ON personal_information.customer_id = c.customer_id
           JOIN transactions t ON c.customer_card_id = t.customer_card_id) as t1) * 1.0) *
                                           100.00) <= 100.00) THEN 'Low' END Customer_Average_Check_Segment,
                                intensiv                                          Customer_Frequency,
                                CASE
                                    WHEN (((ROW_NUMBER() OVER (ORDER BY intensiv ASC) * 1.0) /
                                           ((SELECT COUNT(personal_information.customer_id)::int
                                               FROM personal_information) * 1.0) *
                                           100.00)::int <= 10.00)
                                        THEN 'Often'
                                    WHEN (((ROW_NUMBER() OVER (ORDER BY intensiv ASC) * 1.0) /
                                           ((SELECT COUNT(personal_information.customer_id)
                                               FROM personal_information) * 1.0) *
                                           100.00) <= 25.00) THEN 'Occasionally'
                                    ELSE 'Rarely' END                             Customer_Frequency_Segment,
                                time_hour                                         Customer_Inactive_Period,
                                (time_hour / intensiv)                            Customer_Churn_Rate,
                                CASE
                                    WHEN ((time_hour / intensiv) <= 2.0) THEN 'Low'
                                    WHEN ((time_hour / intensiv) <= 5.0) THEN 'Medium'
                                    ELSE 'High' END                               Customer_Churn_Segment
                           FROM (SELECT c.customer_id                               customer_id,
                                        (SUM(transaction_summ) / COUNT(*))::numeric Customer_Average_Check

                                   FROM personal_information
                                            LEFT JOIN cards c ON personal_information.customer_id = c.customer_id
                                            LEFT JOIN transactions t ON c.customer_card_id = t.customer_card_id WHERE c.customer_id IS NOT NULL
                                  GROUP BY c.customer_id) AS t1
                                    LEFT JOIN
                                (SELECT c.customer_id customer_id_2,
                                        (((TO_CHAR(MAX(transaction_datetime) - MIN(transaction_datetime), 'DD'))::int *
                                          1.0 / COUNT(*) *
                                          1.0))       intensiv
                                   FROM personal_information
                                            LEFT JOIN cards c ON personal_information.customer_id = c.customer_id
                                            LEFT JOIN transactions t ON c.customer_card_id = t.customer_card_id
                                  GROUP BY c.customer_id) AS t2 ON t2.customer_id_2 = t1.customer_id
                                    LEFT JOIN
                                (SELECT customer_id,
                                        (TO_CHAR(time_interv, 'DD')::int * 1.0 +
                                         (TO_CHAR(time_interv, 'HH')::int / 24.0 +
                                          TO_CHAR(time_interv, 'MM')::int / 1440.0 +
                                          TO_CHAR(time_interv, 'SS')::int / 86400.0)) * 1.0 time_hour
                                   FROM (SELECT customer_id,
                                                (SELECT MAX(analysis_formation) FROM date_of_analysis_formation) -
                                                MAX(transaction_datetime) time_interv
                                           FROM cards
                                                    JOIN transactions t ON cards.customer_card_id = t.customer_card_id
                                          GROUP BY customer_id) AS t_time) AS t3 ON t3.customer_id = t1.customer_id)
      SELECT table_1.customer_id customer_id,
             Customer_Average_Check,
             Customer_Average_Check_Segment,
             Customer_Frequency,
             Customer_Frequency_Segment,
             Customer_Inactive_Period,
             Customer_Churn_Rate,
             Customer_Churn_Segment,
             Segment,
             Customer_Primary_Store

        FROM table_1
                 LEFT JOIN segment_table st ON Customer_Average_Check_Segment = st.Average_check AND
                                          Customer_Frequency_Segment = st.Frequency_of_purchases AND
                                          Customer_Churn_Segment = st.Churn_probability
                 JOIN (SELECT customer_id,
                              CASE
                                  WHEN (res_2_transaction_store_id IS NULL) THEN res_1_transaction_store_id
                                  ELSE res_2_transaction_store_id END Customer_Primary_Store
                         FROM (SELECT res_1.customer_id,
                                      res_1.transaction_store_id res_1_transaction_store_id,
                                      share_of_transactions,
                                      res_2.transaction_store_id res_2_transaction_store_id
                                 FROM (SELECT res_1.customer_id, res_1.transaction_store_id, share_of_transactions
                                         FROM (SELECT t3.customer_id,
                                                      t3.transaction_store_id,
                                                      share_of_transactions,
                                                      t4.transaction_datetime,
                                                      RANK()
                                                      OVER ( PARTITION BY t3.customer_id ORDER BY t3.customer_id, transaction_datetime DESC) rank
                                                 FROM (SELECT t1.customer_id,
                                                              t1.transaction_store_id,
                                                              (count_pursh * 1.0) / (count_all_pursh * 1.0)
                                                                  share_of_transactions
                                                         FROM (SELECT customer_id, transaction_store_id, COUNT(*) count_pursh
                                                                 FROM cards
                                                                          JOIN transactions t ON cards.customer_card_id = t.customer_card_id
                                                                GROUP BY customer_id, transaction_store_id) AS t1
                                                                  JOIN (SELECT customer_id, COUNT(*) count_all_pursh
                                                                          FROM cards
                                                                                   JOIN transactions t ON cards.customer_card_id = t.customer_card_id
                                                                         GROUP BY customer_id) AS t2
                                                                       ON t1.customer_id = t2.customer_id
                                                        WHERE count_pursh = (SELECT MAX(count_pursh)
                                                                               FROM (SELECT customer_id, transaction_store_id, COUNT(*) count_pursh
                                                                                       FROM cards
                                                                                                JOIN transactions t ON cards.customer_card_id = t.customer_card_id
                                                                                      GROUP BY customer_id, transaction_store_id) AS t2
                                                                              WHERE t1.customer_id = t2.customer_id)) t3
                                                          JOIN (SELECT customer_id, transaction_store_id, transaction_datetime
                                                                  FROM cards
                                                                           JOIN transactions t ON cards.customer_card_id = t.customer_card_id) AS t4
                                                               ON t3.customer_id = t4.customer_id AND
                                                                  t3.transaction_store_id =
                                                                  t4.transaction_store_id) res_1
                                        WHERE rank = 1) AS res_1
                                          LEFT JOIN
                                      (SELECT customer_id, transaction_store_id
                                         FROM (SELECT customer_id, transaction_store_id, COUNT(*) count_pursh
                                                 FROM (SELECT customer_id, transaction_store_id, transaction_datetime, rang

                                                         FROM (SELECT customer_id,
                                                                      transaction_store_id,
                                                                      transaction_datetime,
                                                                      RANK()
                                                                      OVER ( PARTITION BY customer_id ORDER BY customer_id, transaction_datetime, transaction_store_id DESC) rang
                                                                 FROM cards
                                                                          JOIN transactions t ON cards.customer_card_id = t.customer_card_id
                                                                ORDER BY customer_id, transaction_datetime DESC) AS res
                                                        WHERE rang < 4
                                                        ORDER BY customer_id) AS res_2
                                                GROUP BY customer_id, transaction_store_id) AS res_2
                                        WHERE count_pursh = 3) AS res_2
                                      ON res_1.customer_id = res_2.customer_id) AS res) AS res
                      ON res.customer_id = table_1.customer_id) AS res_part2_1;

