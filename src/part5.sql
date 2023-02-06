SET datestyle TO ISO,DMY;
DROP FUNCTION IF EXISTS defining_an_offer_condition(Date_beginning timestamp, Date_completion timestamp,
                                                    count_transactions int);
CREATE OR REPLACE FUNCTION defining_an_offer_condition(Date_beginning timestamp, Date_completion timestamp,
                                                       count_transactions int)
    RETURNS table
            (
                customer_id             varchar,
                Start_Date              timestamp,
                End_Date                timestamp,
                Required_Transactions_Count numeric
            )
AS
$$
BEGIN
  RETURN QUERY SELECT cv.customer_id,
           Date_beginning                                                                    AS Start_Date,
           Date_completion                                                                      AS End_Date,
           ROUND((Date_completion::date - Date_beginning::date) / Customer_Frequency) + count_transactions AS Required_Transactions_Count
      FROM customers_view cv;
END;
$$ LANGUAGE 'plpgsql';


DROP FUNCTION IF EXISTS receiving_remuneration(Date_beginning timestamp, Date_completion timestamp,
                                                  count_transactions int, max_churn_index numeric, max_share_of_discount_transaction numeric, allowable_margin_share numeric);
CREATE OR REPLACE FUNCTION receiving_remuneration(Date_beginning timestamp, Date_completion timestamp,
                                                  count_transactions int, max_churn_index numeric, max_share_of_discount_transaction numeric, allowable_margin_share numeric)
    RETURNS table
            (
                customer_id                 varchar,
                Start_Date                  timestamp,
                End_Date                    timestamp,
                Required_Transactions_Count numeric,
                Group_Name                  varchar,
                Offer_Discount_Depth        numeric
            )
AS
$$
BEGIN
    RETURN QUERY SELECT t1.customer_id,
        Date_beginning                                                                    AS Start_Date,
        Date_completion                                                                      AS End_Date,
        t1.Required_Transactions_Count as Required_Transactions_Count,
        sg.group_name as Group_Name,
        t2.Offer_Discount_Depth as Offer_Discount_Depth
        FROM defining_an_offer_condition(Date_beginning, Date_completion, count_transactions) t1
    JOIN determination_of_the_group(max_churn_index,max_share_of_discount_transaction,allowable_margin_share) t2 on t2.customer_id = t1.customer_id
    join sku_group sg on sg.group_id = t2.group_id;
END;
$$ LANGUAGE 'plpgsql';


SELECT *
  FROM receiving_remuneration('18.08.2022', '18.08.2022', 1,3,70,30 );
