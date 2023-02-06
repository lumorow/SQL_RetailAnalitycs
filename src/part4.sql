DROP FUNCTION IF EXISTS sorted_group();
CREATE OR REPLACE FUNCTION sorted_group()
    RETURNS TABLE
            (
                customer_id            varchar,
                group_id               varchar,
                group_affinity_index   numeric,
                group_churn_rate       numeric,
                group_discount_share   numeric,
                group_minimum_discount numeric,
                av_margin numeric
            )
AS
$$
WITH cte_row_groups AS
         (SELECT *, rank() OVER (PARTITION BY customer_id ORDER BY group_affinity_index DESC) AS number_id,
                   AVG(group_margin) OVER (PARTITION BY customer_id, group_id)                     AS av_margin
          FROM groups_view)
SELECT customer_id, group_id, group_affinity_index, group_churn_rate, group_discount_share,group_minimum_discount, av_margin
FROM cte_row_groups
$$ LANGUAGE SQL;


DROP FUNCTION IF EXISTS determination_of_the_group(max_churn_index numeric, max_share_of_discount_transaction numeric, allowable_margin_share numeric);
CREATE OR REPLACE FUNCTION determination_of_the_group(max_churn_index numeric, max_share_of_discount_transaction numeric, allowable_margin_share numeric)
RETURNS table
            (
                customer_id                 varchar,
                Group_ID                    varchar,
                Offer_Discount_Depth        numeric
            )
AS
$$
DECLARE
    id       varchar  := -1;
    value    record;
    group_cur CURSOR FOR
        (SELECT *
         FROM sorted_group());
    is_check bool := TRUE;
BEGIN
    FOR value IN group_cur
        LOOP
            IF is_check != TRUE AND id = value.customer_id THEN
                CONTINUE;
            END IF;
            IF value.group_churn_rate <= max_churn_index AND value.group_discount_share <= max_share_of_discount_transaction THEN
                IF abs(value.av_margin * allowable_margin_share/100) >= ceil((value.group_minimum_discount*100)/5.0)*0.05 * abs(value.av_margin) THEN
                    Customer_ID = value.customer_id;
                    Group_ID = value.group_id;
                    Offer_Discount_Depth = ceil((value.group_minimum_discount*100)/5.0)*5;
                    is_check = FALSE;
                    id = Customer_ID;
                    RETURN NEXT;
                ELSE
                    is_check = TRUE;
                END IF;
            ELSE
                is_check = TRUE;
            END IF;
        END LOOP;
END;
$$ LANGUAGE 'plpgsql';


create or replace  function get_average_check_n(n int)
RETURNS table(customer_id varchar ,transaction_summ numeric)
    as $$
    select customer_id, avg(transaction_summ)
    from (select
        customer_id,
        transaction_summ,
        transaction_datetime,
        row_number() over (partition by customer_id order by transaction_datetime desc) as count
    from transactions
    join cards c on c.customer_card_id = transactions.customer_card_id
    order by customer_id, transaction_datetime desc) as foo
    where count<=n
    group by customer_id;
    $$ language sql;


create or replace  function get_average_check_date(begin_t date, end_t date)
RETURNS table(customer_id varchar ,cur_trans_avg numeric)
    as
    $$
    BEGIN
    CASE
        WHEN begin_t < first_transaction_date() THEN begin_t  =  first_transaction_date();
        WHEN end_t > last_transaction_date() THEN end_t = last_transaction_date();
        ELSE
        END CASE;

    return query select
        c.customer_id,
        avg(t.transaction_summ)::numeric as  cur_trans_avg
    from transactions t
    join cards c on c.customer_card_id = t.customer_card_id
    where t.transaction_datetime between begin_t and end_t
    group by c.customer_id;
    end;
    $$ language plpgsql;


CREATE
    OR REPLACE FUNCTION first_transaction_date() RETURNS DATE
AS
$$
SELECT transaction_datetime::timestamp::date
FROM transactions
ORDER BY transaction_datetime
LIMIT 1
$$ LANGUAGE SQL;


CREATE
    OR REPLACE FUNCTION last_transaction_date() RETURNS DATE
AS
$$
SELECT transaction_datetime::timestamp::date
FROM transactions
ORDER BY transaction_datetime DESC
LIMIT 1
$$ LANGUAGE SQL;


CREATE or replace FUNCTION get_current_avg(method varchar,
                        info varchar)
    RETURNS table(customer_id varchar,
                Required_Check_Measure numeric)
    as
    $$
    DECLARE
        first date;
        second date;
    BEGIN
    IF(method = '1')
        THEN
            first = split_part(info, ' ', 1)::date;
            second = split_part(info, ' ', 2)::date;
            return query
            select * from get_average_check_date(first,second);
        ELSE
            return query
                select * from get_average_check_n(info::int);
        END if;
    END;
    $$ language 'plpgsql';

-- DROP FUNCTION growth_of_average_check(method varchar,
--                         info varchar,
--                         coefficient numeric,
--                         max_churn_rate numeric,
--                         max_discount_share numeric,
--                         margin_share numeric);
CREATE
    OR REPLACE FUNCTION growth_of_average_check(method varchar,
                        info varchar,
                        coefficient numeric,
                        max_churn_rate numeric,
                        max_discount_share numeric,
                        margin_share numeric)
    RETURNS TABLE
            (
                Customer_ID            varchar,
                Required_Check_Measure numeric,
                Group_Name             varchar,
                Offer_Discount_Depth   numeric
            )
    AS
    $$
    DECLARE
    BEGIN
    RETURN QUERY SELECT a.Customer_ID, b.Required_Check_Measure*coefficient, sku_group.group_name, a.offer_discount_depth
                 from get_current_avg(method, info) b
                          JOIN determination_of_the_group(max_churn_rate, max_discount_share, margin_share) a
                               on a.customer_id = b.Customer_ID
                          JOIN sku_group on sku_group.group_id = a.group_id;
    END;
    $$
        LANGUAGE plpgsql;


SELECT *
from growth_of_average_check('2', '100',  1.15, 3, 70, 30);