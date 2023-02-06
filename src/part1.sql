CREATE DATABASE SQL_3;
-- DROP DATABASE SQL_3;
CREATE EXTENSION IF NOT EXISTS citext
    WITH SCHEMA public;

DROP TABLE IF EXISTS Personal_information CASCADE;
DROP TABLE IF EXISTS Cards CASCADE;
DROP TABLE IF EXISTS Transactions CASCADE;
DROP TABLE IF EXISTS Date_of_analysis_formation CASCADE;
DROP TABLE IF EXISTS SKU_group CASCADE;
DROP TABLE IF EXISTS Stores CASCADE;
DROP TABLE IF EXISTS Product_grid CASCADE;
DROP TABLE IF EXISTS Checks CASCADE;

SET datestyle TO ISO,DMY;

CREATE TABLE Personal_information
(
    Customer_ID            varchar PRIMARY KEY,
    Customer_Name          varchar NOT NULL,
    CHECK ( Customer_Name ~ '^([А-Я]{1}[а-яё]{1,23}|[A-Z]{1}[a-z]{1,23})$'),
    Customer_Surname       VARCHAR NOT NULL,
    CHECK ( Customer_Surname ~ '^([А-Я]{1}[а-яё]{1,23}|[A-Z]{1}[a-z]{1,23})$'),
    Customer_Primary_Email citext  NOT NULL UNIQUE,
    CHECK ( Customer_Primary_Email ~
            '^[a-zA-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$'),
    Customer_Primary_Phone varchar NOT NULL UNIQUE,
    CHECK ( Customer_Primary_Phone ~ '^((\+7)+([0-9]){10})$')
);

CREATE TABLE Cards
(
    Customer_Card_ID varchar PRIMARY KEY,
    Customer_ID      varchar NOT NULL,
    CONSTRAINT fk_customer_id FOREIGN KEY (Customer_ID) REFERENCES Personal_information (Customer_ID)
);

CREATE TABLE IF NOT EXISTS SKU_group
(
    Group_ID   varchar NOT NULL PRIMARY KEY,
    Group_Name varchar NOT NULL,
    CHECK ( Group_Name ~ '^[[а-яА-ЯёЁa-zA-Z0-9!@#$%^&*()_+\-=\[\]{};:"\\|,.<>\/?]*$')
);

CREATE TABLE IF NOT EXISTS Product_grid
(
    SKU_ID             varchar NOT NULL UNIQUE,
    SKU_Name           varchar,
--         CHECK ( SKU_Name ~ '^[[а-яА-ЯёЁa-zA-Z0-9!@#$%^&*()_+\-=\[\]{};:"\\|,.<>\/?]*$'),
    Group_ID           varchar NOT NULL,
    CONSTRAINT fk_croup_id FOREIGN KEY (Group_ID) REFERENCES SKU_group (Group_ID)
);

CREATE TABLE IF NOT EXISTS Stores
(
    Transaction_Store_ID varchar NOT NULL,
    SKU_ID               varchar NOT NULL,
    CONSTRAINT fk_sku_id FOREIGN KEY (SKU_ID) REFERENCES Product_grid (SKU_ID),
    SKU_Purchase_Price   double precision,
    SKU_Retail_Price     double precision
);

CREATE TABLE Transactions
(
    Transaction_ID       varchar PRIMARY KEY,
    Customer_Card_ID     varchar NOT NULL,
    CONSTRAINT fk_card_id FOREIGN KEY (Customer_Card_ID) REFERENCES Cards (Customer_Card_ID),
    Transaction_Summ     float,
    Transaction_DateTime timestamp,
    Transaction_Store_ID VARCHAR NOT NULL
);


CREATE TABLE Checks
(
    Transaction_ID varchar,
    CONSTRAINT fk_transaction_id FOREIGN KEY (Transaction_ID) REFERENCES Transactions (transaction_id),
    SKU_ID         varchar,
    CONSTRAINT fk_checks_id FOREIGN KEY (SKU_ID) REFERENCES Product_grid (SKU_ID),
    SKU_Amount     float,
    SKU_Summ       float,
    SKU_Summ_Paid  float,
    SKU_Discount   float
);



CREATE TABLE IF NOT EXISTS Date_of_analysis_formation
(
    Analysis_Formation timestamp
);

SELECT TO_CHAR(Analysis_Formation, 'DD.MM.YYYY HH:MM:SS') AS NEW
  FROM Date_of_analysis_formation;

--                                                         <<< Import from .csv and .tsv >>>


CREATE OR REPLACE FUNCTION Insert_From(filepath text, new_delimeter varchar) RETURNS void AS
$$
DECLARE
    cp_cmd    text;
    extension varchar;
BEGIN
    IF new_delimeter = ','
    THEN
        extension = '.csv';
    ELSEIF new_delimeter = '\t'
    THEN
        extension = '.tsv';
    END IF;
    cp_cmd :=
                                    'COPY Personal_information(Customer_ID,Customer_Name,Customer_Surname,Customer_Primary_Email,Customer_Primary_Phone)
                                    FROM ' || QUOTE_LITERAL(filepath || 'personal_information' || extension) ||
                                    'DELIMITER E' || '''' || new_delimeter || '''' ||
                                    'CSV HEADER';
    EXECUTE cp_cmd;
    cp_cmd :=
                                    'COPY Cards(Customer_Card_ID,Customer_ID)
                                    FROM ' || QUOTE_LITERAL(filepath || 'cards' || extension) ||
                                    'DELIMITER E' || '''' || new_delimeter || '''' ||
                                    'CSV HEADER';
    EXECUTE cp_cmd;
    cp_cmd :=
                                    'COPY SKU_group(Group_ID,Group_Name)
                                    FROM ' || QUOTE_LITERAL(filepath || 'sku_group' || extension) ||
                                    'DELIMITER E' || '''' || new_delimeter || '''' ||
                                    'CSV HEADER';
    EXECUTE cp_cmd;
    cp_cmd :=
                                    'COPY Product_grid(SKU_ID,SKU_Name,Group_ID)
                                    FROM ' || QUOTE_LITERAL(filepath || 'product_grid' || extension) ||
                                    'DELIMITER E' || '''' || new_delimeter || '''' ||
                                    'CSV HEADER';
    EXECUTE cp_cmd;
    cp_cmd :=
                                    'COPY Stores(Transaction_Store_ID,SKU_ID,SKU_Purchase_Price,SKU_Retail_Price)
                                    FROM ' || QUOTE_LITERAL(filepath || 'stores' || extension) ||
                                    'DELIMITER E' || '''' || new_delimeter || '''' ||
                                    'CSV HEADER';
    EXECUTE cp_cmd;
    cp_cmd :=
                                    'COPY Transactions(Transaction_ID,Customer_Card_ID,Transaction_Summ,Transaction_DateTime,Transaction_Store_ID)
                                    FROM ' || QUOTE_LITERAL(filepath || 'transactions' || extension) ||
                                    'DELIMITER E' || '''' || new_delimeter || '''' ||
                                    'CSV HEADER';
    EXECUTE cp_cmd;
    cp_cmd :=
                                    'COPY Checks(Transaction_ID,SKU_ID,SKU_Amount,SKU_Summ,SKU_Summ_Paid,SKU_Discount)
                                    FROM ' || QUOTE_LITERAL(filepath || 'checks' || extension) ||
                                    'DELIMITER E' || '''' || new_delimeter || '''' ||
                                    'CSV HEADER';
    EXECUTE cp_cmd;
    cp_cmd :=
                                    'COPY Date_of_analysis_formation(Analysis_Formation)
                                    FROM ' || QUOTE_LITERAL(filepath || 'date_of_analysis_formation' || extension) ||
                                    'DELIMITER E' || '''' || new_delimeter || '''' ||
                                    'CSV HEADER';
    EXECUTE cp_cmd;
END;
$$
    LANGUAGE plpgsql;
SELECT Insert_From('/Users/mzoraida/SQL3_RetailAnalitycs_v1.0-0/datasets/', '\t');


--                                                         <<< Export from .csv and .tsv >>>


CREATE OR REPLACE FUNCTION Insert_To(filepath text, new_delimeter char) RETURNS void AS
$$
DECLARE
    cp_cmd    text;
    extension varchar;
BEGIN
    IF new_delimeter = ','
    THEN
        extension = '.csv';
    ELSEIF new_delimeter = '\t'
    THEN
        extension = '.tsv';
    END IF;
    cp_cmd :=
                                    'COPY (SELECT * FROM Personal_information)
                                    TO ' || QUOTE_LITERAL(filepath || 'personal_information' || extension) ||
                                    'DELIMITER E' || '''' || new_delimeter || '''' ||
                                    'CSV HEADER';
    EXECUTE cp_cmd;
    cp_cmd :=
                                    'COPY (SELECT * FROM Cards)
                                    TO ' || QUOTE_LITERAL(filepath || 'cards' || extension) ||
                                    'DELIMITER E' || '''' || new_delimeter || '''' ||
                                    'CSV HEADER';
    EXECUTE cp_cmd;
    cp_cmd :=
                                    'COPY (SELECT * FROM SKU_group)
                                    TO ' || QUOTE_LITERAL(filepath || 'sku_group' || extension) ||
                                    'DELIMITER E' || '''' || new_delimeter || '''' ||
                                    'CSV HEADER';
    EXECUTE cp_cmd;
    cp_cmd :=
                                    'COPY (SELECT * FROM Product_grid)
                                    TO ' || QUOTE_LITERAL(filepath || 'product_grid' || extension) ||
                                    'DELIMITER E' || '''' || new_delimeter || '''' ||
                                    'CSV HEADER';
    EXECUTE cp_cmd;
    cp_cmd :=
                                    'COPY (SELECT * FROM Stores)
                                    TO ' || QUOTE_LITERAL(filepath || 'stores' || extension) ||
                                    'DELIMITER E' || '''' || new_delimeter || '''' ||
                                    'CSV HEADER';
    EXECUTE cp_cmd;
    cp_cmd :=
                                    'COPY (SELECT * FROM Transactions)
                                    TO ' || QUOTE_LITERAL(filepath || 'transactions' || extension) ||
                                    'DELIMITER E' || '''' || new_delimeter || '''' ||
                                    'CSV HEADER';
    EXECUTE cp_cmd;
    cp_cmd :=
                                    'COPY (SELECT * FROM Checks)
                                    TO ' || QUOTE_LITERAL(filepath || 'checks' || extension) ||
                                    'DELIMITER E' || '''' || new_delimeter || '''' ||
                                    'CSV HEADER';
    EXECUTE cp_cmd;
    cp_cmd :=
                                    'COPY (SELECT * FROM Date_of_analysis_formation)
                                    TO ' || QUOTE_LITERAL(filepath || 'date_of_analysis_formation' || extension) ||
                                    'DELIMITER E' || '''' || new_delimeter || '''' ||
                                    'CSV HEADER';
    EXECUTE cp_cmd;
END;
$$
    LANGUAGE plpgsql;
SELECT Insert_To('/Users/mzoraida/SQL3_RetailAnalitycs_v1.0-0/src/export/', '\t');
