Use database ecommerce_dw;
USE SCHEMA analytics;

--  create a masking policy 
CREATE OR REPLACE MASKING POLICY mask_email AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'DATA_SCIENTIST') THEN val
    ELSE CONCAT(SUBSTR(val,1,1), '****', SPLIT_PART(val,'@',2))
  END;

-- mask customer emails in the mart_sales_overview 

ALTER TABLE analytics.mart_sales_overview 
MODIFY COLUMN customer_email 
SET MASKING POLICY mask_email;

USE SCHEMA analytics;
-- Create row access policy for customer country
CREATE OR REPLACE ROW ACCESS POLICY rls_country AS (country STRING) RETURNS BOOLEAN ->
  CASE
    WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'DATA_SCIENTIST') THEN TRUE
    ELSE country = 'Madagascar'  -- Example: restrict analysts to Madagascar only
  END;
-- Add row access to customer country column in the marts_sales_overview table
ALTER TABLE analytics.mart_sales_overview
ADD ROW ACCESS POLICY rls_country ON (customer_country);
