CREATE OR REPLACE WAREHOUSE ecommerce
  WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60  -- suspend after 1 minute idle
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for aws s3 external data load';
    
CREATE OR REPLACE DATABASE ecommerce_dw;
USE DATABASE ecommerce_dw;

CREATE OR REPLACE SCHEMA raw;
CREATE OR REPLACE SCHEMA control;

