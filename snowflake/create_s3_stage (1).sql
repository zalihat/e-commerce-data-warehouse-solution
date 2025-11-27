use database ecommerce_dw;
use schema RAW;

CREATE OR REPLACE STAGE s3_ecommerce_stage 
	URL = 's3://zalihat-ecommerce-raw-dev/bronze' 
	CREDENTIALS = ( AWS_KEY_ID = '' AWS_SECRET_KEY = '' ) 
	DIRECTORY = ( ENABLE = true );