CREATE TABLE IF NOT EXISTS accounts (
		id BIGINT COMMENT 'Synthetic Id',
		account_id STRING,
		name STRING,
		address1 STRING,
		address2 STRING,
		city STRING,
		state STRING,
		zip STRING,
		capacity DOUBLE,
		date_modified STRING)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
	LOCATION '${resourcePath}/accounts/';

DROP TABLE IF EXISTS non_solar_accounts;
CREATE TABLE IF NOT EXISTS non_solar_accounts
	LIKE accounts
	LOCATION '${resultPath}/nonSolarAccounts/';

DROP TABLE IF EXISTS solar_accounts;
CREATE TABLE IF NOT EXISTS solar_accounts
	LIKE accounts
	LOCATION '${resultPath}/solarAccounts/';

INSERT OVERWRITE TABLE non_solar_accounts
SELECT * from accounts WHERE capacity = 0.0;

INSERT OVERWRITE TABLE solar_accounts
SELECT * from accounts WHERE capacity <> 0.0;
