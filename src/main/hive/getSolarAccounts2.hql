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
	LOCATION '${resourcePath}/accounts';

CREATE TABLE IF NOT EXISTS do_not_call (
		id BIGINT)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
	LOCATION '${resourcePath}/doNotCall/';

DROP TABLE IF EXISTS accounts_to_contact;
CREATE TABLE IF NOT EXISTS accounts_to_contact
	LIKE do_not_call
	LOCATION '${resultPath}/accountsToContact';

INSERT OVERWRITE TABLE accounts_to_contact
SELECT accounts.id
	FROM accounts
	LEFT OUTER JOIN do_not_call ON accounts.id = do_not_call.id
	WHERE accounts.capacity == 0 AND do_not_call.id IS NULL;
