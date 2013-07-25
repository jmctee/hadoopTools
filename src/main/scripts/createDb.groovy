import groovy.sql.Sql

def dropTable = '''DROP TABLE IF EXISTS accounts;'''

def createTable = '''CREATE TABLE accounts (
  id                  bigint PRIMARY KEY,
  account_identifier  varchar(40) NOT NULL,
  name                varchar(84) NOT NULL,
  address1            varchar(128) NOT NULL,
  address2            varchar(128),
  city                varchar(84) NOT NULL,
  state               varchar(2) NOT NULL,
  zip                 varchar(10) NOT NULL,
  system_capacity     real NOT NULL,
  last_updated        timestamp NOT NULL);'''

def sql = Sql.newInstance("jdbc:postgresql://hadoop-dn3:5432/solar_accounts", "hadoopuser", "password", "org.postgresql.Driver")

sql.execute(dropTable)
sql.execute(createTable)
