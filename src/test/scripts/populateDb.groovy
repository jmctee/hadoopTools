import groovy.sql.Sql

def addAccount(def accounts, def id, def insertionDate) {

  def lastDigit = id
  if (id > 9) {
    lastDigit = id % 10
  }

  def account_identifier = "${lastDigit}${lastDigit}-${lastDigit}${lastDigit}${lastDigit}-${id}".toString()
  def name = "First${lastDigit} Last${id}".toString()
  def address1 = "${id} ${lastDigit}${lastDigit}${lastDigit} Street".toString()
  def address2 = "".toString()
  def city = "City${id}".toString()
  def state = "CO".toString()
  def zip = "8001${lastDigit}".toString()
  def system_capacity_map = ["80011": 1, "80012": 2, "80013": 7.56]

  double system_capacity

  if (system_capacity_map.containsKey(zip)) {
    system_capacity = system_capacity_map[zip]
  }
  else {
    system_capacity = 0
  }

  accounts.add(id: id,
          account_identifier: account_identifier,
          name: name,
          address1: address1,
          address2: address2,
          city: city,
          state: state,
          zip: zip,
          system_capacity: system_capacity,
          last_updated: insertionDate)
}

if (args.size() != 2) {
  println "usage: groovy populateDb.groovy <starting id> <ending id>"
  System.exit(-1)
}

def sql = Sql.newInstance("jdbc:postgresql://hadoop-dn3:5432/solar_accounts", "hadoopuser", "password", "org.postgresql.Driver")

def insertionDate = new java.sql.Timestamp(new Date().time)

def accounts = sql.dataSet("accounts")

(args[0]..args[1]).each { id ->
  addAccount(accounts, Integer.parseInt(id), insertionDate)
}

def query = "select * from accounts"
sql.eachRow query, { account ->
  println account
}