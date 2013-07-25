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
  def system_capacity_map = ["80011": 6.0, "80012": 7.0, "80013": 7.56]

  double system_capacity

  if (system_capacity_map.containsKey(zip)) {
    system_capacity = system_capacity_map[zip]
  }
  else {
    system_capacity = 0
  }


  def map = [:]

  map.id = id
  map.account_identifier = account_identifier
  map.name = name
  map.address1 = address1
  map.address2 = address2
  map.city = city
  map.state = state
  map.zip = zip
  map.system_capacity = system_capacity
  map.last_updated = insertionDate

  accounts.add(map)
}

if (args.size() != 2) {
  println "usage: groovy populateDb.groovy <starting id> <ending id>"
  System.exit(-1)
}

def insertionDate = new java.sql.Timestamp(new Date().time)

def accounts = []

(args[0]..args[1]).each { id ->
  addAccount(accounts, Integer.parseInt(id), insertionDate)
}

accounts.each { account ->
  def line = ""
  account.each { k, v ->
    if (line.length() != 0) {
      line += ','
    }
    line += v
  }

  println line
}
