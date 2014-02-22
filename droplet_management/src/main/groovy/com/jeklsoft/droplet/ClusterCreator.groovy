package com.jeklsoft.droplet

import com.jeklsoft.rest.UnsecuredRestClient

class ClusterCreator {

  def client = new UnsecuredRestClient()

  def baseUrl
  def clientKey
  def apiKey

  public ClusterCreator() {
    InputStream is = getClass().getResourceAsStream("/api.properties");

    def properties = [:]
    is.text.eachLine { line ->
      def prop = line.split('=')
      if (prop.size() > 1) {
        properties[prop[0]] = prop[1]
      }
      else {
        properties[prop[0]] = ''
      }
    }

    baseUrl = properties['baseUrl']
    clientKey = properties['clientKey']
    apiKey = properties['apiKey']
  }

  def getUrlContent(def url) {
    def response = client.withBaseUrl(baseUrl).get(url, UnsecuredRestClient.JSON)

    if (response.status.toString() == 'OK') {
      response
    }
    else {
      // TODO - refactor this to use an option
      null
    }
  }

  def getImageList() {
    getUrlContent("images/?client_id=${clientKey}&api_key=${apiKey}&filter=my_images").images
  }

  def getRegionList() {
    getUrlContent("regions/?client_id=${clientKey}&api_key=${apiKey}").regions
  }

  def getKeyList() {
    getUrlContent("ssh_keys/?client_id=${clientKey}&api_key=${apiKey}").ssh_keys
  }

  def getDropletSizeList() {
    getUrlContent("sizes/?client_id=${clientKey}&api_key=${apiKey}").sizes
  }

  def createDroplet(def dropletName, def imageId, def dropletSizeId, def regionId, def sshKeyId) {
    getUrlContent("droplets/new?client_id=${clientKey}&api_key=${apiKey}&name=${dropletName}&size_id=${dropletSizeId}&image_id=${imageId}&region_id=${regionId}&ssh_key_ids=${sshKeyId}").droplet.id.toString()
  }

  def getIdsNeededForDropletCreation(def desiredRegion, def desiredKey, def desiredSize, def nodes) {
    def result = [:]

    def attributeMap = [:]

    attributeMap['images'] = imageList
    attributeMap['regions'] = regionList
    attributeMap['keys'] = keyList
    attributeMap['sizes'] = dropletSizeList

    def imageIds = [:]
    attributeMap.images.each { object ->
      def name = object.name
      def id = object.id
      def node = name - "-base"
      if (node in nodes) {
        imageIds[node] = id
      }
    }

    result['imageIds'] = imageIds

    attributeMap.regions.each { object ->
      if (object.name == desiredRegion) {
        result['regionId'] = object.id
      }
    }

    attributeMap.keys.each { object ->
      if (object.name == desiredKey) {
        result['sshKeyId'] = object.id
      }
    }

    attributeMap.sizes.each { object ->
      if (object.name == desiredSize) {
        result['dropletSizeId'] = object.id
      }
    }

    result
  }

  def spinupDelay(def secondsToDelay) {
    println "Waiting ${secondsToDelay} seconds for droplets to spin up"

    def scaledSecondsToDelay = secondsToDelay / 10

    (1..scaledSecondsToDelay).each { ii ->
      sleep 10 * 1000
      println "Elapsed: ${ii * 10} seconds"
    }
  }

  def getAllDropletInfo() {
    getUrlContent("droplets?client_id=${clientKey}&api_key=${apiKey}").droplets
  }

  def getDestroyAllDroplets() {
    def droplets = allDropletInfo

    droplets.each { droplet ->
      println "Destroying droplet ${droplet.name}:${droplet.id}:${droplet.ip_address}"
      getUrlContent("droplets/${droplet.id}/destroy?client_id=${clientKey}&api_key=${apiKey}").droplets
    }
  }

  def createAllDroplets(def desiredRegion, def desiredKey, def desiredSize, def nodes) {
    def ids = getIdsNeededForDropletCreation(desiredRegion, desiredKey, desiredSize, nodes)

    def nodeIds = [:]
    nodes.each { node ->
      nodeIds[node] = createDroplet(node, ids.imageIds[node], ids.dropletSizeId, ids.regionId, ids.sshKeyId)
    }

    spinupDelay(60)

    def droplets = allDropletInfo

    droplets.each { droplet ->
      println "Droplet ${droplet.name} created at ${droplet.ip_address}"
    }
  }

  def establishConnection(def nodeName) {
    println "Establishing connection with ${nodeName}"

    //def connection = "down"
    def result = runCommand("ssh-keyscan ${nodeName}")

    if (!result.contains("${nodeName} ssh-rsa")) {
      println "Failed to connect to ${nodeName}"
      result = ''
    }

    result
  }

  def getUpdateNetworkAccessSettings() {
    try {
      def env = System.getenv()
      def home = env['HOME']
      runCommand('rm -Rf updatedConfiguration', 2)
      runCommand('mkdir updatedConfiguration', 2)
      runCommand('rm -Rf savedConfigurations', 2)
      runCommand('mkdir savedConfigurations', 2)
      runCommand("cp ${home}/.ssh/known_hosts savedConfigurations/", 2)
      runCommand('cp /etc/hosts savedConfigurations/', 2)
      runCommand('cp templates/known_hosts_local updatedConfiguration/', 2)
      runCommand('cp templates/etc_hosts_local updatedConfiguration/', 2)
      runCommand('cp templates/etc_hosts_droplet updatedConfiguration/', 2)

      def localHostsFile = new File("updatedConfiguration/etc_hosts_local")
      def dropletHostsFile = new File("updatedConfiguration/etc_hosts_droplet")

      def droplets = allDropletInfo

      droplets.each { droplet ->
        def line = "${droplet.ip_address}\t${droplet.name}\n"
        localHostsFile.append(line)
        dropletHostsFile.append(line)
      }

      runCommand('sudo cp updatedConfiguration/etc_hosts_local /etc/hosts', 120)

      def knownHostsLocalFile = new File("updatedConfiguration/known_hosts_local")
      droplets.each { droplet ->
        def result = establishConnection(droplet.name)
        if (result.size()) {
          knownHostsLocalFile.append(result)
        }
      }

      runCommand("cp updatedConfiguration/known_hosts_local ${home}/.ssh/known_hosts")

      def authorized_keys = "${runCommand("cat ${home}/.ssh/id_rsa.pub").trim()}\n"

      droplets.each { droplet ->
        runCommand("scp updatedConfiguration/etc_hosts_droplet root@${droplet.name}:/etc/hosts")
        runCommand("scp updatedConfiguration/known_hosts_local root@${droplet.name}:.ssh/known_hosts")
        authorized_keys += "${runCommand("getAuthorizedKeys.sh ${droplet.name}").trim()}\n"
      }

      new File("updatedConfiguration/authorized_keys").write(authorized_keys)

      droplets.each { droplet ->
        runCommand("scp updatedConfiguration/authorized_keys root@${droplet.name}:.ssh/authorized_keys")
      }
    }
    catch (Exception e) {

      println "Failed: ${e.class.name} ${e.message}"
    }
  }

  def runCommand(def command, def timeout = 60, def echo = false) {
    if (echo) println command
    def process = command.execute()
    process.waitForOrKill(timeout * 1000)

    def text = process.text

    if (process.exitValue()) {
      println "Running '${command}' failed with error code ${process.exitValue()}"
      throw new RuntimeException("${process.exitValue()}")
    }
    else {
      if (echo) {
        if (text.size()) {
          println text
        }
        else {
          println "\tSucceeded"
        }
      }

      text
    }
  }

  public static void main(String[] args) {
    def desiredRegion = "New York 1"
    def desiredKey = "mcjoe@jeklsoft.com"
    def desiredSize = "4GB"
    def hadoop_nn = "hadoop-nn"
    def hadoop_dn1 = "hadoop-dn1"
    def hadoop_dn2 = "hadoop-dn2"
    def hadoop_dn3 = "hadoop-dn3"
    def nodes = [hadoop_nn, hadoop_dn1, hadoop_dn2, hadoop_dn3]

    def creator = new ClusterCreator()

    def command = ''
    if (args.size() != 0) {
      command = args[0]
    }

    switch (command) {
    case "create":
    case "c":
      creator.createAllDroplets(desiredRegion, desiredKey, desiredSize, nodes)
      break
    case "destroy":
    case "d":
      creator.destroyAllDroplets
      break
    case "network":
    case "n":
      creator.updateNetworkAccessSettings
      break
    default:
      println "Usage:\n\tgroovy ClusterCreator.groovy (create|c)|(destroy|d)|(network)|n)"
    }
  }
}
