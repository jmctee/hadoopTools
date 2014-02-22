package com.jeklsoft.rest

import com.github.kevinsawicki.http.HttpRequest

class UnsecuredRestClient {
  static final def XML = 'application/xml'
  static final def JSON = 'application/json'

  def baseUrl
  def appId
  def appKey
  def user
  def password

  public def withBaseUrl(def baseUrl) {
    this.baseUrl = baseUrl
    this
  }

  public def withAppId(def appId) {
    this.appId = appId
    this
  }

  public def withAppKey(def appKey) {
    this.appKey = appKey
    this
  }

  public def withUser(def user, def password) {
    this.user = user
    this.password = password
    this
  }

  def get(def path, def type = XML) {
    if (!baseUrl) throw RuntimeException('No URL, really?')

    def url = baseUrl + path
    def client = HttpRequest.get(url)
    client.trustAllCerts()
    client.trustAllHosts()
    client.accept(type)
    if (appId) client.header('app_id', appId)
    if (appKey) client.header('app_key', appKey)
    if (user) client.basic(user, password)

    def body = client.body()
    def code = client.code()

    def response
    if (type == XML) {
      response = new XmlSlurper().parseText(body)
    }
    else {
      response = new groovy.json.JsonSlurper().parseText(body)
    }

    if (code != 200) {
      throw new RuntimeException("Received: ${client.code()} for url: ${url}")
    }

    response
  }
}
