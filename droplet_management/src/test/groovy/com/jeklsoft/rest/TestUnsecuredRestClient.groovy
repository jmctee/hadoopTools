package com.jeklsoft.rest

import org.junit.Before
import org.junit.Ignore
import org.junit.Test

@Ignore
class TestUnsecuredRestClient {

  def baseUrl
  def appId
  def appKey
  def clientKey
  def apiKey

  @Before
  public void init() {
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
    appId = properties['appId']
    appKey = properties['appKey']
    clientKey = properties['clientKey']
    apiKey = properties['apiKey']
  }

  @Test
  public void testGetAvailableImages() {
    def url = "images/?client_id=${clientKey}&api_key=${apiKey}&filter=my_images"

    def client = new UnsecuredRestClient()

    def response

    if (appId) {
      response = client.withBaseUrl(baseUrl)
              .withAppId(appId)
              .withAppKey(appKey)
              .get(url, UnsecuredRestClient.JSON)
    }
    else {
      response = client.withBaseUrl(baseUrl)
              .get(url, UnsecuredRestClient.JSON)
    }

    println response
  }
}