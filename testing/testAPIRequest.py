
# REF: python Locust for load testing of API

import requests
import json

URL_XML = "http://address"
def test_xml_post_request(ids):
    for id in ids: 
        ifile = open("data/request_{}.xml".format(id), "r")
        xml_input = ifile.read()
        #print(xml_input)
        headers = {'Content-type': 'application/xml'}
        response  = requests.post(url=URL_XML,data=xml_input,headers=headers)
        print('ID: {}'.format(id))
        if response.ok:
            print(response.json())
        else:
            print("response not okay. status code: {}".format(response.status_code))


URL_JSON = "http://address"
def test_json_post_request(ids):
  for id in ids:
    ifile =  open('data/requset_{}.json'.format(id), 'r')
    json_input = ifile.read()	
    request_json = json.loads(json_input)
    headers = {'Content-type': 'application/json',}
    proxies = {
         "http": "http://address",
         "https":"http://address",
        }
    response = requests.request("POST", url=URL_JSON, 
                                json=request_json,
                                headers=headers,
                                proxies=proxies,
                            )							
    if response.ok:
      print("response ok")
    else:
      print("response not okay. status code: {}".format(response.status_code))


if __name__ == "__main__":
    for i in range(6000):
      ids = #["1", "2", "3", "4", "5"]:
      test_xml_post_request(ids)
      test_json_post_request(ids)
