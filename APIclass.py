import numpy as np
import pandas as pd
import pymssql
import numpy as np
import matplotlib.pyplot as plt
from requests.exceptions import HTTPError
import requests
import json


class APIDATA(object):
    def __init__(self):
        super().__init__()
        self.http_proxy  = "http://192.168.10.98:8080"
        self.https_proxy = "https://192.168.10.98:8080"
        
        self.proxyDict = { 
                          "http"  : self.http_proxy, 
                          "https" : self.https_proxy
                          }
        
        self.server = "https://192.168.20.228"
        self.username = "korawat"
        self.password = "Jiqcl71?"
        
    def get_data(self,API_ENDPOINT, header=None):
        with requests.Session() as s:
            try:
                r = s.get(API_ENDPOINT, verify=False, headers=header)        
        
                # If the response was successful, no Exception will be raised
                r.raise_for_status()
            except HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')  
            except Exception as err:
                print(f'Other error occurred: {err}')  
            else:
                print('Success!')
        return r.content
    
    def post_data(self,API_ENDPOINT, json_data, header=None):
        with requests.Session() as s:
            try:
                r = s.post(API_ENDPOINT, json=json_data, verify=False, headers=header)
        
                # If the response was successful, no Exception will be raised
                r.raise_for_status()
            except HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')  
            except Exception as err:
                print(f'Other error occurred: {err}')  
            else:
                print('Success!')
        return r.content
    
    def generate_token(self):
        login_endpoint = "https://192.168.20.229/api/v1/auth/login"
        json_data = {
                "username": self.username,
                "password": self.password,
                "showActualError": True
        }
        json_str0 = self.post_data(login_endpoint, json_data)
        json_data0 = json.loads(json_str0)
        myToken = json_data0["access_token"]
        head = {'Authorization': 'token {}'.format(myToken)}
        return head
    
    def get_api_type(self, api_url):
        method = None
        if 'info' in api_url:
            method = "GET"
        else:
            method = "POST"
        return method
    
    def get_api_data(self,api_url,payload):
        head = self.generate_token()
        API_ENDPOINT = self.server + api_url
        
        api_type = self.get_api_type(api_url)
        data_df = None
        
        if api_type == "GET":
            json_str = self.get_data(API_ENDPOINT, header=head)
            
            try:
                json_data = json.loads(json_str)["result"]["data"]
                data_df = pd.DataFrame(json_data)
            except KeyError:
                print("Load Data Failed !!!!!!!!!!!!!!!!!!!! from the API "+ api_url +" between "+ str(payload) )
            
        else:
            json_str = self.post_data(API_ENDPOINT,payload, header=head)

            try:
                json_data = json.loads(json_str)["result"]["data"]
                data_df = pd.DataFrame(json_data)
            except KeyError:
                print("Load Data Failed !!!!!!!!!!!!!!!!!!!! from the API "+ api_url +" between "+ str(payload) )

        return data_df
    

class APIDATA_PRODUCTION(object):
    def __init__(self):
        super().__init__()
        self.http_proxy  = "http://192.168.10.98:8080"
        self.https_proxy = "https://192.168.10.98:8080"
        
        self.proxyDict = { 
                          "http"  : self.http_proxy, 
                          "https" : self.https_proxy
                          }
        
        self.server = "https://192.168.50.41:8443"
        self.username = "korawat"
        self.password = "Jiqcl71?"
        
    def get_data(self,API_ENDPOINT, header=None):
        with requests.Session() as s:
            try:
                r = s.get(API_ENDPOINT, verify=False, headers=header)        
        
                # If the response was successful, no Exception will be raised
                r.raise_for_status()
            except HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')  
            except Exception as err:
                print(f'Other error occurred: {err}')  
            else:
                print('Success!')
        return r.content
    
    def post_data(self,API_ENDPOINT, json_data, header=None):
        with requests.Session() as s:
            try:
                r = s.post(API_ENDPOINT, json=json_data, verify=False, headers=header)
        
                # If the response was successful, no Exception will be raised
                r.raise_for_status()
            except HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')  
            except Exception as err:
                print(f'Other error occurred: {err}')  
            else:
                print('Success!')
        return r.content
    
    def generate_token(self):
        login_endpoint = "https://192.168.50.41:8443/api/v1/auth/login"
        json_data = {
                "username": self.username,
                "password": self.password,
                "showActualError": True
        }
        json_str0 = self.post_data(login_endpoint, json_data)
        json_data0 = json.loads(json_str0)
        myToken = json_data0["access_token"]
        head = {'Authorization': 'token {}'.format(myToken)}
        return head
    
    def get_api_type(self, api_url):
        method = None
        if 'info' in api_url:
            method = "GET"
        else:
            method = "POST"
        return method
    
    def get_api_data(self,api_url,payload):
        head = self.generate_token()
        API_ENDPOINT = self.server + api_url
        
        api_type = self.get_api_type(api_url)
        data_df = None
        
        if api_type == "GET":
            json_str = self.get_data(API_ENDPOINT, header=head)
            
            try:
                json_data = json.loads(json_str)["result"]["data"]
                data_df = pd.DataFrame(json_data)
            except KeyError:
                print("Load Data Failed !!!!!!!!!!!!!!!!!!!! from the API "+ api_url +" between "+ str(payload) )
            
        else:
            json_str = self.post_data(API_ENDPOINT,payload, header=head)

            try:
                json_data = json.loads(json_str)["result"]["data"]
                data_df = pd.DataFrame(json_data)
            except KeyError:
                print("Load Data Failed !!!!!!!!!!!!!!!!!!!! from the API "+ api_url +" between "+ str(payload) )

        return data_df
    

if __name__ == '__main__':
    ######## Test API
    
    ### GET Method
    api_url = "/api/v1/bondinfo/issue-rating"
    payload = {
                 "start_period": "2020-05-13",
                 "end_period": "2020-05-14"
              }
    api = APIDATA()
    
    df = api.get_api_data(api_url,payload)
    
    ### POST Method
    api_url2 = "/api/v1/mtm/plain-vanilla"
    payload2 = {
                 "start_period": "2020-05-13",
                 "end_period": "2020-05-14"
              }
    df2 = api.get_api_data(api_url2,payload2)
    
    
    
    