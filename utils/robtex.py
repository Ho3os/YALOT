import requests
import json

class Robtex(object):
    """description of class"""
    def __init__(self, api_key=''):
        self.api = api_key

    def queryIP(self,ip_address):
        url = f"https://freeapi.robtex.com/ipquery/{ip_address}"
        response = requests.get(url)
    
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Error: {response.status_code}")
            return None

    
    def queryAS(self,as_number):
        url = f"https://freeapi.robtex.com/asquery/{as_number}"
        response = requests.get(url)
    
        if response.status_code == 200:
            return json.loads(response.text)["nets"]
        else:
            print(f"Error: {response.status_code}")
            return None

    def queryPDNS(self,query_domain):
        url = f"https://freeapi.robtex.com/pdns/forward/{query_domain}"
        response = requests.get(url)
    
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Error: {response.status_code}")
            return None

    def queryPDNS_reverse(ip_address):
        url = f"https://freeapi.robtex.com/pdns/reverse/{ip_address}"
        response = requests.get(url)
    
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Error: {response.status_code}")
            return None
