import json
import logging
import urllib
from urllib import parse,request
import azure.functions as func
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential


def main(event: func.EventGridEvent):
    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })

    ### Logging into EventGrid framework ####
    logging.info('Python EventGrid trigger processed an event: %s', result)

    ### This is the URL to submit the CDE Airflow job. Get yours from the CDE Jobs API. Change it to your value ###
    url='https://xxxxxxxx.cde-xxxxxxxx.hrong-az.xxxx-xxxx.cloudera.site/dex/api/v1/jobs/airflow-driver/run'
    
    ### This is the URL to get the Auth token for submittng the CDE job. Ideally it is Grafana Host name + "gateway/authtkn/knoxtoken/api/v1/token". Change it to your value ###
    token_url="https://service.cde-xxxxxxxx.hrong-az.xxxx-xxxx.cloudera.site/gateway/authtkn/knoxtoken/api/v1/token"

    ### Your key vault name where your credentials secrets are stored. Change it to your value ####
    keyVaultName='<Your keyvault name>'
    KVUri = f"https://{keyVaultName}.vault.azure.net"

    ## Get credentials secrets(User id and password) from Azure Key Vault
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)

    ## Your secret name here. Change it to your value
    secretName='<Your user name>'
    retrieved_secret = client.get_secret(secretName)
    
    auth_user=secretName
    auth_password=retrieved_secret.value

    passman = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    passman.add_password(None, token_url, auth_user, auth_password)
    authhandler = urllib.request.HTTPBasicAuthHandler(passman)
    opener = urllib.request.build_opener(authhandler)
    urllib.request.install_opener(opener)
    res = urllib.request.urlopen(token_url)
    res_body = res.read()
    json_res_body=json.loads(res_body.decode('utf-8'))

    ## Retrieve the access_token which should be passed into the Jobs API Header for the CDE job
    bearer_token=json_res_body['access_token']


    ## prep the headers required to submit the Airflow CDE job using the CDE Jobs API
    headers = {"accept": "application/json","Content-Type": "application/json","Authorization": "Bearer {0}".format(bearer_token)}
    payload = {"hidden": True,"overrides": { "airflow": { "config": { "additionalProp1": "string", "additionalProp2": "string", "additionalProp3": "string" } }, "runtimeImageResourceName": "string", "spark": { "args": [ "string" ], "className": "string", "conf": { "additionalProp1": "string", "additionalProp2": "string", "additionalProp3": "string" }, "driverCores": 0, "driverMemory": "string", "executorCores": 0, "executorMemory": "string", "file": "string", "files": [ "string" ], "jars": [        "string"      ],      "logLevel": "string",      "name": "string",      "numExecutors": 0,      "proxyUser": "string",      "pyFiles": [        "string"      ],      "pythonEnvResourceName": "string"    }  },  "user": "string",  "variables": {    "additionalProp1": "string",    "additionalProp2": "string",    "additionalProp3": "string"  }}

    data=json.dumps(payload)
    data=str(data)
    data=data.encode('utf-8')

    ## Submit the CDE Airflow job
    req=request.Request(url,data=data, headers=headers)
    resp = request.urlopen(req)
    
