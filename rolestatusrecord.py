import json
import requests
import logutil
from datetime import datetime
from time import sleep
from requests.auth import HTTPBasicAuth


def get_roleinfo():
    impala_url = 'http://192.168.0.128:7180/api/v49/clusters/Cluster%201/services/impala/roles'
    resp = requests.get(url=impala_url, auth=HTTPBasicAuth("admin", "admin"))
    impalainfo = json.loads(resp.text)
    items = impalainfo["items"]
    catalogrole = ''
    catastatus = ''
    catahost = ''
    staterole = ''
    statestatus = ''
    statehost = ''
    catahealth = ''
    statehealth = ''
    for i in range(len(items)):
        roletype = items[i]["type"]
        if roletype == "CATALOGSERVER":
            catalogrole = items[i]["name"]
            catastatus = items[i]["roleState"]
            catahost = items[i]["hostRef"]
            catahealth = items[i]["healthSummary"]
        if roletype == 'STATESTORE':
            staterole = items[i]["name"]
            statestatus = items[i]["roleState"]
            statehost = items[i]["hostRef"]
            statehealth = items[i]["healthSummary"]

    return catalogrole, catastatus, catahost, catahealth, staterole, statestatus, statehost, statehealth

def get_host_health(host):
    host_url = 'http://192.168.0.128:7180/api/v49/hosts/' + host
    resp = requests.get(url=host_url, auth=HTTPBasicAuth("admin", "admin"))
    hostinfo = json.loads(resp.text)
    hoststatus = dict(hostinfo).get("healthSummary")
    return hoststatus


if __name__ == '__main__':
    today = datetime.today().strftime("%Y-%m-%d")
    logname = 'rolestatus-' + today + '.logs'
    logger = logutil.logmodule(logname)
    count = 0
    while True:
        catalogrole, catastatus, catahost, catahealth, staterole, statestatus, statehost, statehealth = get_roleinfo()
        catahost = dict(catahost).get("hostId")
        hosthealth = get_host_health(catahost)
        logger.info(
            "{} {} {} {} {} {} {} {}".format(catalogrole, catastatus, catahost, catahealth, staterole, statestatus,
                                             statehost, statehealth))
        logger.info("{}".format(hosthealth))
        count += 1
        if count >= 1200:
            break
        sleep(2)
