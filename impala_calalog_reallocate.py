import json
import os.path
import random
import requests
import logutil
from datetime import datetime
from time import sleep
from requests.auth import HTTPBasicAuth


def get_hosts():
    host_url = cmapi + '/hosts'
    resp = requests.get(url=host_url, auth=HTTPBasicAuth(user, password))
    hostinfo = json.loads(resp.text)
    host_list = []
    items = hostinfo["items"]
    for i in range(len(items)):
        item = {}
        item["hostId"] = items[i]["hostId"]
        item["hostname"] = items[i]["hostname"]
        host_list.append(item)
    return host_list


def get_roleinfo():
    impala_url = cmapi + '/clusters/Cluster%201/services/impala/roles'
    resp = requests.get(url=impala_url, auth=HTTPBasicAuth(user, password))
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


def stop_role(rolename):
    stop_url = cmapi + '/clusters/Cluster%201/services/impala/roleCommands/stop'
    session = requests.session()
    session.auth = (user, password)
    items = '{"items":[ "' + str(rolename) + '"]}'
    print(items)
    session.headers.update({'Content-Type': 'application/json'})
    cont = session.post(url=stop_url, data=items)
    if cont.status_code == 200:
        logger.info(rolename + "--停止成功！")
    else:
        logger.info("停止有异常需处理~")


def delete_role(rolename):
    delete_url = cmapi + '/clusters/Cluster%201/services/impala/roles/' + rolename
    print(delete_url)
    session = requests.session()
    session.auth = (user, password)
    session.headers.update({'Content-Type': 'application/json'})
    cont = session.delete(url=delete_url)
    if cont.status_code == 200:
        logger.info(rolename + "--删除成功！")
    else:
        logger.info("删除有异常需处理~")


def create_role(roletype, hostId, hostname):
    create_url = cmapi + '/clusters/Cluster%201/services/impala/roles'
    session = requests.session()
    session.auth = (user, password)
    items = '{"items":[{"type": "' + str(roletype) + '", "hostRef": {"hostId": "' + str(
        hostId) + '", "hostname": "' + str(
        hostname) + '"}, "roleConfigGroupRef": {"roleConfigGroupName": "impala-' + str(roletype) + '-BASE"}}]}'
    print(items)
    session.headers.update({'Content-Type': 'application/json'})
    cont = session.post(url=create_url, data=items)
    if cont.status_code == 200:
        logger.info(str(roletype) + "--创建成功！")
    else:
        logger.info("创建有异常需处理~")


def start_role(rolename):
    start_url = cmapi + '/clusters/Cluster%201/services/impala/roleCommands/start'
    session = requests.session()
    session.auth = (user, password)
    items = '{"items":["' + str(rolename) + '"]}'
    print(items)
    session.headers.update({'Content-Type': 'application/json'})
    cont = session.post(url=start_url, data=items)
    if cont.status_code == 200:
        logger.info(rolename + "--启动成功！")
    else:
        logger.info("启动有异常需处理~")


def restart_role(rolename):
    start_url = cmapi + '/clusters/Cluster%201/services/impala/roleCommands/restart'
    session = requests.session()
    session.auth = (user, password)
    items = '{"items":["' + str(rolename) + '"]}'
    print(items)
    session.headers.update({'Content-Type': 'application/json'})
    cont = session.post(url=start_url, data=items)
    if cont.status_code == 200:
        logger.info(rolename + "--重启成功！")
    else:
        logger.info("重启有异常需处理~")


def restart_service(service):
    restart_url = cmapi + '/clusters/Cluster%201/services/' + str(service) + '/commands/restart'
    session = requests.session()
    session.auth = (user, password)
    session.headers.update({'Content-Type': 'application/json'})
    cont = session.post(url=restart_url)
    if cont.status_code == 200:
        logger.info(service + "--重启成功！")
    else:
        logger.info("重启有异常需处理~")


def get_host_health(host):
    host_url = cmapi + '/hosts/' + host
    resp = requests.get(url=host_url, auth=HTTPBasicAuth(user, password))
    hostinfo = json.loads(resp.text)
    hoststatus = dict(hostinfo).get("healthSummary")
    return hoststatus


if __name__ == '__main__':
    global cmapi, user, password
    cmapi = 'http://192.168.0.128:7180/api/v49'
    user = 'admin'
    password = 'admin'
    service = 'impala'
    today = datetime.today().strftime("%Y-%m-%d")
    if not os.path.exists('logs'):
        os.mkdir("logs")
    logname = 'logs/' + 'impalacatalog-' + today + '.logs'
    logger = logutil.logmodule(logname)
    catalogrole, catastatus, catahost, catahealth, staterole, statestatus, statehost, statehealth = get_roleinfo()
    print(catalogrole, catastatus, catahost, catahealth, staterole, statestatus, statehost, statehealth)
    catahostId = dict(catahost).get("hostId")
    hosthealth = get_host_health(catahostId)
    logger.info("catalog_host_health: " + hosthealth)
    # 尝试重启role
    stop_count = 0
    if catahealth == 'BAD' and statehealth != "BAD" and hosthealth != "BAD":
        while True:
            catalogrole, catastatus, catahost, catahealth, staterole, statestatus, statehost, statehealth = get_roleinfo()
            if catahealth == 'BAD':
                stop_count += 1
                if stop_count >= 4:
                    logger.info("发送重启命令 - " + str(stop_count))
                    restart_role(catalogrole)
                    stop_count = 0
                    break
            else:
                stop_count = 0
                break
            logger.info(stop_count)
            sleep(60)
    if statehealth == "BAD" and catahealth != 'BAD' and hosthealth != "BAD":
        while True:
            catalogrole, catastatus, catahost, catahealth, staterole, statestatus, statehost, statehealth = get_roleinfo()
            if statehealth == "BAD":
                stop_count += 1
                if stop_count >= 4:
                    logger.info("发送重启命令 - " + str(stop_count))
                    restart_role(staterole)
                    stop_count = 0
                    break
            else:
                stop_count = 0
                break
            logger.info(stop_count)
            sleep(60)
    # 在host列表中排除catalog节点
    host_list = get_hosts()
    host_list = [host for host in host_list if dict(host).get("hostId") != dict(catahost).get("hostId")]
    # for host in host_list:
    #     if dict(host).get("hostId") == dict(catahost).get("hostId"):
    #         host_list.remove(host)
    # 随机获取一个节点安装服务
    i = random.randint(0, len(host_list) - 1)
    hostname = host_list[i].get("hostname")
    hostId = host_list[i].get("hostId")
    logger.info("catalog、statestore服务新部署节点: " + hostname)
    # 服务状态都异常时，迁移服务
    if catahealth == 'BAD' and statehealth == "BAD" and hosthealth == "BAD":
        while True:
            catalogrole, catastatus, catahost, catahealth, staterole, statestatus, statehost, statehealth = get_roleinfo()
            if catahealth == 'BAD' and statehealth == "BAD" and hosthealth == "BAD":
                stop_count += 1
                if stop_count >= 4:
                    logger.info(stop_count)
                    # 停止实例
                    stop_role(catalogrole)
                    sleep(5)
                    stop_role(staterole)
                    sleep(5)
                    # 删除实例
                    delete_role(catalogrole)
                    sleep(5)
                    delete_role(staterole)
                    sleep(5)
                    # 创建实例
                    create_role('STATESTORE', hostId, hostname)
                    sleep(5)
                    create_role('CATALOGSERVER', hostId, hostname)
                    # 启动实例
                    sleep(10)
                    catalogrole, catastatus, catahost, catahealth, staterole, statestatus, statehost, statehealth = get_roleinfo()
                    start_role(staterole)
                    sleep(30)
                    start_role(catalogrole)
                    sleep(60)
                    restart_service(service)
                    stop_count = 0
                    break
            else:
                stop_count = 0
                break
            logger.info(stop_count)
            sleep(60)
