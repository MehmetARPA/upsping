from elasticsearch7 import Elasticsearch
es = Elasticsearch([{"host": "10.60.193.245", "port": 9200}])
import datetime,sys
from datetime import timedelta
sys.path.insert(1, '/opt/syntetic/shared_library/')
from tsps_create_event.tsps_event_create import createevent
import json
import warnings
warnings.filterwarnings('ignore')
import threading, queue
import subprocess

def setQueue(liste):
    q = queue.Queue()
    for i in liste:
        q.put(i)
    return q

def ping_trigger(threadName,q):
    while not q.empty():
        item = q.get()
        address=(item["_source"]["IP Address"])
        rtr=subprocess.call( "ping -c 1 -w 5  %s" % address,shell=True,stdout=open('/dev/null', 'w'),stderr=subprocess.STDOUT)
        saveIPStatus(item,rtr)
        q.task_done()

def thread_create_and_start(q,thread_count):
    for i in range(thread_count):
        worker = threading.Thread(target=ping_trigger, args=(f"Thread-{i}",q), daemon=False)
        worker.start()

def get_subelist():
    search_pipeline= {}
    response = es.search(index="proje_sube_ups_bilgileri", body=search_pipeline,from_=0, size=50)
    return response["hits"]["hits"]

thread_create_and_start(setQueue(get_subelist()),25)

def saveIPStatus(item,rtr):
    dt= datetime.datetime.now().replace(microsecond=0).isoformat()
    json_data = {
    "Branch Code": "{}".format(item["_source"]["Branch Code"]),
    "Branch Name": "{}".format(item["_source"]["Branch Name"]),
    "IP Address": "{}".format(item["_source"]["IP Address"]),
    "LocatIon-1": float(item["_source"]["LocatIon-1"].replace(",",".")),
    "LocatIon-2": float(item["_source"]["LocatIon-2"].replace(",",".")),
    "status":rtr,
    "timestamp":dt
    }
   # dt= datetime.datetime.now().replace(microsecond=0).isoformat()
    response = es.index(index="sube_ups_erisim_durum",doc_type='_doc', body=json_data)
    print(json_data)
