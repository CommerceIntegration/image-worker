from azure.servicebus import ServiceBusService, Message, Queue
import azure.storage
from azure.storage.blob import BlockBlobService
import config
import subprocess
import json
import os
import time

def get_image(url):
    filename = url.split('/')[-1]
    wget_command = ["wget", url]
    code = subprocess.call(wget_command)
    if code == 0:
        print('successfuly wgot ' + filename)
    else:
        print('wget failed')
        return None
    return filename

def process(infile):    
    if not infile:
        return None
    outfile = "out_" + infile    
    avconv_command = ["avconv", 
    "-i",
    infile,
    "-vf",
    "split[bg][orig],[bg]boxblur=5:5:5:5:5:5,scale='max(ih,iw)':'max(ih,iw)'[bgf],[bgf][orig]overlay=main_w/2-overlay_w/2:main_h/2-overlay_h/2",
    outfile]
    code = subprocess.call(avconv_command)
    if code == 0:
        print('successfuly processed ' + infile)
    else:
        print('wget failed')
        outfile = None
    os.remove(infile)
    return outfile

def handle_output(outputname):
    block_blob_service = BlockBlobService(account_name=config.storage_acc_name, account_key=config.storage_acc_key)
    block_blob_service.create_blob_from_path(config.storage_container, outputname, outputname)
    

if __name__ == "__main__":
    bus_service = ServiceBusService(
        service_namespace=config.sb_name,
        shared_access_key_name=config.sb_key_name,
        shared_access_key_value=config.sb_key_val)
    while True:
        msg = bus_service.receive_queue_message(config.q_name, peek_lock=False, timeout=60)
        if msg.body:
            print('got new messgae: ' + msg.body)
            process_params = json.loads(msg.body)
            filename = get_image(process_params['url'])
            processed = process(filename)
            handle_output(processed)
            bus_service.send_topic_message(config.topic_name, Message(json.dumps(process_params)))
        else:
            print('no new msg in queue')