from azure.servicebus import ServiceBusService, Message, Queue
import config

if __name__ == "__main__":
    bus_service = ServiceBusService(
        service_namespace=config.sb_name,
        shared_access_key_name=config.sb_key_name,
        shared_access_key_value=config.sb_key_val)
    msg = Message('{ "url" : "http://vignette4.wikia.nocookie.net/althistory/images/8/8d/Eiffel-tower.jpg", "sender" : "abc"}')
    bus_service.send_queue_message('imgprocjobs', msg)
    print('sent message ' + msg.body)
        