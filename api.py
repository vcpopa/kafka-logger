from fastapi import FastAPI
import uvicorn
import ssl
from kafka import KafkaProducer
import json


    
app = FastAPI()

@app.on_event("startup")
def startup():
    global kafka_node
    
    kafka_host='desktop-plsjgtd-docker-desktop'
    kafka_port = 29092

    kafka_node=KafkaProducer(bootstrap_servers=f'{kafka_host}:{kafka_port}',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'),batch_size=0,acks='all')

@app.post("/kafka-prod")
async def kafka_producer_(pipeline:str, org:str, message:str, status:int) -> None:
    message={
        'pipeline': pipeline,
        'org': org,
        "message": message,
        'status':int(status)
    }
    kafka_node.send('test_topic', value=message)
    print(message)


if __name__ == "__main__":
    ssl_keyfile = r"local_key.key"
    ssl_certfile = r"local_cert.crt"

    ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
    ciphers = ctx.get_ciphers()

    v12ciphers = ":".join(
        [cipher["name"] for cipher in ciphers if cipher["protocol"] == "TLSv1.2"]
    )

    uvicorn.run(
        app=app,
        host="laptop-victor.kooka-bonito.ts.net",
        port=3000,
        ssl_keyfile=ssl_keyfile,
        ssl_certfile=ssl_certfile,
        ssl_ciphers=v12ciphers,
    )
