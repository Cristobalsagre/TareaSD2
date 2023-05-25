from kafka import KafkaProducer, KafkaConsumer
from threading import Thread
import time
import random
import json
import datetime

# Parámetros de Kafka
bootstrap_servers = 'kafka:9092'  # Servidores de bootstrap de Kafka
topic = 'data'  # Nombre del tema de Kafka

# Clase de dispositivo IoT
class IoTDevice(Thread):
    def __init__(self, device_id, delta_t, data_size):
        super(IoTDevice, self).__init__()
        self.device_id = device_id
        self.delta_t = delta_t
        self.data_size = data_size
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def run(self):
        start_time = datetime.datetime.now()
        
        while True:
            timestamp = time.time()
            values = self.generate_data()
            data = {
                'Timestamp': timestamp,
                'Values': values
            }
            json_data = json.dumps(data)
            self.producer.send(topic, value=json_data)
            print(f"Device {self.device_id} - Data: {json_data}")
            time.sleep(self.delta_t)

        end_time = datetime.datetime.now()
        execution_time = end_time - start_time
        with open('tiempos.txt', 'a') as file:
            file.write(f"IoTDevice - Device {self.device_id}: {execution_time.total_seconds()} seconds\n")

    def generate_data(self):
        data = [random.randint(1, 100) for _ in range(self.data_size)]
        return data

# Clase de consumidor de Kafka
class KafkaConsumerThread(Thread):
    def __init__(self):
        super(KafkaConsumerThread, self).__init__()
        self.consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, value_deserializer=lambda v: json.loads(v.decode('utf-8')))

    def run(self):
        start_time = datetime.datetime.now()
        
        self.consumer.subscribe([topic])
        for message in self.consumer:
            data = message.value
            print(f"Received data: {data}")

        end_time = datetime.datetime.now()
        execution_time = end_time - start_time
        with open('tiempos.txt', 'a') as file:
            file.write(f"KafkaConsumerThread: {execution_time.total_seconds()} seconds\n")

# Parámetros del sistema
num_devices = 2  # Número de dispositivos IoT
delta_t = 2  # Intervalo de tiempo entre envío de datos (en segundos)
data_size = 4  # Tamaño de la información a enviar

# Crear dispositivos IoT
devices = []
for i in range(num_devices):
    device = IoTDevice(i+1, delta_t, data_size)
    devices.append(device)

# Iniciar dispositivos IoT
for device in devices:
    device.start()

# Iniciar consumidor de Kafka
consumer_thread = KafkaConsumerThread()
consumer_thread.start()

# Esperar a que los dispositivos terminen (esto se ejecuta hasta que se interrumpa manualmente)
for device in devices:
    device.join()

# Esperar a que el consumidor de Kafka termine (esto se ejecuta hasta que se interrumpa manualmente)
consumer_thread.join()

# Medir y guardar el tiempo total de ejecución
total_start_time = datetime.datetime.now()

# Resto del código

total_end_time = datetime.datetime.now()
total_execution_time = total_end_time - total_start_time
