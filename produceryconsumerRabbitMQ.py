import threading
import time
import random
import json
import pika

PRINT_LOCK = threading.Lock()  # Lock para asegurar impresión secuencial

class IoTDevice(threading.Thread):
    def __init__(self, device_id, delta_t, data_size):
        super().__init__()
        self.device_id = device_id
        self.delta_t = delta_t
        self.data_size = data_size
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=f'data_{self.device_id}')

    def run(self):
        while True:
            time.sleep(self.delta_t)
            data = {
                "Device": self.device_id,
                "Timestamp": time.time(),
                "Values": [random.randint(1, 100) for _ in range(self.data_size)]
            }
            message = json.dumps(data)
            self.channel.basic_publish(exchange='', routing_key=f'data_{self.device_id}', body=message)

            with PRINT_LOCK:
                print(f"Device {self.device_id} enviando - Data: {data}")

            # Recibir el mensaje enviado por el dispositivo actual
            method, properties, body = self.channel.basic_get(queue=f'data_{self.device_id}', auto_ack=True)
            if body:
                received_data = json.loads(body)
                with PRINT_LOCK:
                    print(f"Recibiendo en Device {self.device_id} - Data: {received_data}")

    def stop(self):
        self.connection.close()

if __name__ == "__main__":
    num_devices = 10
    delta_t = 2
    data_size = 4

    devices = []

    for i in range(1, num_devices + 1):
        device = IoTDevice(i, delta_t, data_size)
        devices.append(device)
        device.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Deteniendo los dispositivos...")
        for device in devices:
            device.stop()

    for device in devices:
        device.join()

