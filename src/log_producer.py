import time
import random
from kafka import KafkaProducer
from datetime import datetime

KAFKA_TOPIC = 'user_logs'
KAFKA_BROKER = 'localhost:9092'

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'Mozilla/5.0 (Linux; Android 10; Pixel 3 XL) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Mobile Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1.38'
]

def generate_log():
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ip = f"192.168.0.{random.randint(1, 255)}"
    user_agent = random.choice(USER_AGENTS)
    return f"{timestamp},{ip},{user_agent}"

def produce_logs():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
    try:
        while True:
            log = generate_log()
            producer.send(KAFKA_TOPIC, value=log.encode('utf-8'))
            print(f"Produced log: {log}")
            time.sleep(1)  
    except KeyboardInterrupt:
        print("Log production stopped.")
    finally:
        producer.close()

if __name__ == "__main__":
    produce_logs()