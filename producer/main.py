from faker import Faker
from confluent_kafka import SerializingProducer

import json
import simplejson
import random
import time
from datetime import datetime

fake = Faker()

def generate_sales_transactiions():
    user = fake.simple_profile()

    return{
        "transactionId": fake.uuid4(),
        "productId": random.choice(['p1', 'p2', 'p3', 'p4', 'p5', 'p6']),
        "productName": random.choice(['computer', 'phone', 'case', 'mouse', 'headphone', 'powerbank']),
        'productCategory': random.choice(['electronics', 'apparel', 'grocery', 'home', 'beauty', 'sports']),
        'productPrice': round(random.uniform(10, 1000), 2),
        'productQuantity': random.randint(1, 10),
        'productBrand': random.choice(['apple', 'samsung', 'oneplus', 'mi', 'boat', 'sony']),
        'currency': random.choice(['USD', 'INR']),
        'customerId': user['username'],
        'transactionDate': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "paymentMethod": random.choice(['credit_card', 'debit_card', 'wire_transfer'])
    }


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f"Message delivered to {msg.topic} [{msg.partition()}]")


def main():
    topic = 'transactions'
    producer = SerializingProducer({
        'bootstrap.servers' : 'localhost:9092'
    })

    current_time = datetime.now()

    while (datetime.now() - current_time).seconds < 120:
        try:
            transaction = generate_sales_transactiions()
            transaction['totalAmount'] = transaction['productPrice'] * transaction['productQuantity']
            print(transaction)
            producer.produce(topic,
                             key=transaction['transactionId'],
                             value=json.dumps(transaction),
                             on_delivery=delivery_report
                             )
            producer.poll(0)
            time.sleep(5)
        except BufferError:
            print("Buffer full! Waiting...")
            time.sleep(1)
        except Exception as e:
            print(e)

    producer.flush()

if __name__ == "__main__":
    main()