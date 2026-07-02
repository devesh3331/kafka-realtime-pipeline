from kafka import KafkaProducer
from faker import Faker
import json
import time
import random

# Kafka se connect karo
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
# value_serializer matlab: Python dict → JSON string → bytes
# Kafka sirf bytes samajhta hai, plain Python dict nahi

fake = Faker('en_IN')  # Indian fake data

PRODUCTS = [
    {"name": "iPhone 15", "category": "Mobile", "price": 79999},
    {"name": "Samsung TV", "category": "Electronics", "price": 45999},
    {"name": "Nike Shoes", "category": "Fashion", "price": 8999},
    {"name": "Laptop Dell", "category": "Computing", "price": 65999},
    {"name": "AirPods Pro", "category": "Audio", "price": 24999},
    {"name": "Kurta Set", "category": "Fashion", "price": 1299},
    {"name": "Rice Cooker", "category": "Kitchen", "price": 2499},
]

def generate_order():
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 3)
    return {
        "order_id": fake.uuid4(),
        "customer_name": fake.name(),
        "customer_city": fake.city(),
        "product": product["name"],
        "category": product["category"],
        "price_per_unit": product["price"],
        "quantity": quantity,
        "total_amount": product["price"] * quantity,
        "status": random.choice(["placed", "confirmed", "processing"]),
        "timestamp": fake.iso8601()
    }

print("🚀 Producer shuru ho gaya - Kafka ko orders bhej raha hai...")
print("Band karne ke liye Ctrl+C dabao\n")

count = 0
while True:
    order = generate_order()
    producer.send('orders', value=order)
    count += 1
    print(f"Order #{count} bheja → {order['product']} | "
          f"₹{order['total_amount']:,} | {order['customer_city']}")
    time.sleep(1)  # Har 1 second mein ek order