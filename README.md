# 🪙 PHP Kafka Crypto Order Matching Simulator

A simple simulation of a crypto exchange order producer and consumer using **PHP** and **Apache Kafka**, built for educational and demo purposes.

This project demonstrates how a producer can send mock trade orders to a Kafka topic, and how a consumer can process them in real-time — mimicking a basic order matching engine.

---

## 📦 Tech Stack

- **PHP 8+**
- **librdkafka** via [php-rdkafka](https://github.com/arnaud-lb/php-rdkafka)
- **Apache Kafka** with Docker
- **Zookeeper** for Kafka coordination
- **Docker Compose**

---

## 🛠 Features

- Produce fake crypto trade orders (buy/sell) with price and amount
- Consume and display live order data from a Kafka topic
- Clean, class-based PHP code
- Ready-to-run with Docker

---

## 🚀 Getting Started

### 1. Clone the repo

```bash
git clone https://github.com/miqo-srapionyan/php-kafka-crypto-exchange.git
cd php-kafka-exchange
```

### 2. Start the services

Make sure Docker is running, then start Kafka, Zookeeper, and the PHP app container:

```bash
docker-compose up -d --build
```
This will:

- Build the PHP app container with php-rdkafka extension
- Start Kafka and Zookeeper using Docker
- Create the orders topic

### 3. Produce an order
Run the producer script inside the container to send a mock crypto trade order:
```bash
curl -X POST -d "type=buy&price=12000&amount=0.25" http://localhost:8080/index.php
```
Example output
```
✅ Order sent: {"type":"buy","price":23456.78,"amount":0.05,"timestamp":1714583200}
```

### 4. Consume orders
In a separate terminal window, run the consumer to listen to incoming orders:

```bash
docker exec -it php php consumer/run.php
```
Example output
```
✅ Message received: {"type":"buy","price":12000,"amount":0.25,"timestamp":1746100872}
```

## 📁 Project Structure

```
├── consumer/
│   └── OrderConsumer.php
│   └── run.php
├── producer/
│   └── OrderProducer.php
│   └── index.php
├── logs/
│   └── trades.log
├── docker-compose.yml
├── Dockerfile
├── README.md
├── LICENSE
```

## 📚 Notes

Topic name used: orders<br />
Kafka broker address in Docker: kafka:9092<br />
Flush is required: Call $producer->flush() after producing messages to ensure delivery to Kafka

## License

This project is licensed under the [MIT License](LICENSE).

Made with ❤️ by [Mikayel Srapionyan](https://github.com/miqo-srapionyan)