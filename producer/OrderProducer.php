<?php

class OrderProducer
{
    private RdKafka\Producer $producer;
    private RdKafka\ProducerTopic $topic;

    public function __construct(string $broker = "kafka:9092", string $topicName = "orders")
    {
        try {
            $conf = new RdKafka\Conf();
            $conf->set('bootstrap.servers', $broker);
            $this->producer = new RdKafka\Producer($conf);
            $this->topic = $this->producer->newTopic($topicName);
        } catch (\Exception $e) {
            echo "Error creating producer: " . $e->getMessage();
        }
    }

    public function sendOrder(string $type, float $price, float $amount): void
    {
        $order = [
            'type'      => $type,
            'price'     => $price,
            'amount'    => $amount,
            'timestamp' => time()
        ];

        try {
            $this->topic->produce(RD_KAFKA_PARTITION_UA, 0, json_encode($order));
            // Wait for delivery
            $this->producer->flush(1000); // important!
            echo "✅ Order sent: " . json_encode($order) . PHP_EOL;
        } catch (\Exception $e) {
            echo "❌ Error sending order: " . $e->getMessage() . PHP_EOL;
        }
    }
}
