<?php

class OrderConsumer
{
    private RdKafka\KafkaConsumer $consumer;
    private array $orderBook = ['buy' => [], 'sell' => [['amount' => 0.2, 'price' => 10]]];

    public function __construct(string $topic = "orders")
    {
        $conf = new RdKafka\Conf();
        $conf->set('bootstrap.servers', 'kafka:9092');
        $conf->set('group.id', 'order-matcher');
        $conf->set('metadata.broker.list', 'kafka:9092');
        $conf->set('auto.offset.reset', 'earliest');
        $this->consumer = new RdKafka\KafkaConsumer($conf);
        $this->consumer->subscribe([$topic]);
    }

    public function run(): void
    {
        echo "Listening for orders...\n";
        while (true) {
            $message = $this->consumer->consume(1000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    echo "✅ Message received: " . $message->payload . PHP_EOL;
                    $order = json_decode($message->payload, true);
                    $this->addOrder($order);
                    $this->matchOrders();
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo "⏳ No more messages in partition... waiting\n";
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "⏱ Timed out... retrying\n";
                    break;
                default:
                    echo "❌ Error: " . $message->errstr() . PHP_EOL;
                    break;
            }
        }
    }

    private function addOrder(array $order): void
    {
        $type = $order['type'];
        $this->orderBook[$type][] = $order;

        usort($this->orderBook['buy'], fn ($a, $b) => $b['price'] <=> $a['price']);
        usort($this->orderBook['sell'], fn ($a, $b) => $a['price'] <=> $b['price']);
    }

    private function matchOrders(): void
    {
        $matches = [];
        echo json_encode($this->orderBook);

        while (!empty($this->orderBook['buy']) && !empty($this->orderBook['sell'])) {
            $buy = $this->orderBook['buy'][0];
            $sell = $this->orderBook['sell'][0];

            if ($buy['price'] >= $sell['price']) {
                $amount = min($buy['amount'], $sell['amount']);
                $matches[] = [
                    'price'     => $sell['price'],
                    'amount'    => $amount,
                    'timestamp' => time()
                ];

                $buy['amount'] -= $amount;
                $sell['amount'] -= $amount;

                $buy['amount'] <= 0 ? array_shift($this->orderBook['buy']) : $this->orderBook['buy'][0] = $buy;
                $sell['amount'] <= 0 ? array_shift($this->orderBook['sell']) : $this->orderBook['sell'][0] = $sell;
            } else {
                break;
            }
        }

        foreach ($matches as $trade) {
            echo "Matched: ".json_encode($trade).PHP_EOL;
            $logFile = dirname(__DIR__) . '/logs/trades.log';
            $data = json_encode($trade) . PHP_EOL;

            $result = @file_put_contents($logFile, $data, FILE_APPEND | LOCK_EX);

            if ($result === false) {
                echo "❌ Failed to write to {$logFile}\n";
                print_r(error_get_last());
            } else {
                echo "✅ Wrote to {$logFile}: {$data}";
            }
        }
    }
}
