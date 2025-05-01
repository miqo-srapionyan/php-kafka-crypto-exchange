<?php
require 'OrderProducer.php';

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $type = $_POST['type'] ?? null;
    $price = (float)($_POST['price'] ?? 0);
    $amount = (float)($_POST['amount'] ?? 0);

    if (!$type || !$price || !$amount) {
        http_response_code(400);
        echo "Missing fields.";
        exit;
    }

    $producer = new OrderProducer();
    $producer->sendOrder($type, $price, $amount);
} else {
    echo "Use POST method.";
}
