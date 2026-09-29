
const express = require('express');
const amqp = require('amqplib');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 3000;
const BROKER_URL =
  process.env.BROKER_URL || 'amqp://guest:guest@localhost:5672';

app.use(express.json());

let channel = null;

async function connectRabbitMQ() {
  try {
    const connection = await amqp.connect(BROKER_URL);
    channel = await connection.createChannel();

    await channel.assertQueue('order.created', {
      durable: true
    });

    console.log('Order Service connected to RabbitMQ');
  } catch (error) {
    console.error('RabbitMQ connection failed:', error.message);
    console.log('Will retry RabbitMQ connection in 5 seconds');

    setTimeout(connectRabbitMQ, 5000);
  }
}

app.get('/health', (req, res) => {
  res.json({
    service: 'order-service',
    status: 'running',
    rabbitmq: channel ? 'connected' : 'disconnected'
  });
});

app.get('/api/orders', (req, res) => {
  res.json({
    message: 'Order Service is running',
    orders: []
  });
});

app.post('/api/orders', async (req, res) => {
  try {
    const { customer, items } = req.body;

    if (!customer || !customer.trim()) {
      return res.status(400).json({
        error: 'Customer name is required'
      });
    }

    if (!Array.isArray(items) || items.length === 0) {
      return res.status(400).json({
        error: 'At least one order item is required'
      });
    }

    let total = 0;

    for (const item of items) {
      if (
        !item.productId ||
        !item.name ||
        !Number.isInteger(item.quantity) ||
        item.quantity < 1 ||
        !Number.isFinite(item.unitPrice) ||
        item.unitPrice < 0
      ) {
        return res.status(400).json({
          error: 'Invalid order item'
        });
      }

      total += item.quantity * item.unitPrice;
    }

    const order = {
      orderId: crypto.randomUUID(),
      customer: customer.trim(),
      items,
      total: Number(total.toFixed(2)),
      status: 'created',
      createdAt: new Date().toISOString()
    };

    if (!channel) {
      return res.status(503).json({
        error: 'Message broker is not connected. Please try again.'
      });
    }

    channel.sendToQueue(
      'order.created',
      Buffer.from(JSON.stringify(order)),
      { persistent: true }
    );

    console.log('New order created:', order.orderId);

    return res.status(201).json({
      message: 'Order created successfully',
      ...order
    });
  } catch (error) {
    console.error('Order creation failed:', error.message);

    return res.status(500).json({
      error: 'Failed to create order'
    });
  }
});

app.listen(PORT, () => {
  console.log(`Order Service running on port ${PORT}`);
  connectRabbitMQ();
});