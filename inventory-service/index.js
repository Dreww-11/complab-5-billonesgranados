
const amqp = require('amqplib');

const BROKER_URL =
  process.env.BROKER_URL || 'amqp://guest:guest@localhost:5672';

// Sample inventory for the shopping application
const inventory = {
  p1: {
    name: 'Everyday Backpack',
    stock: 100
  },
  p2: {
    name: 'Wireless Headphones',
    stock: 50
  },
  p3: {
    name: 'Reusable Bottle',
    stock: 200
  }
};

async function startInventoryService() {
  try {
    const connection = await amqp.connect(BROKER_URL);
    const channel = await connection.createChannel();

    await channel.assertQueue('order.created', {
      durable: true
    });

    await channel.assertQueue('inventory.updated', {
      durable: true
    });

    console.log('Inventory Service connected to RabbitMQ');
    console.log('Waiting for new orders...');

    channel.consume('order.created', (message) => {
      if (!message) return;

      try {
        const order = JSON.parse(message.content.toString());
        const results = [];
        let allAvailable = true;

        for (const item of order.items || []) {
          const product = inventory[item.productId];

          if (!product || product.stock < item.quantity) {
            allAvailable = false;

            results.push({
              productId: item.productId,
              available: false,
              message: 'Product not found or insufficient stock'
            });

            continue;
          }

          product.stock -= item.quantity;

          results.push({
            productId: item.productId,
            name: product.name,
            quantityOrdered: item.quantity,
            remainingStock: product.stock,
            available: true
          });
        }

        const update = {
          orderId: order.orderId,
          status: allAvailable ? 'updated' : 'insufficient_stock',
          items: results,
          updatedAt: new Date().toISOString()
        };

        channel.sendToQueue(
          'inventory.updated',
          Buffer.from(JSON.stringify(update)),
          { persistent: true }
        );

        console.log(
          `Inventory processed for order ${order.orderId}: ${update.status}`
        );

        channel.ack(message);
      } catch (error) {
        console.error('Inventory processing failed:', error.message);
        channel.nack(message, false, false);
      }
    });
  } catch (error) {
    console.error('Inventory Service connection failed:', error.message);
    console.log('Retrying in 5 seconds...');

    setTimeout(startInventoryService, 5000);
  }
}

startInventoryService();