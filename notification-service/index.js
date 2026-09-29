
const amqp = require('amqplib');

const BROKER_URL =
  process.env.BROKER_URL || 'amqp://guest:guest@localhost:5672';

async function startNotificationService() {
  try {
    const connection = await amqp.connect(BROKER_URL);
    const channel = await connection.createChannel();

    await channel.assertQueue('order.created', {
      durable: true
    });

    await channel.assertQueue('payment.completed', {
      durable: true
    });

    console.log('Notification Service connected to RabbitMQ');
    console.log('Waiting for order and payment events...');

    // Listen for new orders
    channel.consume('order.created', (message) => {
      if (!message) return;

      try {
        const order = JSON.parse(message.content.toString());

        console.log('-----------------------------------');
        console.log('NEW ORDER NOTIFICATION');
        console.log('Order ID:', order.orderId);
        console.log('Customer:', order.customer);
        console.log('Total: $' + Number(order.total).toFixed(2));
        console.log('Status: Order received');
        console.log('-----------------------------------');

        channel.ack(message);
      } catch (error) {
        console.error('Order notification failed:', error.message);
        channel.nack(message, false, false);
      }
    });

    // Listen for completed payments
    channel.consume('payment.completed', (message) => {
      if (!message) return;

      try {
        const payment = JSON.parse(message.content.toString());

        console.log('-----------------------------------');
        console.log('PAYMENT NOTIFICATION');
        console.log('Order ID:', payment.orderId);
        console.log('Amount: $' + Number(payment.amount).toFixed(2));
        console.log('Payment Status:', payment.status);
        console.log('Processed At:', payment.processedAt);
        console.log('-----------------------------------');

        channel.ack(message);
      } catch (error) {
        console.error('Payment notification failed:', error.message);
        channel.nack(message, false, false);
      }
    });
  } catch (error) {
    console.error(
      'Notification Service connection failed:',
      error.message
    );
    console.log('Retrying in 5 seconds...');

    setTimeout(startNotificationService, 5000);
  }
}

startNotificationService();