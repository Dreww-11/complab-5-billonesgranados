
const amqp = require('amqplib');

const BROKER_URL =
  process.env.BROKER_URL || 'amqp://guest:guest@localhost:5672';

async function startPaymentService() {
  try {
    const connection = await amqp.connect(BROKER_URL);
    const channel = await connection.createChannel();

    await channel.assertQueue('order.created', {
      durable: true
    });

    await channel.assertQueue('payment.completed', {
      durable: true
    });

    console.log('Payment Service connected to RabbitMQ');
    console.log('Waiting for new orders...');

    channel.consume('order.created', (message) => {
      if (!message) return;

      try {
        const order = JSON.parse(message.content.toString());

        console.log('Processing payment for order:', order.orderId);

        const payment = {
          orderId: order.orderId,
          customer: order.customer,
          amount: order.total,
          status: 'completed',
          processedAt: new Date().toISOString()
        };

        channel.sendToQueue(
          'payment.completed',
          Buffer.from(JSON.stringify(payment)),
          { persistent: true }
        );

        console.log(
          `Payment completed for ${order.orderId}: $${order.total}`
        );

        channel.ack(message);
      } catch (error) {
        console.error('Payment processing failed:', error.message);
        channel.nack(message, false, false);
      }
    });
  } catch (error) {
    console.error('Payment Service connection failed:', error.message);
    console.log('Retrying in 5 seconds...');

    setTimeout(startPaymentService, 5000);
  }
}

startPaymentService();