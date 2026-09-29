
const express = require('express');
const cors = require('cors');

const app = express();
const PORT = 8080;

const ORDER_SERVICE_URL =
  process.env.ORDER_SERVICE_URL || 'http://localhost:3000';

app.use(cors({
  origin: process.env.CORS_ORIGIN || 'http://localhost:3000'
}));

app.use(express.json());

app.get('/health', (req, res) => {
  res.json({
    service: 'api-gateway',
    status: 'running'
  });
});

// Forward order requests to the Order Service
app.all('/api/orders', async (req, res) => {
  try {
    const response = await fetch(
      `${ORDER_SERVICE_URL}/api/orders`,
      {
        method: req.method,
        headers: {
          'Content-Type': 'application/json'
        },
        body: ['GET', 'HEAD'].includes(req.method)
          ? undefined
          : JSON.stringify(req.body)
      }
    );

    const data = await response.text();

    res.status(response.status);
    res.type(
      response.headers.get('content-type') || 'application/json'
    );
    res.send(data);
  } catch (error) {
    console.error('Order Service error:', error.message);

    res.status(502).json({
      error: 'Unable to connect to Order Service'
    });
  }
});

app.listen(PORT, () => {
  console.log(`API Gateway running on port ${PORT}`);
});