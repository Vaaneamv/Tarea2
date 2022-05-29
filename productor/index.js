const express = require("express");
const { Kafka } = require('kafkajs')
const cors = require("cors");
const app = express();
app.use(cors());
app.use(express.json());

const port = process.env.PORT || 3000;
const kafka = new Kafka({
  brokers: [process.env.kafkaHost]
})

const productor = kafka.producer({ groupId: 'tarea2'});

const run = async () => {
  await productor.connect()
}
run().catch(console.error)

app.post('/login', async(req, res) => {
  console.log(req.body)
  console.log(Date.now())
  req.body.hora=Date.now()
  await productor.send({
    topic: 'canal',
    messages: [
      { value: JSON.stringify(req.body) },
    ],
  })
  res.send('hello world');
})

app.listen(port, () => {
  console.log(`API RUN AT http://localhost:${port}`);
});

