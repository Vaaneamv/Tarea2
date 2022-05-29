const express = require("express");
const { Kafka } = require('kafkajs')
const cors = require("cors");
const app = express();
app.use(cors());
app.use(express.json());

const port = process.env.PORT || 5000;
const kafka = new Kafka({
  brokers: [process.env.kafkaHost]
})
let blocked={"users-blocked":[]};
app.get("/blocked", async (req, res) => {
  res.send(blocked);  
});

let registro=[];
const consumer = kafka.consumer({ groupId: 'tarea2'})
const consumerStart = async () => {
  await consumer.connect({groupId:'tarea2'})
  await consumer.subscribe({ topic: 'canal', fromBeginning: true })
  await consumer.run({
    eachMessage: async ({ message }) => {
      let mensaje=JSON.parse(`${message.value}`)
      registro.push(mensaje)
      filtro=registro.filter(x => x.user==mensaje["user"])
      if(blocked["users-blocked"].includes(mensaje["user"])==false){
        if(filtro.length>=5)
        {
          if(((filtro[filtro.length-1].hora)-(filtro[filtro.length-5].hora))<60000)
          {
            blocked["users-blocked"].push(mensaje["user"])
          }
        }
      } 
    },
  })
}
consumerStart().catch(console.error)

app.listen(port, () => {
  console.log(`API RUN AT http://localhost:${port}`);
});
