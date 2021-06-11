/*jshint esversion: 8*/
const { Kafka } = require('kafkajs');
var rest = require("express")();

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
});

rest.get("/pump/:msg", (query, response) => {
    const pump = async () => {
        const producer = kafka.producer();
        await producer.connect();
        await producer.send({
            topic: 'test-topic',
            autoCommit: true,
            messages: [
              { value: query.params.msg },
            ],
          });
    };

    pump().then(() => response.send("done"));
});

rest.listen(8000);

/*
Joulukuu: 3h

*/
/*
(async () => {
    console.log("next reply log");
    const consumer = kafka.consumer({ groupId: 'test-group' })
    await consumer.connect();
    await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });
    consumer.run({
        eachMessage: ({topic, partition, message}) => {
            console.log(message.value.toString());
        }
    });
})();*/
rest.get("/", (req,res) => res.sendFile("static/kafka.html", {root: __dirname}));

(async () => {
    const consumer = kafka.consumer({ groupId: 'test-group' });
    await consumer.connect();
    await consumer.subscribe({ topic: 'test-topic', fromBeginning: false });

    let latest = [];
    rest.get("/pull", (query, response) => {
        response.send(latest);
        latest = [];
    });
    consumer.run({
        eachMessage: ({topic, partition, message}) => {
            console.log(message.value.toString());
            latest.push(message.value.toString());
        }
    });
    console.log("subscribed");
})();