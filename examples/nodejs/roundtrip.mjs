// npm ci && npm start; each run owns a fresh topic and classic consumer group.
import { Kafka, logLevel } from 'kafkajs';
import { randomUUID } from 'node:crypto';
import { createRequire } from 'node:module';
const require=createRequire(import.meta.url);
const topic='demo.nodejs.'+randomUUID().replaceAll('-','');
const kafka=new Kafka({clientId:'kafka-wire-example',brokers:(process.env.KAFKA_WIRE_BROKERS||'127.0.0.1:9092').split(','),logLevel:logLevel.ERROR,connectionTimeout:5000,requestTimeout:15000,retry:{retries:3}});
const messages=[Buffer.from('a plain line'),Buffer.from('{"id":1,"note":"json is just bytes here"}'),Buffer.from(Array.from({length:256},(_,i)=>i)),Buffer.alloc(0),Buffer.from('こんにちは · Kafka')];
const admin=kafka.admin(),producer=kafka.producer({idempotent:false}),consumer=kafka.consumer({groupId:topic+'.group',retry:{retries:3}});
let timer;
try {
 await admin.connect();await admin.createTopics({topics:[{topic,numPartitions:1,replicationFactor:1}]});
 await producer.connect();await producer.send({topic,acks:-1,messages:messages.map(value=>({key:'k',value}))});await producer.disconnect();
 await consumer.connect();await consumer.subscribe({topic,fromBeginning:true});
 const received=[];
 await new Promise(async(resolve,reject)=>{
  timer=setTimeout(()=>reject(Error('consume deadline exceeded')),30000);
  consumer.on(consumer.events.CRASH,({payload})=>reject(payload.error));
  try {await consumer.run({autoCommit:false,eachMessage:async({partition,message})=>{
   received.push({partition,...message});if(received.length>=messages.length)resolve();
  }});}catch(error){reject(error);}
 });clearTimeout(timer);
 if(received.length!==messages.length||received.some((r,i)=>r.partition!==0||r.offset!==String(i)||!r.key?.equals(Buffer.from('k'))||r.value===null||!r.value.equals(messages[i])))throw Error('byte/key/order/count mismatch');
 await consumer.commitOffsets([{topic,partition:0,offset:String(messages.length)}]);
 await consumer.stop();
 const offsets=await admin.fetchOffsets({groupId:topic+'.group',topics:[topic]});
 if(offsets[0]?.partitions[0]?.offset!==String(messages.length))throw Error('committed offset did not round trip');
 console.log(JSON.stringify({schema:'kafka-wire.client-check',version:1,client:'KafkaJS',client_version:require('kafkajs/package.json').version,language:'node',runtime:process.version,status:'passed',records:messages.length,checks:['create-topic','produce-acks','byte-fidelity','key-fidelity','partition-order','classic-group','offset-commit-fetch'],settings:{idempotence:false,compression:'none',partitions:1,security:'PLAINTEXT'}}));
} finally {clearTimeout(timer);await Promise.allSettled([consumer.disconnect(),producer.disconnect(),admin.disconnect()]);}
