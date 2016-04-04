This Repository contains Rabbitmq consumer to consume messages from Q.

Below are learning.
Rabbitmq will throw exception if a queue is exist and we are creating same named queue with different parameters.
If we declare queue with autodelete true then Q will be deleted when all the consumer removed from it.
If we created durable Q with auto delete true then Q will be deleted when all the consumer removed from Q.
Rabbitmq will use ruond robbin algo if multiple consumer are listinig to the Q.
Their is a provision to configure the Q to not remove messages until the consumer asknowledges the delivery.
The publish API allow us to be configure a published message to be persistant.
Fair dispatch
rabbitmq will dispatch messages when message enter into the Q.it will not look for how many ack have received.
we can configure how many messages should be fetched at a time 
int prefetchCount = 1;
channel.basicQos(prefetchCount);
It will dispatch one message and will wait till ack receive. it will dispatch next message only it receives ack.
Always declare separate channel for consumer and publisher.
when we publish message to fanout exchange. it's routing key will be ignored and it will disptched to all listing Q
Fanout exchanges will ignore the binding keys if we bind Q to fanout exchange
If multiple Q are bind to exchange with same routing key then exchange will act like fanout. It will dispatch message to all matching Q
Should be use consumer per channel
Unroutable messages can be handled using ReturnListener
Automatic recovery from network failures can be configured on factory by using 
factory.setAutomaticRecoveryEnabled(true);
If rabbitmq node is not rechable. default it will retry after 5 seconds and can be configured using
factory.setNetworkRecoveryInterval(10000);
We can set the ExceptionHandler on ConnectionFactory basis.
We can use Lyra-High availability RabbitMQ client(https://github.com/jhalterman/lyra)


