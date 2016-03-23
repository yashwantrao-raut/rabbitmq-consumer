package fanout;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class MessageConsumer1 {
    public static void main(String[] args) throws IOException, TimeoutException {
        System.out.println("Consuming message!");
        ConnectionFactory connectionFactory=new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection=connectionFactory.newConnection();
        Channel channel=connection.createChannel();

        channel.exchangeDeclare("fanoutExchane", "fanout", true);
        String q1Name = channel.queueDeclare().getQueue();
        channel.queueBind(q1Name,"fanoutExchane","");

        Consumer consumer= new DefaultConsumer(channel){

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [x] fanoutExchane Consumer 1 Received '" + message + "'");
                try {
                    TimeUnit.SECONDS.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };

        channel.basicConsume(q1Name,false,consumer);

    }

}
