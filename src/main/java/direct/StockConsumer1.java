package direct;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import direct.data.Offer;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class StockConsumer1 {

    public static final String QNAME = "DD_Q";

    public static void main(String[] args) throws IOException, TimeoutException {
        System.out.println("Consuming message!");
        ConnectionFactory connectionFactory=new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection=connectionFactory.newConnection();
        Channel channel=connection.createChannel();
        int prefetchCount = 1;
        channel.basicQos(prefetchCount);

        channel.exchangeDeclare("directExchane", "direct", true);
        channel.queueDeclare(QNAME, true, false, false, null);
        channel.queueBind(QNAME, "directExchane", "DD");

        Consumer consumer= new DefaultConsumer(channel){

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {

                ObjectMapper objectMapper =new ObjectMapper();
                Offer offer=objectMapper.readValue(body,Offer.class);
                System.out.println("Received Offer :: "+offer);

                String message = new String(body, "UTF-8");
                System.out.println(" [x] DD Consumer 1 Received :'" + message + "'");
                try {
                    TimeUnit.SECONDS.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };

        channel.basicConsume(QNAME,false,consumer);

    }

}
