package post;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.UUID;

public class Producer {
    public static void main(String[] args) {
        //生成Producer
        DefaultMQProducer producer = new DefaultMQProducer("pro_qch_test");
        //配置Producer
        producer.setNamesrvAddr("192.168.6.3:9876;192.168.6.4:9876");
        producer.setInstanceName(UUID.randomUUID().toString());
        //启动Producer
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
            return;
        }

        //生产消息
        String str = "Hello RocketMQ!----" + UUID.randomUUID().toString();
        Message msg = new Message("qch_20170706", str.getBytes());
        try {
            producer.send(msg);
        } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
            e.printStackTrace();
            return;
        }

        //停止Producer
        producer.shutdown();
        System.out.print("[----------]Succeed\n");
    }
}