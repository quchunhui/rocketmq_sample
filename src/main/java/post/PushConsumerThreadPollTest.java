package post;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PushConsumerThreadPollTest {
    public static void main(String[] args) {
        int threadCount = 3;
        int waitTime = 60000;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            Runnable runner = new ExecutorThread(String.valueOf(i));
            executor.execute(runner);
        }

        try {
            Thread.sleep(60000);
            executor.shutdown();
            executor.awaitTermination(waitTime, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
        }
    }


}

class ExecutorThread implements Runnable {
    private String name = "";
    private int count = 0;

    ExecutorThread(String name) {
        this.name = name;
    }

    @Override
    public void run() {
        StartPushConsumer();
    }

    private void StartPushConsumer() {
        System.out.print("Consumer Name=" + name + "\n");
        count = 0;
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("con_group_1");
        consumer.setInstanceName(UUID.randomUUID().toString());
//        //广播消费
//        consumer.setMessageModel(MessageModel.BROADCASTING);
        //集群消费
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeMessageBatchMaxSize(32);
        consumer.setNamesrvAddr("192.168.6.3:9876;192.168.6.4:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.registerMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> {
            System.out.print("list count=" + list.size() + "\n");
            for(MessageExt me : list) {
                count ++;
                System.out.print("name=" + name + ", count=" + count + ", msg=" + new String(me.getBody()) + "\n");
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        try {
            consumer.subscribe("qch_20170706", "*");
            consumer.start();
            System.out.print("Consumer started. name=" + name + "\n");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}