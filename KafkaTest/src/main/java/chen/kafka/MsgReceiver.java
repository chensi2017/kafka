package chen.kafka;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

public class MsgReceiver implements Runnable{
	private static final Logger logger = Logger.getLogger(MsgReceiver.class);
	private BlockingQueue<Map<TopicPartition, OffsetAndMetadata>> commitQueue = new LinkedBlockingQueue<>();
    private Map<String, Object> consumerConfig;
    private String alarmTopic;
    private ConcurrentHashMap<TopicPartition, RecordProcessor> recordProcessorTasks;
    private ConcurrentHashMap<TopicPartition, Thread> recordProcessorThreads;
    public MsgReceiver(Map<String, Object> consumerConfig, String alarmTopic,
                       ConcurrentHashMap<TopicPartition, RecordProcessor> recordProcessorTasks,
                       ConcurrentHashMap<TopicPartition, Thread> recordProcessorThreads) {
        this.consumerConfig = consumerConfig;
        this.alarmTopic = alarmTopic;
        this.recordProcessorTasks = recordProcessorTasks;
        this.recordProcessorThreads = recordProcessorThreads;
    }

    @Override
    public void run() {
        //kafka Consumer是非线程安全的,所以需要每个线程建立一个consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);
        consumer.subscribe(Arrays.asList(alarmTopic));
        //检查线程中断标志是否设置, 如果设置则表示外界想要停止该任务,终止该任务
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    //查看该消费者是否有需要提交的偏移信息, 使用非阻塞读取
                    Map<TopicPartition, OffsetAndMetadata> toCommit = commitQueue.poll();
                    if (toCommit != null) {
                        logger.debug("commit TopicPartition offset to kafka: " + toCommit);
                        consumer.commitSync(toCommit);
                    }
                    //最多轮询100ms
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    if (records.count() > 0) {
                        logger.debug("poll records size: " + records.count());
                    }

                    for (final ConsumerRecord<String, String> record : records) {
                        String topic = record.topic();
                        int partition = record.partition();
                        TopicPartition topicPartition = new TopicPartition(topic, partition);
                        RecordProcessor processTask = recordProcessorTasks.get(topicPartition);
                        //如果当前分区还没有开始消费, 则就没有消费任务在map中
                        if (processTask == null) {
                            //生成新的处理任务和线程, 然后将其放入对应的map中进行保存
                            processTask = new RecordProcessor(commitQueue);
                            recordProcessorTasks.put(topicPartition, processTask);
                            Thread thread = new Thread(processTask);
                            thread.setName("Thread-for " + topicPartition.toString());
                            logger.info("start Thread: " + thread.getName());
                            thread.start();
                            recordProcessorThreads.put(topicPartition, thread);
                        }
                        //将消息放到
                        processTask.addRecordToQueue(record);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.warn("MsgReceiver exception " + e + " ignore it");
                }
            }
        } finally {
            consumer.close();
        }
    }
}
