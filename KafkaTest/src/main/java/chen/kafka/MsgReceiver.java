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
        //kafka Consumer�Ƿ��̰߳�ȫ��,������Ҫÿ���߳̽���һ��consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);
        consumer.subscribe(Arrays.asList(alarmTopic));
        //����߳��жϱ�־�Ƿ�����, ����������ʾ�����Ҫֹͣ������,��ֹ������
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    //�鿴���������Ƿ�����Ҫ�ύ��ƫ����Ϣ, ʹ�÷�������ȡ
                    Map<TopicPartition, OffsetAndMetadata> toCommit = commitQueue.poll();
                    if (toCommit != null) {
                        logger.debug("commit TopicPartition offset to kafka: " + toCommit);
                        consumer.commitSync(toCommit);
                    }
                    //�����ѯ100ms
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    if (records.count() > 0) {
                        logger.debug("poll records size: " + records.count());
                    }

                    for (final ConsumerRecord<String, String> record : records) {
                        String topic = record.topic();
                        int partition = record.partition();
                        TopicPartition topicPartition = new TopicPartition(topic, partition);
                        RecordProcessor processTask = recordProcessorTasks.get(topicPartition);
                        //�����ǰ������û�п�ʼ����, ���û������������map��
                        if (processTask == null) {
                            //�����µĴ���������߳�, Ȼ��������Ӧ��map�н��б���
                            processTask = new RecordProcessor(commitQueue);
                            recordProcessorTasks.put(topicPartition, processTask);
                            Thread thread = new Thread(processTask);
                            thread.setName("Thread-for " + topicPartition.toString());
                            logger.info("start Thread: " + thread.getName());
                            thread.start();
                            recordProcessorThreads.put(topicPartition, thread);
                        }
                        //����Ϣ�ŵ�
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
