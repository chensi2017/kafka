package chen.kafka;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableMap;

public class KafkaMultiProcessorTest {
	private static final Logger logger = Logger.getLogger(KafkaMultiProcessorTest.class);
    //���ĵ�topic
    private String alarmTopic="chensi";
    //brokers��ַ
    private String servers="192.168.116.180:9092";
    //����group
    private String group="cs";
    //kafka����������
    private Map<String, Object> consumerConfig;
    private Thread[] threads;
    //���洦��������̵߳�map
    private ConcurrentHashMap<TopicPartition, RecordProcessor> recordProcessorTasks = new ConcurrentHashMap<>();
    private ConcurrentHashMap<TopicPartition, Thread> recordProcessorThreads = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        KafkaMultiProcessorTest test = new KafkaMultiProcessorTest();
        //....ʡ������topic��group�Ĵ���
        test.init();
    }

    public void init() {
    	//1.��ȡconsumer����
        consumerConfig = getConsumerConfig();
        logger.debug("get kafka consumerConfig: " + consumerConfig.toString());
        //����threadsNum���߳����ڶ�ȡkafka��Ϣ, ��λ��ͬһ��group��, ���topic��12������, ���12��consumer���ж�ȡ
        int threadsNum = 3;
        logger.debug("create " + threadsNum + " threads to consume kafka warn msg");
        //�����߳�����
        threads = new Thread[threadsNum];
        for (int i = 0; i < threadsNum; i++) {
            MsgReceiver msgReceiver = new MsgReceiver(consumerConfig, alarmTopic, recordProcessorTasks, recordProcessorThreads);
            Thread thread = new Thread(msgReceiver);
            threads[i] = thread;
            thread.setName("alarm msg consumer " + i);
        }
        //�����⼸���߳�
        for (int i = 0; i < threadsNum; i++) {
            threads[i].start();
        }
        logger.debug("finish creating" + threadsNum + " threads to consume kafka warn msg");
    }

    //�����������߳�
    public void destroy() {
        closeRecordProcessThreads();
        closeKafkaConsumer();
    }

    private void closeRecordProcessThreads() {
        logger.debug("start to interrupt record process threads");
        for (Map.Entry<TopicPartition, Thread> entry : recordProcessorThreads.entrySet()) {
            Thread thread = entry.getValue();
            thread.interrupt();
        }
        logger.debug("finish interrupting record process threads");
    }

    private void closeKafkaConsumer() {
        logger.debug("start to interrupt kafka consumer threads");
        //ʹ��interrupt�ж��߳�, ���̵߳�ִ�з������Ѿ���������Ӧ�ж��ź�
        for (int i = 0; i < threads.length; i++) {
            threads[i].interrupt();
        }
        logger.debug("finish interrupting consumer threads");
    }

    //kafka consumer����
    //����д�������ļ���
    private Map<String, Object> getConsumerConfig() {
        return ImmutableMap.<String,Object>builder()
                .put("bootstrap.servers", servers)
                .put("group.id", group)
                .put("enable.auto.commit", "false")
                .put("session.timeout.ms", "30000")
                .put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .put("max.poll.records", 1000)
                .build();
    }

    public void setAlarmTopic(String alarmTopic) {
        this.alarmTopic = alarmTopic;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public void setGroup(String group) {
        this.group = group;
    }
}
