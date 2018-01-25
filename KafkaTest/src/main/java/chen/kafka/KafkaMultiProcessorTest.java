package chen.kafka;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableMap;

public class KafkaMultiProcessorTest {
	private static final Logger logger = Logger.getLogger(KafkaMultiProcessorTest.class);
    //订阅的topic
    private String alarmTopic="chensi";
    //brokers地址
    private String servers="192.168.116.180:9092";
    //消费group
    private String group="cs";
    //kafka消费者配置
    private Map<String, Object> consumerConfig;
    private Thread[] threads;
    //保存处理任务和线程的map
    private ConcurrentHashMap<TopicPartition, RecordProcessor> recordProcessorTasks = new ConcurrentHashMap<>();
    private ConcurrentHashMap<TopicPartition, Thread> recordProcessorThreads = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        KafkaMultiProcessorTest test = new KafkaMultiProcessorTest();
        //....省略设置topic和group的代码
        test.init();
    }

    public void init() {
    	//1.获取consumer配置
        consumerConfig = getConsumerConfig();
        logger.debug("get kafka consumerConfig: " + consumerConfig.toString());
        //创建threadsNum个线程用于读取kafka消息, 且位于同一个group中, 这个topic有12个分区, 最多12个consumer进行读取
        int threadsNum = 3;
        logger.debug("create " + threadsNum + " threads to consume kafka warn msg");
        //创建线程数组
        threads = new Thread[threadsNum];
        for (int i = 0; i < threadsNum; i++) {
            MsgReceiver msgReceiver = new MsgReceiver(consumerConfig, alarmTopic, recordProcessorTasks, recordProcessorThreads);
            Thread thread = new Thread(msgReceiver);
            threads[i] = thread;
            thread.setName("alarm msg consumer " + i);
        }
        //启动这几个线程
        for (int i = 0; i < threadsNum; i++) {
            threads[i].start();
        }
        logger.debug("finish creating" + threadsNum + " threads to consume kafka warn msg");
    }

    //销毁启动的线程
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
        //使用interrupt中断线程, 在线程的执行方法中已经设置了响应中断信号
        for (int i = 0; i < threads.length; i++) {
            threads[i].interrupt();
        }
        logger.debug("finish interrupting consumer threads");
    }

    //kafka consumer配置
    //后期写到配置文件中
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
