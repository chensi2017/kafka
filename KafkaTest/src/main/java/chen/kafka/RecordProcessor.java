package chen.kafka;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

public class RecordProcessor implements Runnable{
	private static Logger logger = Logger.getLogger(RecordProcessor.class);

    //����MsgReceiver�̷߳��͹�������Ϣ
    private BlockingQueue<ConsumerRecord<String, String>> queue = new LinkedBlockingQueue<>();
    //������consumer�߳��ύ����ƫ�ƵĶ���
    private BlockingQueue<Map<TopicPartition, OffsetAndMetadata>> commitQueue;
    //��һ���ύʱ��
    private LocalDateTime lastTime = LocalDateTime.now();
    //������20������, �ͽ���һ���ύ
    private long commitLength = 20L;
    //������һ���ύ���, ���ύһ��
    private Duration commitTime = Duration.ofSeconds(2);
    //��ǰ���߳����ѵ���������
    private int completeTaskNum = 0;
    //������һ�����ѵ�����
    private ConsumerRecord<String, String> lastUncommittedRecord;

    //���ڱ�������ƫ������queue, ��MsgReceiver�ṩ
    public RecordProcessor(BlockingQueue<Map<TopicPartition, OffsetAndMetadata>> commitQueue) {
        this.commitQueue = commitQueue;
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                //��ʱ�����Ƶ�poll, consumer�������ѹ����Ķ���. ÿ�������̶߳����Լ��Ķ���.
                ConsumerRecord<String, String> record = queue.poll(100, TimeUnit.MICROSECONDS);
                if (record != null) {
                    //�������
                    process(record);
                    //�����������1
                    this.completeTaskNum++;
                    //������һ�������¼
                    lastUncommittedRecord = record;
                }
                //�ύƫ�Ƹ�consumer
                commitToQueue();
            }
        } catch (InterruptedException e) {
            //�̱߳�interrupt,ֱ���˳�
            logger.info(Thread.currentThread() + "is interrupted");
        }
    }

    private void process(ConsumerRecord<String, String> record) {
        System.out.println(record);
    }

    //����ǰ������ƫ�����ŵ�queue��, ��MsgReceiver�����ύ
    private void commitToQueue() throws InterruptedException {
        //���û�����ѻ������һ�����������Ѿ��ύƫ����Ϣ, ���ύƫ����Ϣ
        if (lastUncommittedRecord == null) {
            return;
        }
        //����������趨������, ������������commitLength��Ϣ
        boolean arrivedCommitLength = this.completeTaskNum % commitLength == 0;
        //��ȡ��ǰʱ��, ���Ƿ��Ѿ�������Ҫ�ύ��ʱ��
        LocalDateTime currentTime = LocalDateTime.now();
        boolean arrivedTime = currentTime.isAfter(lastTime.plus(commitTime));
        //����������趨����, ���ߵ����趨ʱ��, ��ô�ͷ���ƫ�Ƶ�������, �������߷�����poll���ƫ����Ϣ����, �����ύ
        if (arrivedCommitLength || arrivedTime) {
            lastTime = currentTime;
            long offset = lastUncommittedRecord.offset();
            int partition = lastUncommittedRecord.partition();
            String topic = lastUncommittedRecord.topic();
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            logger.debug("partition: " + topicPartition + " submit offset: " + (offset + 1L) + " to consumer task");
            Map<TopicPartition, OffsetAndMetadata> map = Collections.singletonMap(topicPartition, new OffsetAndMetadata(offset + 1L));
            commitQueue.put(map);
            //�ÿ�
            lastUncommittedRecord = null;
        }
    }

    //consumer�߳������̵߳Ķ��������record
    public void addRecordToQueue(ConsumerRecord<String, String> record) {
        try {
            queue.put(record);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
