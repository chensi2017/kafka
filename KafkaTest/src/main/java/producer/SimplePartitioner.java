package producer;

import kafka.producer.Partitioner;

public class SimplePartitioner implements Partitioner{

	@Override
	public int partition(Object key, int numPartitions) {
		int partition = 0;

        String k = (String) key;

        partition = Math.abs(k.hashCode()) % numPartitions;

        return partition;
	}
	
}
