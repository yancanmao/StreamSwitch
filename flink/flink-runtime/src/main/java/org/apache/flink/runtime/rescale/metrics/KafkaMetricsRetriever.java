package org.apache.flink.runtime.rescale.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rescale.JobRescalePartitionAssignment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaMetricsRetriever implements StreamSwitchMetricsRetriever {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsRetriever.class);
	private AtomicBoolean closed = new AtomicBoolean();
	private CountDownLatch shutdownLatch = new CountDownLatch(1);
	private String servers;

	Set<String> containerIds;

	private Configuration jobConfiguration;
	private String TOPIC;
	private static KafkaConsumer<String, String> consumer;
	private JobGraph jobGraph;
	private JobVertexID vertexID;
	private List<JobVertexID> upstreamVertexIDs = new ArrayList<>();
	private int nRecords;

	private long startTs = System.currentTimeMillis();
	private int metrcsWarmUpTime;
	private int numPartitions;

	@Override
	public void init(JobGraph jobGraph, JobVertexID vertexID, Configuration jobConfiguration, int numPartitions) {
		this.jobGraph = jobGraph;
		this.vertexID = vertexID;

		this.jobConfiguration = jobConfiguration;
		TOPIC = jobConfiguration.getString("policy.metrics.topic", "flink_metrics");
		servers = jobConfiguration.getString("policy.metrics.servers", "localhost:9092");
		nRecords = jobConfiguration.getInteger("model.retrieve.nrecords", 15);
		metrcsWarmUpTime = jobConfiguration.getInteger("model.metrics.warmup", 100);

		JobVertex curVertex = jobGraph.findVertexByID(vertexID);
		for (JobEdge jobEdge : curVertex.getInputs()) {
			JobVertexID id = jobEdge.getSource().getProducer().getID();
			upstreamVertexIDs.add(id);
		}

		this.numPartitions = numPartitions;
		initConsumer();
	}

	public void initConsumer(){
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, vertexID.toString());
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer(props);
		consumer.subscribe(Arrays.asList(TOPIC));
	}

	@Override
	public Map<String, Object> retrieveMetrics() {
		// TODO: source operator should be skipped
		// retrieve metrics from Kafka, consume topic and get upstream and current operator metrics

		Map<String, Object> metrics = new HashMap<>();

		HashMap<String, Long> partitionArrived = new HashMap<>();
		HashMap<String, Double> executorUtilization = new HashMap<>();
		HashMap<String, Long> partitionProcessed = new HashMap<>();
		HashMap<String, Boolean> partitionValid = new HashMap<>();

		HashMap<String, HashMap<String, Long>> upstreamArrived = new HashMap<>(); // store all executors, not just vertex id

		HashMap<String, Long> executorTimeStamp = new HashMap<>(); // latest timestamp of each partition, timestamp smaller than value in this should be invalid

		synchronized (consumer) {
			consumer.poll(100);
			Set<TopicPartition> assignedPartitions = consumer.assignment();
			consumer.seekToEnd(assignedPartitions);
			for (TopicPartition partition : assignedPartitions) {
//				System.out.println(vertexID + ": " + consumer.position(partition));
				long endPosition = consumer.position(partition);
				long recentMessagesStartPosition = endPosition - nRecords < 0 ? 0 : endPosition - nRecords;
				consumer.seek(partition, recentMessagesStartPosition);
				LOG.info("------------------------------");
				LOG.info("start position: " + recentMessagesStartPosition
					+ " end position: " + endPosition);
				LOG.info("------------------------------");
			}

			ConsumerRecords<String, String> records = consumer.poll(100);
			if (!records.isEmpty()) {
				// parse records, should construct metrics hashmap
				for (ConsumerRecord<String, String> record : records) {
					if (record.value().equals("")) {
						continue;
					}
					String[] ratesLine = record.value().split(",");
					JobVertexID jobVertexId = JobVertexID.fromHexString(ratesLine[0]);
					if (jobVertexId.equals(this.vertexID)) {
						retrievePartitionProcessed(ratesLine, partitionProcessed, executorUtilization,
							partitionValid, executorTimeStamp);
					}

					// find upstream numRecordOut
					if (upstreamVertexIDs.contains(jobVertexId)) {
						// get executor id
						String upstreamExecutorId = ratesLine[1];
						// put into the corresponding upstream queue, aggregate later
						HashMap<String, Long> curUpstreamArrived = upstreamArrived.getOrDefault(upstreamExecutorId, new HashMap<>());
						retrievePartitionArrived(ratesLine, curUpstreamArrived, partitionValid);
						upstreamArrived.put(upstreamExecutorId, curUpstreamArrived);
					}
				}
			}
		}

		// aggregate partitionArrived
		for (Map.Entry entry : upstreamArrived.entrySet()) {
			for (Map.Entry subEntry : ((HashMap<String, Long>) entry.getValue()).entrySet()) {
				String keygroup = (String) subEntry.getKey();
				long keygroupArrived = (long) subEntry.getValue();
				partitionArrived.put(keygroup, partitionArrived.getOrDefault(keygroup, 0l) + keygroupArrived);
			}
		}

		if (System.currentTimeMillis() - startTs < metrcsWarmUpTime*1000
			&& (partitionProcessed.size() <= 5 || partitionArrived.size() <= 5)) {
			// make all metrics be 0.0
			for (int i=0; i<numPartitions; i++) {
				String keyGroup = String.valueOf(i);
				partitionArrived.put(keyGroup, 0l);
				partitionProcessed.put(keyGroup, 0l);
				partitionValid.put(keyGroup, true);
			}
		}

		if (partitionArrived.size() != partitionProcessed.size()) {
			partitionValid.put(partitionValid.keySet().stream().findFirst().get(), false);
		}

		if (partitionArrived.isEmpty() || partitionProcessed.isEmpty() || executorUtilization.isEmpty()) {
			partitionValid.put("0", false);
		}

		LOG.info("utilization: " + executorUtilization);

		metrics.put("Arrived", partitionArrived);
		metrics.put("Processed", partitionProcessed);
		metrics.put("Utilization", executorUtilization);
		metrics.put("Validity", partitionValid);

		return metrics;
	}

	public JobVertexID getVertexId() {
		return vertexID;
	}

	public String getPartitionId(String keyGroup) {
		return  keyGroup.split(":")[0];
//		return  "Partition " + keyGroup.split(":")[0];
	}

	public void retrievePartitionArrived(String[] ratesLine, HashMap<String, Long> partitionArrived,
										 HashMap<String, Boolean> partitionValid) {
		// TODO: need to consider multiple upstream tasks, we need to sum values from different upstream tasks
		if (!ratesLine[11].equals("0")) {
			String[] keyGroupsArrived = ratesLine[11].split("&");
			for (String keyGroup : keyGroupsArrived) {
				String partition = getPartitionId(keyGroup);
				long arrived = Long.valueOf(keyGroup.split(":")[1]);
				if (partitionArrived.getOrDefault(partition, 0l) <= arrived) {
					partitionArrived.put(partition, arrived);
					partitionValid.put(partition, true);
				}
			}
		}
	}

	public void retrievePartitionProcessed(String[] ratesLine, HashMap<String, Long> partitionProcessed,
				   	HashMap<String, Double> executorUtilization, HashMap<String, Boolean> partitionValid, HashMap<String, Long> executorTimeStamp) {
		// keygroups processed
		if (!ratesLine[12].equals("0")) {
			long timestamp = Long.valueOf(ratesLine[13]);
			String executorId = ratesLine[1].split("-")[1];

			// utilization of executor
			if (Integer.valueOf(executorId) == JobRescalePartitionAssignment.UNUSED_SUBTASK) {
				return;
			}

			// use timestamp to keeps the latest metrics of each executor.
			if (executorTimeStamp.getOrDefault(executorId, 0l) >= timestamp) {
				return;
			}

			executorTimeStamp.put(executorId, timestamp);

			String[] keyGroupsProcessed = ratesLine[12].split("&");
			int actual_processed = 0;
			for (String keyGroup : keyGroupsProcessed) {
				String partition = getPartitionId(keyGroup);
				long processed = Long.valueOf(keyGroup.split(":")[1]);
				if (partitionProcessed.getOrDefault(partition, 0l) <= processed) {
					partitionProcessed.put(partition, processed);
					partitionValid.put(partition, true);
					actual_processed += processed;
				}
			}

			// TODO: do we need to find a maximum one?
			if (Double.valueOf(ratesLine[10]) > 0) {
				executorUtilization.put(executorId, Double.valueOf(ratesLine[10]));
			}

//			LOG.info("Executor id: " + ratesLine[1] + " utilization: " + ratesLine[10] + " processed: " + ratesLine[8]
//				+ " true rate: " + ratesLine[3] + " observed rate: " + ratesLine[5]);
//			LOG.info("actual processed: " + actual_processed + " records in: " + ratesLine[8] + " partition processed: " + ratesLine[12]);
		}
	}
}
