package org.apache.flink.runtime.rescale;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.List;
import java.util.Map;

public interface JobRescaleAction {

	JobGraph getJobGraph();

	void repartition(JobVertexID vertexID, JobRescalePartitionAssignment jobRescalePartitionAssignment);

	void scaleOut(JobVertexID vertexID, int newParallelism, JobRescalePartitionAssignment jobRescalePartitionAssignment);

	void scaleIn(JobVertexID vertexID, int newParallelism, JobRescalePartitionAssignment jobRescalePartitionAssignment);

	enum ActionType {
		REPARTITION,
		SCALE_OUT,
		SCALE_IN
	}
}
