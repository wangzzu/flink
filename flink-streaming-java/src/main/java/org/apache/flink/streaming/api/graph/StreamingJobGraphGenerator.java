/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.InputOutputFormatContainer;
import org.apache.flink.runtime.jobgraph.InputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.WithMasterCheckpointHook;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.UdfStreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;

/**
 * The StreamingJobGraphGenerator converts a {@link StreamGraph} into a {@link JobGraph}.
 * note: StreamGraph to JobGraph
 * note: 核心的数据结构是：JobVertex/JobEdge/IntermediateDataSet
 */
@Internal
public class StreamingJobGraphGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingJobGraphGenerator.class);

	// ------------------------------------------------------------------------

	//note: 传入 stream graph 创建 job graph
	public static JobGraph createJobGraph(StreamGraph streamGraph) {
		return createJobGraph(streamGraph, null);
	}

	//note: 创建 jobGraph
	public static JobGraph createJobGraph(StreamGraph streamGraph, @Nullable JobID jobID) {
		return new StreamingJobGraphGenerator(streamGraph, jobID).createJobGraph();
	}

	// ------------------------------------------------------------------------

	private final StreamGraph streamGraph;

	//note: id -> JobVertex
	private final Map<Integer, JobVertex> jobVertices;
	private final JobGraph jobGraph;
	//note: 已经构建的 JobVertex 的 id 集合
	private final Collection<Integer> builtVertices;

	//note: 物理边的集合（排除了 chain 内部的边）, 按创建顺序排序
	private final List<StreamEdge> physicalEdgesInOrder;

	//note: 保存 chain 信息，部署时用来构建 OperatorChain，startNodeId -> (currentNodeId -> StreamConfig)
	private final Map<Integer, Map<Integer, StreamConfig>> chainedConfigs;

	//note: 记录所有节点的配置信息，id -> StreamConfig
	private final Map<Integer, StreamConfig> vertexConfigs;
	//note: 记录每个节点的名字，id -> chainedName
	private final Map<Integer, String> chainedNames;

	private final Map<Integer, ResourceSpec> chainedMinResources;
	private final Map<Integer, ResourceSpec> chainedPreferredResources;

	private final Map<Integer, InputOutputFormatContainer> chainedInputOutputFormats;

	private final StreamGraphHasher defaultStreamGraphHasher;
	private final List<StreamGraphHasher> legacyStreamGraphHashers;

	private StreamingJobGraphGenerator(StreamGraph streamGraph) {
		this(streamGraph, null);
	}

	private StreamingJobGraphGenerator(StreamGraph streamGraph, @Nullable JobID jobID) {
		this.streamGraph = streamGraph;
		this.defaultStreamGraphHasher = new StreamGraphHasherV2();
		this.legacyStreamGraphHashers = Arrays.asList(new StreamGraphUserHashHasher());

		this.jobVertices = new HashMap<>();
		this.builtVertices = new HashSet<>();
		this.chainedConfigs = new HashMap<>();
		this.vertexConfigs = new HashMap<>();
		this.chainedNames = new HashMap<>();
		this.chainedMinResources = new HashMap<>();
		this.chainedPreferredResources = new HashMap<>();
		this.chainedInputOutputFormats = new HashMap<>();
		this.physicalEdgesInOrder = new ArrayList<>();

		jobGraph = new JobGraph(jobID, streamGraph.getJobName());
	}

	//note: 根据 StreamGraph 生成 JobGraph
	private JobGraph createJobGraph() {

		// make sure that all vertices start immediately
		//note: 设置调度模式
		jobGraph.setScheduleMode(streamGraph.getScheduleMode());

		// Generate deterministic hashes for the nodes in order to identify them across
		// submission iff they didn't change.
		/**
		 * note: 为每个 SteamNode 生成一个确定的 hash id，如果提交的拓扑没有改变，则每次生成的 hash id 都是一样的
		 * note: 这里只要保证 source 的顺序是确定的，就可以保证最后生产的 hash id 不变
		 * note: 它是利用 input 节点的 hash 值及该节点在 map 中位置（实际上是 map.size 算的）来计算确定的
		 * note: 实现逻辑见 {@link StreamGraphHasherV2#traverseStreamGraphAndGenerateHashes(StreamGraph)}
		 */
		Map<Integer, byte[]> hashes = defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);

		// Generate legacy version hashes for backwards compatibility
		//note: 这个设置主要是为了防止 hash 机制变化时出现不兼容的情况
		List<Map<Integer, byte[]>> legacyHashes = new ArrayList<>(legacyStreamGraphHashers.size());
		for (StreamGraphHasher hasher : legacyStreamGraphHashers) {
			legacyHashes.add(hasher.traverseStreamGraphAndGenerateHashes(streamGraph));
		}

		Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes = new HashMap<>();

		//note: 最重要的函数，生成 JobVertex/JobEdge 等，并尽可能地将多个节点 chain 在一起
		setChaining(hashes, legacyHashes, chainedOperatorHashes);

		//note: 将每个 JobVertex 的入边集合也序列化到该 JobVertex 的 StreamConfig 中 (出边集合已经在 setChaining 的时候写入了)
		setPhysicalEdges();

		//note: 根据 group name 为每个 JobVertex 指定所属的 SlotSharingGroup 以及设置 CoLocationGroup
		setSlotSharingAndCoLocation();

		//note: checkpoint相关的配置
		configureCheckpointing();

		//note: 用户第三包就是在这里（cacheFile）传给 JobGraph
		JobGraphGenerator.addUserArtifactEntries(streamGraph.getUserArtifacts(), jobGraph);

		// set the ExecutionConfig last when it has been finalized
		try {
			//note: 将 StreamGraph 的 ExecutionConfig 序列化到 JobGraph 的配置中
			jobGraph.setExecutionConfig(streamGraph.getExecutionConfig());
		}
		catch (IOException e) {
			throw new IllegalConfigurationException("Could not serialize the ExecutionConfig." +
					"This indicates that non-serializable types (like custom serializers) were registered");
		}

		return jobGraph;
	}

	private void setPhysicalEdges() {
		Map<Integer, List<StreamEdge>> physicalInEdgesInOrder = new HashMap<Integer, List<StreamEdge>>();

		for (StreamEdge edge : physicalEdgesInOrder) {
			int target = edge.getTargetId();

			List<StreamEdge> inEdges = physicalInEdgesInOrder.computeIfAbsent(target, k -> new ArrayList<>());

			inEdges.add(edge);
		}

		for (Map.Entry<Integer, List<StreamEdge>> inEdges : physicalInEdgesInOrder.entrySet()) {
			int vertex = inEdges.getKey();
			List<StreamEdge> edgeList = inEdges.getValue();

			//note: 将 in Edge 序列化到 vertexConfigs 中
			vertexConfigs.get(vertex).setInPhysicalEdges(edgeList);
		}
	}

	/**
	 * Sets up task chains from the source {@link StreamNode} instances.
	 * note：从 Source StreamNode 实例开始设置 task chain，它将会递归地创建所有的 JobVertex 实例
	 *
	 * <p>This will recursively create all {@link JobVertex} instances.
	 */
	private void setChaining(Map<Integer, byte[]> hashes, List<Map<Integer, byte[]>> legacyHashes, Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {
		for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
			//note: 处理每个 Source StreamNode
			createChain(sourceNodeId, sourceNodeId, hashes, legacyHashes, 0, chainedOperatorHashes);
		}
	}

	private List<StreamEdge> createChain(
			Integer startNodeId,
			Integer currentNodeId,
			Map<Integer, byte[]> hashes,
			List<Map<Integer, byte[]>> legacyHashes,
			int chainIndex,
			Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {

		if (!builtVertices.contains(startNodeId)) {

			List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();

			//note: 以 Edge 的粒度，记录上下游算子能 chain 在一起的 Edge
			List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();
			List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();

			//note: 当前要处理的 StreamNode
			StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);

			//note: 遍历当前的输出节点，判断是否可以 chain 在一起
			for (StreamEdge outEdge : currentNode.getOutEdges()) {
				if (isChainable(outEdge, streamGraph)) { //note: 如果可以 chain 在一起的话
					chainableOutputs.add(outEdge);
				} else {
					nonChainableOutputs.add(outEdge);
				}
			}

			//note: 递归调用
			for (StreamEdge chainable : chainableOutputs) {
				//note: 如果可以 chain 在一起的话，这里的 chainIndex 会加 1
				transitiveOutEdges.addAll(
						createChain(startNodeId, chainable.getTargetId(), hashes, legacyHashes, chainIndex + 1, chainedOperatorHashes));
			}

			for (StreamEdge nonChainable : nonChainableOutputs) {
				transitiveOutEdges.add(nonChainable);
				//note: 不能 chain 一起的话，这里的 chainIndex 是从 0 开始算的，后面也肯定会走到 createJobVertex 的逻辑
				createChain(nonChainable.getTargetId(), nonChainable.getTargetId(), hashes, legacyHashes, 0, chainedOperatorHashes);
			}

			//note: 记录每个 startNodeId 的 hash id（主要是 legacyHashes 中记录的）
			List<Tuple2<byte[], byte[]>> operatorHashes =
				chainedOperatorHashes.computeIfAbsent(startNodeId, k -> new ArrayList<>());

			byte[] primaryHashBytes = hashes.get(currentNodeId);
			//note: OperatorID
			OperatorID currentOperatorId = new OperatorID(primaryHashBytes);

			for (Map<Integer, byte[]> legacyHash : legacyHashes) {
				operatorHashes.add(new Tuple2<>(primaryHashBytes, legacyHash.get(currentNodeId)));
			}

			//note: 记录 chainedName
			chainedNames.put(currentNodeId, createChainedName(currentNodeId, chainableOutputs));
			//note: 计算 Chain 之后 node 的 minResources
			chainedMinResources.put(currentNodeId, createChainedMinResources(currentNodeId, chainableOutputs));
			//note: 计算并记录这个 node 的资源上限
			chainedPreferredResources.put(currentNodeId, createChainedPreferredResources(currentNodeId, chainableOutputs));

			//note: InputFormat & OutputFormat 的处理
			if (currentNode.getInputFormat() != null) {
				getOrCreateFormatContainer(startNodeId).addInputFormat(currentOperatorId, currentNode.getInputFormat());
			}

			if (currentNode.getOutputFormat() != null) {
				getOrCreateFormatContainer(startNodeId).addOutputFormat(currentOperatorId, currentNode.getOutputFormat());
			}

			//note: 如果当前节点是 chain 的起始节点, 则直接创建 JobVertex 并返回 StreamConfig, 否则先创建一个空的 StreamConfig
			//note: 这里实际上，如果节点不能 chain 在一起，那么 currentNodeId 跟 startNodeId 肯定是不相等的
			//note: createJobVertex 函数就是根据 StreamNode 创建对应的 JobVertex, 并返回了空的 StreamConfig
			StreamConfig config = currentNodeId.equals(startNodeId)
					? createJobVertex(startNodeId, hashes, legacyHashes, chainedOperatorHashes)//note: chain 的起始 StreamNode
					: new StreamConfig(new Configuration());

			//note: 设置 JobVertex 的 StreamConfig, 基本上是将 StreamNode 中的配置设置到 StreamConfig 中
			setVertexConfig(currentNodeId, config, chainableOutputs, nonChainableOutputs);

			if (currentNodeId.equals(startNodeId)) { //note: 如果走到这里，证明这个 chain 已经完成
				//note: chain 中起始 StreamNode
				config.setChainStart();
				config.setChainIndex(0);
				config.setOperatorName(streamGraph.getStreamNode(currentNodeId).getOperatorName());
				//note: Config 中也会记录这个 chain 的出边
				config.setOutEdgesInOrder(transitiveOutEdges);
				config.setOutEdges(streamGraph.getStreamNode(currentNodeId).getOutEdges());

				for (StreamEdge edge : transitiveOutEdges) {
					//note: 构建 graph
					connect(startNodeId, edge);
				}

				//note: 将 chain 中所有子节点的 StreamConfig 写入到 headOfChain 节点的 CHAINED_TASK_CONFIG 配置中
				config.setTransitiveChainedTaskConfigs(chainedConfigs.get(startNodeId));

			} else {
				//note: 如果是 chain 中子节点
				chainedConfigs.computeIfAbsent(startNodeId, k -> new HashMap<Integer, StreamConfig>());

				config.setChainIndex(chainIndex);
				StreamNode node = streamGraph.getStreamNode(currentNodeId);
				config.setOperatorName(node.getOperatorName());
				//note: 将当前 StreamNode 的 config 记录到该 chain 的 config 集合中
				chainedConfigs.get(startNodeId).put(currentNodeId, config);
			}

			config.setOperatorID(currentOperatorId);

			if (chainableOutputs.isEmpty()) {
				config.setChainEnd();
			}
			return transitiveOutEdges;

		} else {
			return new ArrayList<>();
		}
	}

	private InputOutputFormatContainer getOrCreateFormatContainer(Integer startNodeId) {
		return chainedInputOutputFormats
			.computeIfAbsent(startNodeId, k -> new InputOutputFormatContainer(Thread.currentThread().getContextClassLoader()));
	}

	//note: 生成当前节点的 name，也是 flink ui 上看到的名字
	private String createChainedName(Integer vertexID, List<StreamEdge> chainedOutputs) {
		String operatorName = streamGraph.getStreamNode(vertexID).getOperatorName();
		if (chainedOutputs.size() > 1) {
			List<String> outputChainedNames = new ArrayList<>();
			for (StreamEdge chainable : chainedOutputs) {
				outputChainedNames.add(chainedNames.get(chainable.getTargetId()));
			}
			return operatorName + " -> (" + StringUtils.join(outputChainedNames, ", ") + ")";
		} else if (chainedOutputs.size() == 1) {
			return operatorName + " -> " + chainedNames.get(chainedOutputs.get(0).getTargetId());
		} else {
			return operatorName;
		}
	}

	//note: 计算这个 Operator 应该使用的最小资源
	private ResourceSpec createChainedMinResources(Integer vertexID, List<StreamEdge> chainedOutputs) {
		ResourceSpec minResources = streamGraph.getStreamNode(vertexID).getMinResources();
		for (StreamEdge chainable : chainedOutputs) {
			minResources = minResources.merge(chainedMinResources.get(chainable.getTargetId()));
		}
		return minResources;
	}

	//note: 计算这个 Operator 应该使用的最大资源限制
	private ResourceSpec createChainedPreferredResources(Integer vertexID, List<StreamEdge> chainedOutputs) {
		ResourceSpec preferredResources = streamGraph.getStreamNode(vertexID).getPreferredResources();
		for (StreamEdge chainable : chainedOutputs) {
			preferredResources = preferredResources.merge(chainedPreferredResources.get(chainable.getTargetId()));
		}
		return preferredResources;
	}

	//note: 创建 JobVertex，并返回一个 空的 StreamConfig 对象
	private StreamConfig createJobVertex(
			Integer streamNodeId,
			Map<Integer, byte[]> hashes,
			List<Map<Integer, byte[]>> legacyHashes,
			Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {

		JobVertex jobVertex;
		StreamNode streamNode = streamGraph.getStreamNode(streamNodeId);

		byte[] hash = hashes.get(streamNodeId);

		if (hash == null) {
			throw new IllegalStateException("Cannot find node hash. " +
					"Did you generate them before calling this method?");
		}

		//note: 创建 JobVertexID
		JobVertexID jobVertexId = new JobVertexID(hash);

		//note: 处理 legacyHashes
		List<JobVertexID> legacyJobVertexIds = new ArrayList<>(legacyHashes.size());
		for (Map<Integer, byte[]> legacyHash : legacyHashes) {
			hash = legacyHash.get(streamNodeId);
			if (null != hash) {
				legacyJobVertexIds.add(new JobVertexID(hash));
			}
		}

		List<Tuple2<byte[], byte[]>> chainedOperators = chainedOperatorHashes.get(streamNodeId);
		List<OperatorID> chainedOperatorVertexIds = new ArrayList<>();
		List<OperatorID> userDefinedChainedOperatorVertexIds = new ArrayList<>();
		if (chainedOperators != null) {
			for (Tuple2<byte[], byte[]> chainedOperator : chainedOperators) {
				chainedOperatorVertexIds.add(new OperatorID(chainedOperator.f0));
				userDefinedChainedOperatorVertexIds.add(chainedOperator.f1 != null ? new OperatorID(chainedOperator.f1) : null);
			}
		}

		if (chainedInputOutputFormats.containsKey(streamNodeId)) {
			jobVertex = new InputOutputFormatVertex(
					chainedNames.get(streamNodeId),
					jobVertexId,
					legacyJobVertexIds,
					chainedOperatorVertexIds,
					userDefinedChainedOperatorVertexIds);

			chainedInputOutputFormats
				.get(streamNodeId)
				.write(new TaskConfig(jobVertex.getConfiguration()));
		} else {
			//note: 创建 JobVertex 对象
			jobVertex = new JobVertex(
					chainedNames.get(streamNodeId),
					jobVertexId,
					legacyJobVertexIds,
					chainedOperatorVertexIds,
					userDefinedChainedOperatorVertexIds);
		}

		//note: 给 JobVertex 设置相应的属性
		jobVertex.setResources(chainedMinResources.get(streamNodeId), chainedPreferredResources.get(streamNodeId));

		jobVertex.setInvokableClass(streamNode.getJobVertexClass());

		int parallelism = streamNode.getParallelism();

		if (parallelism > 0) {
			jobVertex.setParallelism(parallelism);
		} else {
			parallelism = jobVertex.getParallelism();
		}

		jobVertex.setMaxParallelism(streamNode.getMaxParallelism());

		if (LOG.isDebugEnabled()) {
			LOG.debug("Parallelism set: {} for {}", parallelism, streamNodeId);
		}

		// TODO: inherit InputDependencyConstraint from the head operator
		jobVertex.setInputDependencyConstraint(streamGraph.getExecutionConfig().getDefaultInputDependencyConstraint());

		//note: 添加到缓存中
		jobVertices.put(streamNodeId, jobVertex);
		builtVertices.add(streamNodeId);
		jobGraph.addVertex(jobVertex);

		//note: 返回的是一个空的 StreamConfig
		return new StreamConfig(jobVertex.getConfiguration());
	}

	//note: 设置 StreamConfig，基本是将 StreamNode 中配置添加到 Config 中
	@SuppressWarnings("unchecked")
	private void setVertexConfig(Integer vertexID, StreamConfig config,
			List<StreamEdge> chainableOutputs, List<StreamEdge> nonChainableOutputs) {

		//note: 将 StreamNode 的属性添加到 StreamConfig 中
		StreamNode vertex = streamGraph.getStreamNode(vertexID);

		config.setVertexID(vertexID);
		config.setBufferTimeout(vertex.getBufferTimeout());

		config.setTypeSerializerIn1(vertex.getTypeSerializerIn1());
		config.setTypeSerializerIn2(vertex.getTypeSerializerIn2());
		config.setTypeSerializerOut(vertex.getTypeSerializerOut());

		// iterate edges, find sideOutput edges create and save serializers for each outputTag type
		for (StreamEdge edge : chainableOutputs) {
			if (edge.getOutputTag() != null) {
				config.setTypeSerializerSideOut(
					edge.getOutputTag(),
					edge.getOutputTag().getTypeInfo().createSerializer(streamGraph.getExecutionConfig())
				);
			}
		}
		for (StreamEdge edge : nonChainableOutputs) {
			if (edge.getOutputTag() != null) {
				config.setTypeSerializerSideOut(
						edge.getOutputTag(),
						edge.getOutputTag().getTypeInfo().createSerializer(streamGraph.getExecutionConfig())
				);
			}
		}

		//note: 将 Operator Factory 揭露到这个 StreamConfig 中
		config.setStreamOperatorFactory(vertex.getOperatorFactory());
		config.setOutputSelectors(vertex.getOutputSelectors());

		config.setNumberOfOutputs(nonChainableOutputs.size());
		config.setNonChainedOutputs(nonChainableOutputs);
		config.setChainedOutputs(chainableOutputs);

		config.setTimeCharacteristic(streamGraph.getTimeCharacteristic());

		final CheckpointConfig checkpointCfg = streamGraph.getCheckpointConfig();

		config.setStateBackend(streamGraph.getStateBackend());
		config.setCheckpointingEnabled(checkpointCfg.isCheckpointingEnabled());
		if (checkpointCfg.isCheckpointingEnabled()) {
			config.setCheckpointMode(checkpointCfg.getCheckpointingMode());
		}
		else {
			// the "at-least-once" input handler is slightly cheaper (in the absence of checkpoints),
			// so we use that one if checkpointing is not enabled
			config.setCheckpointMode(CheckpointingMode.AT_LEAST_ONCE);
		}
		config.setStatePartitioner(0, vertex.getStatePartitioner1());
		config.setStatePartitioner(1, vertex.getStatePartitioner2());
		config.setStateKeySerializer(vertex.getStateKeySerializer());

		Class<? extends AbstractInvokable> vertexClass = vertex.getJobVertexClass();

		if (vertexClass.equals(StreamIterationHead.class)
				|| vertexClass.equals(StreamIterationTail.class)) {
			config.setIterationId(streamGraph.getBrokerID(vertexID));
			config.setIterationWaitTime(streamGraph.getLoopTimeout(vertexID));
		}

		vertexConfigs.put(vertexID, config);
	}

	private void connect(Integer headOfChain, StreamEdge edge) {

		//note: 记录 StreamEdge，这个主要是 chain 之间的边
		physicalEdgesInOrder.add(edge);

		Integer downStreamvertexID = edge.getTargetId();

		//note: 这里 headVertex 指的是 headOfChain 对应的 JobVertex（也是当前 node 对应的 vertex）
		JobVertex headVertex = jobVertices.get(headOfChain);
		JobVertex downStreamVertex = jobVertices.get(downStreamvertexID);

		StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());

		//note: 这个节点的输入数增加 1
		downStreamConfig.setNumberOfInputs(downStreamConfig.getNumberOfInputs() + 1);

		StreamPartitioner<?> partitioner = edge.getPartitioner();

		ResultPartitionType resultPartitionType;
		switch (edge.getShuffleMode()) {
			case PIPELINED:
				resultPartitionType = ResultPartitionType.PIPELINED_BOUNDED;
				break;
			case BATCH:
				resultPartitionType = ResultPartitionType.BLOCKING;
				break;
			case UNDEFINED:
				resultPartitionType = streamGraph.isBlockingConnectionsBetweenChains() ?
						ResultPartitionType.BLOCKING : ResultPartitionType.PIPELINED_BOUNDED;
				break;
			default:
				throw new UnsupportedOperationException("Data exchange mode " +
					edge.getShuffleMode() + " is not supported yet.");
		}

		//note: 创建 JobEdge（它会连接上下游的 node）
		JobEdge jobEdge;
		if (partitioner instanceof ForwardPartitioner || partitioner instanceof RescalePartitioner) {
			jobEdge = downStreamVertex.connectNewDataSetAsInput( //note: 这个方法会创建 IntermediateDataSet 对象
				headVertex,
				DistributionPattern.POINTWISE, //note: 上游与下游的消费模式，（每个生产任务的 sub-task 会连接到消费任务的一个或多个 sub-task）
				resultPartitionType);
		} else {
			jobEdge = downStreamVertex.connectNewDataSetAsInput(
					headVertex,
					DistributionPattern.ALL_TO_ALL, //note: 每个生产任务的 sub-task 都会连接到每个消费任务的 sub-task
					resultPartitionType);
		}
		// set strategy name so that web interface can show it.
		//note: 设置 partitioner
		jobEdge.setShipStrategyName(partitioner.toString());

		if (LOG.isDebugEnabled()) {
			LOG.debug("CONNECTED: {} - {} -> {}", partitioner.getClass().getSimpleName(),
					headOfChain, downStreamvertexID);
		}
	}

	//note: 是否可以 chain 在一起
	public static boolean isChainable(StreamEdge edge, StreamGraph streamGraph) {
		StreamNode upStreamVertex = streamGraph.getSourceVertex(edge); //note: edge 的 source node
		StreamNode downStreamVertex = streamGraph.getTargetVertex(edge); //note: edge 的 sink node

		//note: 获取输入和输出的 Operator Factory
		StreamOperatorFactory<?> headOperator = upStreamVertex.getOperatorFactory();
		StreamOperatorFactory<?> outOperator = downStreamVertex.getOperatorFactory();

		return downStreamVertex.getInEdges().size() == 1 //note: 下游 Operator 的 Edge 只有一个（如果是多个合并，是无法 Chain 在一起的）
				&& outOperator != null
				&& headOperator != null
				&& upStreamVertex.isSameSlotSharingGroup(downStreamVertex) //note: 对应的 slotSharingGroup 一样
				&& outOperator.getChainingStrategy() == ChainingStrategy.ALWAYS //note: out operator 允许 chain 操作
				&& (headOperator.getChainingStrategy() == ChainingStrategy.HEAD || //note: head Operator 允许跟后面的 chain 在一起
					headOperator.getChainingStrategy() == ChainingStrategy.ALWAYS)
				&& (edge.getPartitioner() instanceof ForwardPartitioner) //note: partitioner 是 ForwardPartitioner 类型
				&& edge.getShuffleMode() != ShuffleMode.BATCH
				&& upStreamVertex.getParallelism() == downStreamVertex.getParallelism() //note: 并发相等
				&& streamGraph.isChainingEnabled(); //note: StreamGraph 允许 Chain 在一起
	}

	private void setSlotSharingAndCoLocation() {
		final HashMap<String, SlotSharingGroup> slotSharingGroups = new HashMap<>();
		final HashMap<String, Tuple2<SlotSharingGroup, CoLocationGroup>> coLocationGroups = new HashMap<>();

		for (Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

			//note: 这里拿到的 JobVertex 的 source StreamNode
			final StreamNode node = streamGraph.getStreamNode(entry.getKey());
			final JobVertex vertex = entry.getValue();

			// configure slot sharing group
			final String slotSharingGroupKey = node.getSlotSharingGroup();
			final SlotSharingGroup sharingGroup;

			if (slotSharingGroupKey != null) {
				sharingGroup = slotSharingGroups.computeIfAbsent(
						slotSharingGroupKey, (k) -> new SlotSharingGroup());
				//note: 设置 SlotSharingGroup
				vertex.setSlotSharingGroup(sharingGroup);
			} else {
				sharingGroup = null;
			}

			// configure co-location constraint
			final String coLocationGroupKey = node.getCoLocationGroup();
			if (coLocationGroupKey != null) {
				if (sharingGroup == null) {
					throw new IllegalStateException("Cannot use a co-location constraint without a slot sharing group");
				}

				Tuple2<SlotSharingGroup, CoLocationGroup> constraint = coLocationGroups.computeIfAbsent(
						coLocationGroupKey, (k) -> new Tuple2<>(sharingGroup, new CoLocationGroup()));

				if (constraint.f0 != sharingGroup) {
					throw new IllegalStateException("Cannot co-locate operators from different slot sharing groups");
				}

				vertex.updateCoLocationGroup(constraint.f1);
				constraint.f1.addVertex(vertex);
			}
		}
	}

	//note: 主要是 checkpoint 相关的配置
	private void configureCheckpointing() {
		CheckpointConfig cfg = streamGraph.getCheckpointConfig();

		long interval = cfg.getCheckpointInterval();
		if (interval < MINIMAL_CHECKPOINT_TIME) {
			// interval of max value means disable periodic checkpoint
			interval = Long.MAX_VALUE;
		}

		//  --- configure the participating vertices ---

		//note: 配置 checkpoint 中要参与的 vertices 节点信息
		// collect the vertices that receive "trigger checkpoint" messages.
		// currently, these are all the sources
		//note: 记录接收 trigger checkpoint msg 的 vertices，当前都是 source 的情况
		List<JobVertexID> triggerVertices = new ArrayList<>();

		// collect the vertices that need to acknowledge the checkpoint
		// currently, these are all vertices
		//note: 记录当前需要向 checkpoint coordinator 发送 ack 的 vertices，当前指的是所有的 vertices
		List<JobVertexID> ackVertices = new ArrayList<>(jobVertices.size());

		// collect the vertices that receive "commit checkpoint" messages
		// currently, these are all vertices
		//note: 记录接收 'commit checkpoint' 的 vertices，当前也指的是所有 vertices
		List<JobVertexID> commitVertices = new ArrayList<>(jobVertices.size());

		for (JobVertex vertex : jobVertices.values()) {
			if (vertex.isInputVertex()) {
				triggerVertices.add(vertex.getID());
			}
			commitVertices.add(vertex.getID());
			ackVertices.add(vertex.getID());
		}

		//  --- configure options ---

		CheckpointRetentionPolicy retentionAfterTermination;
		if (cfg.isExternalizedCheckpointsEnabled()) {
			CheckpointConfig.ExternalizedCheckpointCleanup cleanup = cfg.getExternalizedCheckpointCleanup();
			// Sanity check
			if (cleanup == null) {
				throw new IllegalStateException("Externalized checkpoints enabled, but no cleanup mode configured.");
			}
			retentionAfterTermination = cleanup.deleteOnCancellation() ?
					CheckpointRetentionPolicy.RETAIN_ON_FAILURE :
					CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION;
		} else {
			//note: 默认是 NEVER_RETAIN_AFTER_TERMINATION，作业只要进入终止 checkpoint 就会删除
			retentionAfterTermination = CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
		}

		//note: 默认是 EXACTLY_ONCE
		CheckpointingMode mode = cfg.getCheckpointingMode();

		boolean isExactlyOnce;
		if (mode == CheckpointingMode.EXACTLY_ONCE) {
			isExactlyOnce = true;
		} else if (mode == CheckpointingMode.AT_LEAST_ONCE) {
			isExactlyOnce = false;
		} else {
			throw new IllegalStateException("Unexpected checkpointing mode. " +
				"Did not expect there to be another checkpointing mode besides " +
				"exactly-once or at-least-once.");
		}

		//  --- configure the master-side checkpoint hooks ---

		final ArrayList<MasterTriggerRestoreHook.Factory> hooks = new ArrayList<>();

		for (StreamNode node : streamGraph.getStreamNodes()) {
			if (node.getOperatorFactory() instanceof UdfStreamOperatorFactory) {
				Function f = ((UdfStreamOperatorFactory) node.getOperatorFactory()).getUserFunction();

				if (f instanceof WithMasterCheckpointHook) {
					//note: 它会在 CheckpointCoordinator 端在每次 checkpoint 及 restore 时触发一个 'global action'
					//note: 比如这里可以通过这个接口将状态刷到外部存储
					hooks.add(new FunctionMasterCheckpointHookFactory((WithMasterCheckpointHook<?>) f));
				}
			}
		}

		// because the hooks can have user-defined code, they need to be stored as
		// eagerly serialized values
		//note: 这里对 hooks 做一下序列化
		final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks;
		if (hooks.isEmpty()) {
			serializedHooks = null;
		} else {
			try {
				MasterTriggerRestoreHook.Factory[] asArray =
						hooks.toArray(new MasterTriggerRestoreHook.Factory[hooks.size()]);
				serializedHooks = new SerializedValue<>(asArray);
			}
			catch (IOException e) {
				throw new FlinkRuntimeException("Trigger/restore hook is not serializable", e);
			}
		}

		// because the state backend can have user-defined code, it needs to be stored as
		// eagerly serialized value
		//note: 对 state backend 类做下序列化
		final SerializedValue<StateBackend> serializedStateBackend;
		if (streamGraph.getStateBackend() == null) {
			serializedStateBackend = null;
		} else {
			try {
				serializedStateBackend =
					new SerializedValue<StateBackend>(streamGraph.getStateBackend());
			}
			catch (IOException e) {
				throw new FlinkRuntimeException("State backend is not serializable", e);
			}
		}

		//  --- done, put it all together ---

		//note: 创建一个 JobCheckpointingSettings 对象
		JobCheckpointingSettings settings = new JobCheckpointingSettings(
			triggerVertices,
			ackVertices,
			commitVertices,
			new CheckpointCoordinatorConfiguration( //note: 创建一个 CheckpointCoordinatorConfiguration 对象
				interval,
				cfg.getCheckpointTimeout(),
				cfg.getMinPauseBetweenCheckpoints(),
				cfg.getMaxConcurrentCheckpoints(),
				retentionAfterTermination,
				isExactlyOnce,
				cfg.isPreferCheckpointForRecovery(),
				cfg.getTolerableCheckpointFailureNumber()),
			serializedStateBackend,
			serializedHooks);

		jobGraph.setSnapshotSettings(settings);
	}
}
