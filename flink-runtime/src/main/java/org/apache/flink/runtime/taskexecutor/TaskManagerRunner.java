/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterConfiguration;
import org.apache.flink.runtime.entrypoint.ClusterConfigurationParserFactory;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskexecutor.partition.PartitionTable;
import org.apache.flink.runtime.taskmanager.MemoryLogger;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * note：对于 yarn/standalone 模式，TM 启动的入口
 * This class is the executable entry point for the task manager in yarn or standalone mode.
 * It constructs the related components (network, I/O manager, memory manager, RPC service, HA service)
 * and starts them.
 */
public class TaskManagerRunner implements FatalErrorHandler, AutoCloseableAsync {

	private static final Logger LOG = LoggerFactory.getLogger(TaskManagerRunner.class);

	private static final long FATAL_ERROR_SHUTDOWN_TIMEOUT_MS = 10000L;

	private static final int STARTUP_FAILURE_RETURN_CODE = 1;

	public static final int RUNTIME_FAILURE_RETURN_CODE = 2;

	private final Object lock = new Object();

	private final Configuration configuration;

	private final ResourceID resourceId;

	private final Time timeout;

	private final RpcService rpcService;

	private final HighAvailabilityServices highAvailabilityServices;

	private final MetricRegistryImpl metricRegistry;

	private final BlobCacheService blobCacheService;

	/** Executor used to run future callbacks. */
	private final ExecutorService executor;

	private final TaskExecutor taskManager;

	private final CompletableFuture<Void> terminationFuture;

	private boolean shutdown;

	//note: 初始化 TaskManagerRunner
	public TaskManagerRunner(Configuration configuration, ResourceID resourceId) throws Exception {
		this.configuration = checkNotNull(configuration);
		this.resourceId = checkNotNull(resourceId);

		//note: akka 超时设置
		timeout = AkkaUtils.getTimeoutAsTime(configuration);

		this.executor = java.util.concurrent.Executors.newScheduledThreadPool(
			Hardware.getNumberCPUCores(),
			new ExecutorThreadFactory("taskmanager-future"));

		//note: HA 的配置及服务初始化
		highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(
			configuration,
			executor,
			HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION);

		//note: create rpc service
		rpcService = createRpcService(configuration, highAvailabilityServices);

		//note: 初始化心跳服务
		HeartbeatServices heartbeatServices = HeartbeatServices.fromConfiguration(configuration);

		//note: metrics 服务
		metricRegistry = new MetricRegistryImpl(
			MetricRegistryConfiguration.fromConfiguration(configuration),
			ReporterSetup.fromConfiguration(configuration));

		//note: 启动相应的 metrics 服务
		final RpcService metricQueryServiceRpcService = MetricUtils.startMetricsRpcService(configuration, rpcService.getAddress());
		metricRegistry.startQueryService(metricQueryServiceRpcService, resourceId);

		//note: 初始化 blob 服务
		blobCacheService = new BlobCacheService(
			configuration, highAvailabilityServices.createBlobStore(), null
		);

		//note: 启动 TaskManager 服务及创建 TaskExecutor 对象
		taskManager = startTaskManager(
			this.configuration,
			this.resourceId,
			rpcService,
			highAvailabilityServices,
			heartbeatServices,
			metricRegistry,
			blobCacheService,
			false,
			this);

		this.terminationFuture = new CompletableFuture<>();
		this.shutdown = false;

		//note: 周期性地输出内存相关的日志信息，直到 terminationFuture complete
		MemoryLogger.startIfConfigured(LOG, configuration, terminationFuture);
	}

	// --------------------------------------------------------------------------------------------
	//  Lifecycle management
	// --------------------------------------------------------------------------------------------

	public void start() throws Exception {
		taskManager.start();
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		synchronized (lock) {
			if (!shutdown) {
				shutdown = true;

				final CompletableFuture<Void> taskManagerTerminationFuture = taskManager.closeAsync();

				final CompletableFuture<Void> serviceTerminationFuture = FutureUtils.composeAfterwards(
					taskManagerTerminationFuture,
					this::shutDownServices);

				serviceTerminationFuture.whenComplete(
					(Void ignored, Throwable throwable) -> {
						if (throwable != null) {
							terminationFuture.completeExceptionally(throwable);
						} else {
							terminationFuture.complete(null);
						}
					});
			}
		}

		return terminationFuture;
	}

	private CompletableFuture<Void> shutDownServices() {
		synchronized (lock) {
			Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);
			Exception exception = null;

			try {
				blobCacheService.close();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			try {
				metricRegistry.shutdown();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			try {
				highAvailabilityServices.close();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			terminationFutures.add(rpcService.stopService());

			terminationFutures.add(ExecutorUtils.nonBlockingShutdown(timeout.toMilliseconds(), TimeUnit.MILLISECONDS, executor));

			if (exception != null) {
				terminationFutures.add(FutureUtils.completedExceptionally(exception));
			}

			return FutureUtils.completeAll(terminationFutures);
		}
	}

	// export the termination future for caller to know it is terminated
	public CompletableFuture<Void> getTerminationFuture() {
		return terminationFuture;
	}

	// --------------------------------------------------------------------------------------------
	//  FatalErrorHandler methods
	// --------------------------------------------------------------------------------------------

	@Override
	public void onFatalError(Throwable exception) {
		LOG.error("Fatal error occurred while executing the TaskManager. Shutting it down...", exception);

		if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(exception)) {
			terminateJVM();
		} else {
			closeAsync();

			FutureUtils.orTimeout(terminationFuture, FATAL_ERROR_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);

			terminationFuture.whenComplete(
				(Void ignored, Throwable throwable) -> {
					terminateJVM();
				});
		}
	}

	private void terminateJVM() {
		System.exit(RUNTIME_FAILURE_RETURN_CODE);
	}

	// --------------------------------------------------------------------------------------------
	//  Static entry point
	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) throws Exception {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, "TaskManager", args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		//note: 拿到最大允许打开的文件句柄数
		long maxOpenFileHandles = EnvironmentInformation.getOpenFileHandlesLimit();

		if (maxOpenFileHandles != -1L) {
			LOG.info("Maximum number of open file descriptors is {}.", maxOpenFileHandles);
		} else {
			LOG.info("Cannot determine the maximum number of open file descriptors");
		}

		//note: 加载 tm 相关的配置
		final Configuration configuration = loadConfiguration(args);

		//note: 文件系统的初始化
		FileSystem.initialize(configuration, PluginUtils.createPluginManagerFromRootFolder(configuration));

		//note: 安全模块的相关配置，比如 Hadoop、zk 相关的
		SecurityUtils.install(new SecurityConfiguration(configuration));

		try {
			//note: 启动 tm runner
			SecurityUtils.getInstalledContext().runSecured(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					runTaskManager(configuration, ResourceID.generate());
					return null;
				}
			});
		} catch (Throwable t) {
			final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
			LOG.error("TaskManager initialization failed.", strippedThrowable);
			System.exit(STARTUP_FAILURE_RETURN_CODE);
		}
	}

	//note: 将输入参数解析为 Configuration
	@VisibleForTesting
	static Configuration loadConfiguration(String[] args) throws FlinkParseException {
		final CommandLineParser<ClusterConfiguration> commandLineParser = new CommandLineParser<>(new ClusterConfigurationParserFactory());

		final ClusterConfiguration clusterConfiguration;

		try {
			clusterConfiguration = commandLineParser.parse(args);
		} catch (FlinkParseException e) {
			LOG.error("Could not parse the command line options.", e);
			commandLineParser.printHelp(TaskManagerRunner.class.getSimpleName());
			throw e;
		}

		final Configuration dynamicProperties = ConfigurationUtils.createConfiguration(clusterConfiguration.getDynamicProperties());
		return GlobalConfiguration.loadConfiguration(clusterConfiguration.getConfigDir(), dynamicProperties);
	}

	//note: 启动 TaskManagerRunner
	public static void runTaskManager(Configuration configuration, ResourceID resourceId) throws Exception {
		final TaskManagerRunner taskManagerRunner = new TaskManagerRunner(configuration, resourceId);

		taskManagerRunner.start();
	}

	// --------------------------------------------------------------------------------------------
	//  Static utilities
	// --------------------------------------------------------------------------------------------

	//note: 创建并初始化 TaskExecutor 对象
	public static TaskExecutor startTaskManager(
			Configuration configuration,
			ResourceID resourceID,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			BlobCacheService blobCacheService,
			boolean localCommunicationOnly,
			FatalErrorHandler fatalErrorHandler) throws Exception {

		checkNotNull(configuration);
		checkNotNull(resourceID);
		checkNotNull(rpcService);
		checkNotNull(highAvailabilityServices);

		LOG.info("Starting TaskManager with ResourceID: {}", resourceID);

		InetAddress remoteAddress = InetAddress.getByName(rpcService.getAddress());

		//note: TM 服务相关的配置都维护在这个对象中，这里会把使用的相关参数解析并维护起来
		TaskManagerServicesConfiguration taskManagerServicesConfiguration =
			TaskManagerServicesConfiguration.fromConfiguration(
				configuration,
				resourceID,
				remoteAddress,
				EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag(),
				EnvironmentInformation.getMaxJvmHeapMemory(),
				localCommunicationOnly);

		//note: 初始化 TM 的 TaskManagerMetricGroup，并相应地初始化 TM 的基本状态（内存、CPU 等）监控
		Tuple2<TaskManagerMetricGroup, MetricGroup> taskManagerMetricGroup = MetricUtils.instantiateTaskManagerMetricGroup(
			metricRegistry,
			TaskManagerLocation.getHostName(remoteAddress),
			resourceID,
			taskManagerServicesConfiguration.getSystemResourceMetricsProbingInterval());

		//note: 初始化 TaskManagerServices（TM 相关服务的初始化都在这里）
		TaskManagerServices taskManagerServices = TaskManagerServices.fromConfiguration(
			taskManagerServicesConfiguration,
			taskManagerMetricGroup.f1,
			rpcService.getExecutor()); // TODO replace this later with some dedicated executor for io.

		//note: TaskManager 相关的配置，主要用于 TaskExecutor 的初始化
		TaskManagerConfiguration taskManagerConfiguration = TaskManagerConfiguration.fromConfiguration(configuration);

		String metricQueryServiceAddress = metricRegistry.getMetricQueryServiceGatewayRpcAddress();

		//note: 最后创建 TaskExecutor 对象
		return new TaskExecutor(
			rpcService,
			taskManagerConfiguration,
			highAvailabilityServices,
			taskManagerServices,
			heartbeatServices,
			taskManagerMetricGroup.f0,
			metricQueryServiceAddress,
			blobCacheService,
			fatalErrorHandler,
			new PartitionTable<>());
	}

	/**
	 * Create a RPC service for the task manager.
	 * note：给这个 tm 创建一个 RPC service
	 *
	 * @param configuration The configuration for the TaskManager.
	 * @param haServices to use for the task manager hostname retrieval
	 */
	public static RpcService createRpcService(
			final Configuration configuration,
			final HighAvailabilityServices haServices) throws Exception {

		checkNotNull(configuration);
		checkNotNull(haServices);

		final String taskManagerAddress = determineTaskManagerBindAddress(configuration, haServices);
		final String portRangeDefinition = configuration.getString(TaskManagerOptions.RPC_PORT);

		return AkkaRpcServiceUtils.createRpcService(taskManagerAddress, portRangeDefinition, configuration);
	}

	private static String determineTaskManagerBindAddress(
			final Configuration configuration,
			final HighAvailabilityServices haServices) throws Exception {

		final String configuredTaskManagerHostname = configuration.getString(TaskManagerOptions.HOST);

		if (configuredTaskManagerHostname != null) {
			LOG.info("Using configured hostname/address for TaskManager: {}.", configuredTaskManagerHostname);
			return configuredTaskManagerHostname;
		} else {
			return determineTaskManagerBindAddressByConnectingToResourceManager(configuration, haServices);
		}
	}

	private static String determineTaskManagerBindAddressByConnectingToResourceManager(
			final Configuration configuration,
			final HighAvailabilityServices haServices) throws LeaderRetrievalException {

		final Time lookupTimeout = Time.milliseconds(AkkaUtils.getLookupTimeout(configuration).toMillis());

		final InetAddress taskManagerAddress = LeaderRetrievalUtils.findConnectingAddress(
			haServices.getResourceManagerLeaderRetriever(),
			lookupTimeout);

		LOG.info("TaskManager will use hostname/address '{}' ({}) for communication.",
			taskManagerAddress.getHostName(), taskManagerAddress.getHostAddress());

		HostBindPolicy bindPolicy = HostBindPolicy.fromString(configuration.getString(TaskManagerOptions.HOST_BIND_POLICY));
		return bindPolicy == HostBindPolicy.IP ? taskManagerAddress.getHostAddress() : taskManagerAddress.getHostName();
	}
}
