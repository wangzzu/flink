/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

/**
 * Scheduler that assigns tasks to slots. This class is currently work in progress, comments will be updated as we
 * move forward.
 * note：用于向 task 分配 slot 的调度器
 */
public class SchedulerImpl implements Scheduler {

	private static final Logger log = LoggerFactory.getLogger(SchedulerImpl.class);

	private static final int DEFAULT_SLOT_SHARING_MANAGERS_MAP_SIZE = 128;

	/** Strategy that selects the best slot for a given slot allocation request. */
	//note: 对于一个 slot 分配请求，如何选择最佳的 slot
	@Nonnull
	private final SlotSelectionStrategy slotSelectionStrategy;

	/** The slot pool from which slots are allocated. */
	@Nonnull
	private final SlotPool slotPool;

	/** Executor for running tasks in the job master's main thread. */
	@Nonnull
	private ComponentMainThreadExecutor componentMainThreadExecutor;

	/** Managers for the different slot sharing groups. */
	@Nonnull
	private final Map<SlotSharingGroupId, SlotSharingManager> slotSharingManagers;

	public SchedulerImpl(
		@Nonnull SlotSelectionStrategy slotSelectionStrategy,
		@Nonnull SlotPool slotPool) {
		this(slotSelectionStrategy, slotPool, new HashMap<>(DEFAULT_SLOT_SHARING_MANAGERS_MAP_SIZE));
	}

	@VisibleForTesting
	public SchedulerImpl(
		@Nonnull SlotSelectionStrategy slotSelectionStrategy,
		@Nonnull SlotPool slotPool,
		@Nonnull Map<SlotSharingGroupId, SlotSharingManager> slotSharingManagers) {

		this.slotSelectionStrategy = slotSelectionStrategy;
		this.slotSharingManagers = slotSharingManagers;
		this.slotPool = slotPool;
		this.componentMainThreadExecutor = new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor(
			"Scheduler is not initialized with proper main thread executor. " +
				"Call to Scheduler.start(...) required.");
	}

	@Override
	public void start(@Nonnull ComponentMainThreadExecutor mainThreadExecutor) {
		this.componentMainThreadExecutor = mainThreadExecutor;
	}

	//---------------------------

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(
			SlotRequestId slotRequestId,
			ScheduledUnit scheduledUnit,
			SlotProfile slotProfile,
			boolean allowQueuedScheduling,
			Time allocationTimeout) {
		return allocateSlotInternal(
			slotRequestId,
			scheduledUnit,
			slotProfile,
			allowQueuedScheduling,
			allocationTimeout);
	}

	@Override
	public CompletableFuture<LogicalSlot> allocateBatchSlot(
			SlotRequestId slotRequestId,
			ScheduledUnit scheduledUnit,
			SlotProfile slotProfile,
			boolean allowQueuedScheduling) {
		return allocateSlotInternal(
			slotRequestId,
			scheduledUnit,
			slotProfile,
			allowQueuedScheduling,
			null);
	}

	@Nonnull
	private CompletableFuture<LogicalSlot> allocateSlotInternal(
		SlotRequestId slotRequestId,
		ScheduledUnit scheduledUnit,
		SlotProfile slotProfile,
		boolean allowQueuedScheduling,
		@Nullable Time allocationTimeout) {
		log.debug("Received slot request [{}] for task: {}", slotRequestId, scheduledUnit.getTaskToExecute());

		componentMainThreadExecutor.assertRunningInMainThread();

		final CompletableFuture<LogicalSlot> allocationResultFuture = new CompletableFuture<>();
		internalAllocateSlot(
				allocationResultFuture,
				slotRequestId,
				scheduledUnit,
				slotProfile,
				allowQueuedScheduling,
				allocationTimeout);
		return allocationResultFuture;
	}

	//note: 分配 slot
	private void internalAllocateSlot(
			CompletableFuture<LogicalSlot> allocationResultFuture,
			SlotRequestId slotRequestId,
			ScheduledUnit scheduledUnit,
			SlotProfile slotProfile,
			boolean allowQueuedScheduling,
			Time allocationTimeout) {
		//note: 分配的操作是在这里进行的
		CompletableFuture<LogicalSlot> allocationFuture = scheduledUnit.getSlotSharingGroupId() == null ?
			//note: 分配单个的 slot
			allocateSingleSlot(slotRequestId, slotProfile, allowQueuedScheduling, allocationTimeout) :
			//note: 分配 shared slot
			allocateSharedSlot(slotRequestId, scheduledUnit, slotProfile, allowQueuedScheduling, allocationTimeout);

		allocationFuture.whenComplete((LogicalSlot slot, Throwable failure) -> {
			//note: 如果分配过程中异常的话
			if (failure != null) {
				Optional<SharedSlotOversubscribedException> sharedSlotOverAllocatedException =
						ExceptionUtils.findThrowable(failure, SharedSlotOversubscribedException.class);
				if (sharedSlotOverAllocatedException.isPresent() &&
						sharedSlotOverAllocatedException.get().canRetry()) {

					// Retry the allocation
					internalAllocateSlot(
							allocationResultFuture,
							slotRequestId,
							scheduledUnit,
							slotProfile,
							allowQueuedScheduling,
							allocationTimeout);
				} else {
					cancelSlotRequest(
							slotRequestId,
							scheduledUnit.getSlotSharingGroupId(),
							failure);
					allocationResultFuture.completeExceptionally(failure);
				}
			} else {
				allocationResultFuture.complete(slot);
			}
		});
	}

	@Override
	public void cancelSlotRequest(
		SlotRequestId slotRequestId,
		@Nullable SlotSharingGroupId slotSharingGroupId,
		Throwable cause) {

		componentMainThreadExecutor.assertRunningInMainThread();

		if (slotSharingGroupId != null) {
			releaseSharedSlot(slotRequestId, slotSharingGroupId, cause);
		} else {
			slotPool.releaseSlot(slotRequestId, cause);
		}
	}

	@Override
	public void returnLogicalSlot(LogicalSlot logicalSlot) {
		SlotRequestId slotRequestId = logicalSlot.getSlotRequestId();
		SlotSharingGroupId slotSharingGroupId = logicalSlot.getSlotSharingGroupId();
		FlinkException cause = new FlinkException("Slot is being returned to the SlotPool.");
		cancelSlotRequest(slotRequestId, slotSharingGroupId, cause);
	}

	//---------------------------

	private CompletableFuture<LogicalSlot> allocateSingleSlot(
			SlotRequestId slotRequestId,
			SlotProfile slotProfile,
			boolean allowQueuedScheduling,
			@Nullable Time allocationTimeout) {

		//note: 分配一个单独的 slot
		Optional<SlotAndLocality> slotAndLocality = tryAllocateFromAvailable(slotRequestId, slotProfile);

		if (slotAndLocality.isPresent()) {
			// already successful from available
			//note: 已经成功分配了 slot
			try {
				return CompletableFuture.completedFuture(
					completeAllocationByAssigningPayload(slotRequestId, slotAndLocality.get()));
			} catch (FlinkException e) {
				return FutureUtils.completedExceptionally(e);
			}
		} else if (allowQueuedScheduling) {
			// we allocate by requesting a new slot
			return requestNewAllocatedSlot(slotRequestId, slotProfile, allocationTimeout)
				.thenApply((PhysicalSlot allocatedSlot) -> {
					try {
						return completeAllocationByAssigningPayload(slotRequestId, new SlotAndLocality(allocatedSlot, Locality.UNKNOWN));
					} catch (FlinkException e) {
						throw new CompletionException(e);
					}
				});
		} else {
			// failed to allocate
			return FutureUtils.completedExceptionally(
				new NoResourceAvailableException("Could not allocate a simple slot for " + slotRequestId + '.'));
		}
	}

	@Nonnull
	private CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
			SlotRequestId slotRequestId,
			SlotProfile slotProfile,
			@Nullable Time allocationTimeout) {
		if (allocationTimeout == null) {
			return slotPool.requestNewAllocatedBatchSlot(slotRequestId, slotProfile.getResourceProfile());
		} else {
			return slotPool.requestNewAllocatedSlot(slotRequestId, slotProfile.getResourceProfile(), allocationTimeout);
		}
	}

	@Nonnull
	private LogicalSlot completeAllocationByAssigningPayload(
		@Nonnull SlotRequestId slotRequestId,
		@Nonnull SlotAndLocality slotAndLocality) throws FlinkException {

		final PhysicalSlot allocatedSlot = slotAndLocality.getSlot();

		final SingleLogicalSlot singleTaskSlot = new SingleLogicalSlot(
			slotRequestId,
			allocatedSlot,
			null,
			slotAndLocality.getLocality(),
			this);

		if (allocatedSlot.tryAssignPayload(singleTaskSlot)) {
			return singleTaskSlot;
		} else {
			final FlinkException flinkException =
				new FlinkException("Could not assign payload to allocated slot " + allocatedSlot.getAllocationId() + '.');
			slotPool.releaseSlot(slotRequestId, flinkException);
			throw flinkException;
		}
	}

	private Optional<SlotAndLocality> tryAllocateFromAvailable(
		@Nonnull SlotRequestId slotRequestId,
		@Nonnull SlotProfile slotProfile) {

		//note: 先返回 slot pool 可用的 slot 列表
		Collection<SlotSelectionStrategy.SlotInfoAndResources> slotInfoList =
				slotPool.getAvailableSlotsInformation()
						.stream()
						.map(SlotSelectionStrategy.SlotInfoAndResources::new)
						.collect(Collectors.toList());

		//note: 返回最佳的 slot
		Optional<SlotSelectionStrategy.SlotInfoAndLocality> selectedAvailableSlot =
			slotSelectionStrategy.selectBestSlotForProfile(slotInfoList, slotProfile);

		return selectedAvailableSlot.flatMap(slotInfoAndLocality -> {
			//note: 在 slot pool 中将这个 slot 分配给指定的 SlotRequestId
			Optional<PhysicalSlot> optionalAllocatedSlot = slotPool.allocateAvailableSlot(
				slotRequestId,
				slotInfoAndLocality.getSlotInfo().getAllocationId());

			return optionalAllocatedSlot.map(
				allocatedSlot -> new SlotAndLocality(allocatedSlot, slotInfoAndLocality.getLocality()));
		});
	}

	// ------------------------------- slot sharing code

	//note: allocate shared slot
	private CompletableFuture<LogicalSlot> allocateSharedSlot(
		SlotRequestId slotRequestId,
		ScheduledUnit scheduledUnit,
		SlotProfile slotProfile,
		boolean allowQueuedScheduling,
		@Nullable Time allocationTimeout) {
		// allocate slot with slot sharing
		//note: 取这个 SlotSharingGroupId 对应的 SlotManager
		final SlotSharingManager multiTaskSlotManager = slotSharingManagers.computeIfAbsent(
			scheduledUnit.getSlotSharingGroupId(),
			id -> new SlotSharingManager(
				id,
				slotPool,
				this));

		//note: 这里就拿到在 shared 模式下的 MultiTaskSlot
		final SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality;
		try {
			if (scheduledUnit.getCoLocationConstraint() != null) {
				multiTaskSlotLocality = allocateCoLocatedMultiTaskSlot(
					scheduledUnit.getCoLocationConstraint(),
					multiTaskSlotManager,
					slotProfile,
					allowQueuedScheduling,
					allocationTimeout);
			} else {
				multiTaskSlotLocality = allocateMultiTaskSlot(
					scheduledUnit.getJobVertexId(),
					multiTaskSlotManager,
					slotProfile,
					allowQueuedScheduling,
					allocationTimeout);
			}
		} catch (NoResourceAvailableException noResourceException) {
			return FutureUtils.completedExceptionally(noResourceException);
		}

		// sanity check
		Preconditions.checkState(!multiTaskSlotLocality.getMultiTaskSlot().contains(scheduledUnit.getJobVertexId()));

		final SlotSharingManager.SingleTaskSlot leaf = multiTaskSlotLocality.getMultiTaskSlot().allocateSingleTaskSlot(
			slotRequestId,
			slotProfile.getResourceProfile(),
			scheduledUnit.getJobVertexId(),
			multiTaskSlotLocality.getLocality());
		return leaf.getLogicalSlotFuture();
	}

	/**
	 * Allocates a co-located {@link SlotSharingManager.MultiTaskSlot} for the given {@link CoLocationConstraint}.
	 *
	 * <p>If allowQueuedScheduling is true, then the returned {@link SlotSharingManager.MultiTaskSlot} can be
	 * uncompleted.
	 *
	 * @param coLocationConstraint for which to allocate a {@link SlotSharingManager.MultiTaskSlot}
	 * @param multiTaskSlotManager responsible for the slot sharing group for which to allocate the slot
	 * @param slotProfile specifying the requirements for the requested slot
	 * @param allowQueuedScheduling true if queued scheduling (the returned task slot must not be completed yet) is allowed, otherwise false
	 * @param allocationTimeout timeout before the slot allocation times out
	 * @return A {@link SlotAndLocality} which contains the allocated{@link SlotSharingManager.MultiTaskSlot}
	 * 		and its locality wrt the given location preferences
	 */
	private SlotSharingManager.MultiTaskSlotLocality allocateCoLocatedMultiTaskSlot(
		CoLocationConstraint coLocationConstraint,
		SlotSharingManager multiTaskSlotManager,
		SlotProfile slotProfile,
		boolean allowQueuedScheduling,
		@Nullable Time allocationTimeout) throws NoResourceAvailableException {
		final SlotRequestId coLocationSlotRequestId = coLocationConstraint.getSlotRequestId();

		if (coLocationSlotRequestId != null) {
			// we have a slot assigned --> try to retrieve it
			final SlotSharingManager.TaskSlot taskSlot = multiTaskSlotManager.getTaskSlot(coLocationSlotRequestId);

			if (taskSlot != null) {
				Preconditions.checkState(taskSlot instanceof SlotSharingManager.MultiTaskSlot);

				SlotSharingManager.MultiTaskSlot multiTaskSlot = (SlotSharingManager.MultiTaskSlot) taskSlot;

				if (multiTaskSlot.mayHaveEnoughResourcesToFulfill(slotProfile.getResourceProfile())) {
					return SlotSharingManager.MultiTaskSlotLocality.of(multiTaskSlot, Locality.LOCAL);
				}

				throw new NoResourceAvailableException("Not enough resources in the slot for all co-located tasks.");
			} else {
				// the slot may have been cancelled in the mean time
				coLocationConstraint.setSlotRequestId(null);
			}
		}

		if (coLocationConstraint.isAssigned()) {
			// refine the preferred locations of the slot profile
			slotProfile = new SlotProfile(
				slotProfile.getResourceProfile(),
				Collections.singleton(coLocationConstraint.getLocation()), //note: 期望的 TaskManagerLocation
				slotProfile.getPreferredAllocations());
		}

		// get a new multi task slot
		SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality = allocateMultiTaskSlot(
			coLocationConstraint.getGroupId(),
			multiTaskSlotManager,
			slotProfile,
			allowQueuedScheduling,
			allocationTimeout);

		// check whether we fulfill the co-location constraint
		if (coLocationConstraint.isAssigned() && multiTaskSlotLocality.getLocality() != Locality.LOCAL) {
			multiTaskSlotLocality.getMultiTaskSlot().release(
				new FlinkException("Multi task slot is not local and, thus, does not fulfill the co-location constraint."));

			throw new NoResourceAvailableException("Could not allocate a local multi task slot for the " +
				"co location constraint " + coLocationConstraint + '.');
		}

		final SlotRequestId slotRequestId = new SlotRequestId();
		final SlotSharingManager.MultiTaskSlot coLocationSlot =
			multiTaskSlotLocality.getMultiTaskSlot().allocateMultiTaskSlot(
				slotRequestId,
				coLocationConstraint.getGroupId());

		// mark the requested slot as co-located slot for other co-located tasks
		coLocationConstraint.setSlotRequestId(slotRequestId);

		// lock the co-location constraint once we have obtained the allocated slot
		coLocationSlot.getSlotContextFuture().whenComplete(
			(SlotContext slotContext, Throwable throwable) -> {
				if (throwable == null) {
					// check whether we are still assigned to the co-location constraint
					if (Objects.equals(coLocationConstraint.getSlotRequestId(), slotRequestId)) {
						coLocationConstraint.lockLocation(slotContext.getTaskManagerLocation());
					} else {
						log.debug("Failed to lock colocation constraint {} because assigned slot " +
								"request {} differs from fulfilled slot request {}.",
							coLocationConstraint.getGroupId(),
							coLocationConstraint.getSlotRequestId(),
							slotRequestId);
					}
				} else {
					log.debug("Failed to lock colocation constraint {} because the slot " +
							"allocation for slot request {} failed.",
						coLocationConstraint.getGroupId(),
						coLocationConstraint.getSlotRequestId(),
						throwable);
				}
			});

		return SlotSharingManager.MultiTaskSlotLocality.of(coLocationSlot, multiTaskSlotLocality.getLocality());
	}

	/**
	 * Allocates a {@link SlotSharingManager.MultiTaskSlot} for the given groupId which is in the
	 * slot sharing group for which the given {@link SlotSharingManager} is responsible.
	 * note: 对于指定的 groupId（AbstractID）分配一个 MultiTaskSlot
	 *
	 * <p>If allowQueuedScheduling is true, then the method can return an uncompleted {@link SlotSharingManager.MultiTaskSlot}.
	 *
	 * @param groupId for which to allocate a new {@link SlotSharingManager.MultiTaskSlot}
	 * @param slotSharingManager responsible for the slot sharing group for which to allocate the slot
	 * @param slotProfile slot profile that specifies the requirements for the slot
	 * @param allowQueuedScheduling true if queued scheduling (the returned task slot must not be completed yet) is allowed, otherwise false
	 * @param allocationTimeout timeout before the slot allocation times out; null if requesting a batch slot
	 * @return A {@link SlotSharingManager.MultiTaskSlotLocality} which contains the allocated {@link SlotSharingManager.MultiTaskSlot}
	 * 		and its locality wrt the given location preferences
	 */
	private SlotSharingManager.MultiTaskSlotLocality allocateMultiTaskSlot(
			AbstractID groupId,
			SlotSharingManager slotSharingManager,
			SlotProfile slotProfile,
			boolean allowQueuedScheduling,
			@Nullable Time allocationTimeout) throws NoResourceAvailableException {

		//note: 从已经完成的 node 列表中过滤出与这个 groupId 相关的 slot（如果之前还没有这个 groupId 相关的 slot，这里会返回空集合）
		Collection<SlotSelectionStrategy.SlotInfoAndResources> resolvedRootSlotsInfo =
				slotSharingManager.listResolvedRootSlotInfo(groupId);

		//note: 如果上面得到的集合非空，这里会返回最佳的 slot，否则得到也是 empty
		SlotSelectionStrategy.SlotInfoAndLocality bestResolvedRootSlotWithLocality =
			slotSelectionStrategy.selectBestSlotForProfile(resolvedRootSlotsInfo, slotProfile).orElse(null);

		//note: 根据分配算法拿到的最佳 MultiTaskSlotLocality
		final SlotSharingManager.MultiTaskSlotLocality multiTaskSlotLocality = bestResolvedRootSlotWithLocality != null ?
			new SlotSharingManager.MultiTaskSlotLocality(
				slotSharingManager.getResolvedRootSlot(bestResolvedRootSlotWithLocality.getSlotInfo()),
				bestResolvedRootSlotWithLocality.getLocality()) :
			null;

		if (multiTaskSlotLocality != null && multiTaskSlotLocality.getLocality() == Locality.LOCAL) {
			//note: 如果前面拿到了最佳的分配 slot，并且还是分配到指定请求的 TM 上，这里就直接返回结果了
			return multiTaskSlotLocality;
		}

		final SlotRequestId allocatedSlotRequestId = new SlotRequestId();
		final SlotRequestId multiTaskSlotRequestId = new SlotRequestId();

		//note: 根据 slotProfile 获取最佳的 slot
		Optional<SlotAndLocality> optionalPoolSlotAndLocality = tryAllocateFromAvailable(allocatedSlotRequestId, slotProfile);

		if (optionalPoolSlotAndLocality.isPresent()) {
			SlotAndLocality poolSlotAndLocality = optionalPoolSlotAndLocality.get();
			if (poolSlotAndLocality.getLocality() == Locality.LOCAL || bestResolvedRootSlotWithLocality == null) {

				//note: 如果这里拿到的 slot 与要分配的 slot 是在同一个 TM 上并且之前在 SlotSharingManager 中没有获取到合适的 slot
				final PhysicalSlot allocatedSlot = poolSlotAndLocality.getSlot();
				//note: 新创建一个 MultiTaskSlot 对象
				final SlotSharingManager.MultiTaskSlot multiTaskSlot = slotSharingManager.createRootSlot(
					multiTaskSlotRequestId,
					CompletableFuture.completedFuture(poolSlotAndLocality.getSlot()),
					allocatedSlotRequestId);

				//note: 将这个 multiTaskSlot 绑定到这个 slot 上
				if (allocatedSlot.tryAssignPayload(multiTaskSlot)) {
					return SlotSharingManager.MultiTaskSlotLocality.of(multiTaskSlot, poolSlotAndLocality.getLocality());
				} else {
					//note: 如果不能绑定（之前已经有其他的 task slot 绑定过了）
					multiTaskSlot.release(new FlinkException("Could not assign payload to allocated slot " +
						allocatedSlot.getAllocationId() + '.'));
				}
			}
		}

		if (multiTaskSlotLocality != null) {
			// prefer slot sharing group slots over unused slots
			if (optionalPoolSlotAndLocality.isPresent()) {
				//note: 如果在 SlotSharingManager 申请到了，但是不满足最优的 Locality 条件，这里会释放掉 tryAllocateFromAvailable 申请的
				slotPool.releaseSlot(
					allocatedSlotRequestId,
					new FlinkException("Locality constraint is not better fulfilled by allocated slot."));
			}
			//note: 返回 SlotSharingManager 中申请到的 slot
			return multiTaskSlotLocality;
		}

		if (allowQueuedScheduling) {
			// there is no slot immediately available --> check first for uncompleted slots at the slot sharing group
			//note: 返回 UnresolvedR slot 列表中不包含指定 groupId 的第一个 MultiTaskSlot
			SlotSharingManager.MultiTaskSlot multiTaskSlot = slotSharingManager.getUnresolvedRootSlot(groupId);

			if (multiTaskSlot == null) { //note: 如果还没有的情况下
				// it seems as if we have to request a new slot from the resource manager, this is always the last resort!!!
				//note: 这里向 resource manager 请求新的 slot
				final CompletableFuture<PhysicalSlot> slotAllocationFuture = requestNewAllocatedSlot(
					allocatedSlotRequestId,
					slotProfile,
					allocationTimeout);

				//note: 创建一个新的 multiTaskSlot 对象
				multiTaskSlot = slotSharingManager.createRootSlot(
					multiTaskSlotRequestId,
					slotAllocationFuture,
					allocatedSlotRequestId);

				slotAllocationFuture.whenComplete(
					(PhysicalSlot allocatedSlot, Throwable throwable) -> {
						final SlotSharingManager.TaskSlot taskSlot = slotSharingManager.getTaskSlot(multiTaskSlotRequestId);

						if (taskSlot != null) {
							// still valid
							if (!(taskSlot instanceof SlotSharingManager.MultiTaskSlot) || throwable != null) {
								taskSlot.release(throwable);
							} else {
								if (!allocatedSlot.tryAssignPayload(((SlotSharingManager.MultiTaskSlot) taskSlot))) {
									taskSlot.release(new FlinkException("Could not assign payload to allocated slot " +
										allocatedSlot.getAllocationId() + '.'));
								}
							}
						} else {
							slotPool.releaseSlot(
								allocatedSlotRequestId,
								new FlinkException("Could not find task slot with " + multiTaskSlotRequestId + '.'));
						}
					});
			}

			return SlotSharingManager.MultiTaskSlotLocality.of(multiTaskSlot, Locality.UNKNOWN);
		}

		throw new NoResourceAvailableException("Could not allocate a shared slot for " + groupId + '.');
	}

	private void releaseSharedSlot(
		@Nonnull SlotRequestId slotRequestId,
		@Nonnull SlotSharingGroupId slotSharingGroupId,
		Throwable cause) {

		final SlotSharingManager multiTaskSlotManager = slotSharingManagers.get(slotSharingGroupId);

		if (multiTaskSlotManager != null) {
			final SlotSharingManager.TaskSlot taskSlot = multiTaskSlotManager.getTaskSlot(slotRequestId);

			if (taskSlot != null) {
				taskSlot.release(cause);
			} else {
				log.debug("Could not find slot [{}] in slot sharing group {}. Ignoring release slot request.", slotRequestId, slotSharingGroupId);
			}
		} else {
			log.debug("Could not find slot sharing group {}. Ignoring release slot request.", slotSharingGroupId);
		}
	}

	@Override
	public boolean requiresPreviousExecutionGraphAllocations() {
		return slotSelectionStrategy instanceof PreviousAllocationSlotSelectionStrategy;
	}
}
