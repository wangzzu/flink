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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Manager which is responsible for slot sharing. Slot sharing allows to run different
 * tasks in the same slot and to realize co-location constraints.
 * note: 主要负责 slot sharing 的 manager，它运行在同一个 slot 中运行不同的 task 并实现约束共享
 *
 * <p>The SlotSharingManager allows to create a hierarchy of {@link TaskSlot} such that
 * every {@link TaskSlot} is uniquely identified by a {@link SlotRequestId} identifying
 * the request for the TaskSlot and a {@link AbstractID} identifying the task or the
 * co-location constraint running in this slot.
 * note: SlotSharingManager 管理着一个有层次结构的 TaskSlot 集合，每一个 TaskSlot 是被唯一的 SlotRequestId 确认（AbstractID 表示每一个 task）
 *
 * note: TaskSlot 分为两种: MultiTaskSlot（它可以包含多个 TaskSlot）和 SingleTaskSlot（它是叶子节点）；
 * note: root 节点（起始节点）是从 root MultiTaskSlot 开始，
 *
 * <p>The {@link TaskSlot} hierarchy is implemented by {@link MultiTaskSlot} and
 * {@link SingleTaskSlot}. The former class represents inner nodes which can contain
 * a number of other {@link TaskSlot} and the latter class represents the leaf nodes.
 * The hierarchy starts with a root {@link MultiTaskSlot} which is a future
 * {@link SlotContext} assigned. The {@link SlotContext} represents the allocated slot
 * on the TaskExecutor in which all slots of this hierarchy run. A {@link MultiTaskSlot}
 * can be assigned multiple {@link SingleTaskSlot} or {@link MultiTaskSlot} if and only if
 * the task slot does not yet contain another child with the same {@link AbstractID} identifying
 * the actual task or the co-location constraint.
 *
 * <p>Normal slot sharing is represented by a root {@link MultiTaskSlot} which contains a set
 * of {@link SingleTaskSlot} on the second layer. Each {@link SingleTaskSlot} represents a different
 * task.
 *
 * <p>Co-location constraints are modeled by adding a {@link MultiTaskSlot} to the root node. The co-location
 * constraint is uniquely identified by a {@link AbstractID} such that we cannot add a second co-located
 * {@link MultiTaskSlot} to the same root node. Now all co-located tasks will be added to co-located
 * multi task slot.
 */
public class SlotSharingManager {

	private static final Logger LOG = LoggerFactory.getLogger(SlotSharingManager.class);

	private final SlotSharingGroupId slotSharingGroupId;

	/** Actions to release allocated slots after a complete multi task slot hierarchy has been released. */
	private final AllocatedSlotActions allocatedSlotActions;

	/** Owner of the slots to which to return them when they are released from the outside. */
	private final SlotOwner slotOwner;

	private final Map<SlotRequestId, TaskSlot> allTaskSlots;

	/** Root nodes which have not been completed because the allocated slot is still pending. */
	//note: 还未完成的 slot request
	private final Map<SlotRequestId, MultiTaskSlot> unresolvedRootSlots;

	/** Root nodes which have been completed (the underlying allocated slot has been assigned). */
	//note: 已经分配完成的 node 列表
	private final Map<TaskManagerLocation, Map<AllocationID, MultiTaskSlot>> resolvedRootSlots;

	SlotSharingManager(
			SlotSharingGroupId slotSharingGroupId,
			AllocatedSlotActions allocatedSlotActions,
			SlotOwner slotOwner) {
		this.slotSharingGroupId = Preconditions.checkNotNull(slotSharingGroupId);
		this.allocatedSlotActions = Preconditions.checkNotNull(allocatedSlotActions);
		this.slotOwner = Preconditions.checkNotNull(slotOwner);

		allTaskSlots = new HashMap<>(16);
		unresolvedRootSlots = new HashMap<>(16);
		resolvedRootSlots = new HashMap<>(16);
	}

	public boolean isEmpty() {
		return allTaskSlots.isEmpty();
	}

	public boolean contains(SlotRequestId slotRequestId) {
		return allTaskSlots.containsKey(slotRequestId);
	}

	@Nullable
	TaskSlot getTaskSlot(SlotRequestId slotRequestId) {
		return allTaskSlots.get(slotRequestId);
	}

	/**
	 * Creates a new root slot with the given {@link SlotRequestId}, {@link SlotContext} future and
	 * the {@link SlotRequestId} of the allocated slot.
	 * note: 根据指定的 SlotRequestId 创建一个 MultiTaskSlot 对象
	 *
	 * @param slotRequestId of the root slot
	 * @param slotContextFuture with which we create the root slot
	 * @param allocatedSlotRequestId slot request id of the underlying allocated slot which can be used
	 *                               to cancel the pending slot request or release the allocated slot
	 * @return New root slot
	 */
	@Nonnull
	MultiTaskSlot createRootSlot(
			SlotRequestId slotRequestId,
			CompletableFuture<? extends SlotContext> slotContextFuture,
			SlotRequestId allocatedSlotRequestId) {
		//note: 先创建一个 MultiTaskSlot 对象（root 节点一定是 MultiTaskSlot 类型）
		final MultiTaskSlot rootMultiTaskSlot = new MultiTaskSlot(
			slotRequestId,
			slotContextFuture,
			allocatedSlotRequestId);

		LOG.debug("Create multi task slot [{}] in slot [{}].", slotRequestId, allocatedSlotRequestId);

		allTaskSlots.put(slotRequestId, rootMultiTaskSlot);

		//note: 正在处理，还没完成的记录
		unresolvedRootSlots.put(slotRequestId, rootMultiTaskSlot);

		// add the root node to the set of resolved root nodes once the SlotContext future has
		// been completed and we know the slot's TaskManagerLocation
		slotContextFuture.whenComplete(
			(SlotContext slotContext, Throwable throwable) -> {
				if (slotContext != null) {
					final MultiTaskSlot resolvedRootNode = unresolvedRootSlots.remove(slotRequestId);

					if (resolvedRootNode != null) {
						final AllocationID allocationId = slotContext.getAllocationId();
						LOG.trace("Fulfill multi task slot [{}] with slot [{}].", slotRequestId, allocationId);

						final Map<AllocationID, MultiTaskSlot> innerMap = resolvedRootSlots.computeIfAbsent(
							slotContext.getTaskManagerLocation(),
							taskManagerLocation -> new HashMap<>(4));

						//note: 这里把 root MultiTaskSlot 放到 TaskManagerLocation 对应的集合中
						MultiTaskSlot previousValue = innerMap.put(allocationId, resolvedRootNode);
						Preconditions.checkState(previousValue == null);
					}
				} else {
					rootMultiTaskSlot.release(throwable);
				}
			});

		return rootMultiTaskSlot;
	}

	@Nonnull
	public Collection<SlotSelectionStrategy.SlotInfoAndResources> listResolvedRootSlotInfo(@Nullable AbstractID groupId) {
		return resolvedRootSlots
			.values()
			.stream()
				.flatMap((Map<AllocationID, MultiTaskSlot> map) -> map.values().stream())
				//note: 过滤出不包括这个 groupId 及还没释放的 slot
				.filter(validMultiTaskSlotAndDoesNotContain(groupId))
				.map((MultiTaskSlot multiTaskSlot) -> {
					SlotInfo slotInfo = multiTaskSlot.getSlotContextFuture().join();
					return new SlotSelectionStrategy.SlotInfoAndResources(
							slotInfo,
							slotInfo.getResourceProfile().subtract(multiTaskSlot.getReservedResources()));
				}).collect(Collectors.toList());
	}

	private Predicate<MultiTaskSlot> validMultiTaskSlotAndDoesNotContain(@Nullable AbstractID groupId) {
		return (MultiTaskSlot multiTaskSlot) -> !multiTaskSlot.contains(groupId) && !multiTaskSlot.isReleasing();
	}

	@Nullable
	public MultiTaskSlot getResolvedRootSlot(@Nonnull SlotInfo slotInfo) {
		//note: 根据 TM 位置信息，拿到对应的 MultiTaskSlot
		Map<AllocationID, MultiTaskSlot> forLocationEntry = resolvedRootSlots.get(slotInfo.getTaskManagerLocation());
		//note: 再根据 AbstractID 拿到具体的 MultiTaskSlot
		return forLocationEntry != null ? forLocationEntry.get(slotInfo.getAllocationId()) : null;
	}

	/**
	 * Gets an unresolved slot which does not yet contain the given groupId. An unresolved
	 * slot is a slot whose underlying allocated slot has not been allocated yet.
	 * note: unresolved slot 列表中不包含指定 groupId 的第一个 MultiTaskSlot
	 *
	 * @param groupId which the returned slot must not contain
	 * @return the unresolved slot or null if there was no root slot with free capacities
	 */
	@Nullable
	MultiTaskSlot getUnresolvedRootSlot(AbstractID groupId) {
		return unresolvedRootSlots.values().stream()
			.filter(validMultiTaskSlotAndDoesNotContain(groupId))
			.findFirst()
			.orElse(null);
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder("{\n\tgroupId=").append(slotSharingGroupId).append('\n');

		builder.append("\tunresolved=").append(unresolvedRootSlots).append('\n');
		builder.append("\tresolved=").append(resolvedRootSlots).append('\n');
		builder.append("\tall=").append(allTaskSlots).append('\n');

		return builder.append('}').toString();
	}

	// ------------------------------------------------------------------------
	// Inner classes: TaskSlot hierarchy and helper classes
	// ------------------------------------------------------------------------

	/**
	 * Helper class which contains a {@link MultiTaskSlot} and its {@link Locality}.
	 */
	static final class MultiTaskSlotLocality {
		private final MultiTaskSlot multiTaskSlot;

		//note: 调度后的位置情况
		private final Locality locality;

		MultiTaskSlotLocality(MultiTaskSlot multiTaskSlot, Locality locality) {
			this.multiTaskSlot = Preconditions.checkNotNull(multiTaskSlot);
			this.locality = Preconditions.checkNotNull(locality);
		}

		MultiTaskSlot getMultiTaskSlot() {
			return multiTaskSlot;
		}

		public Locality getLocality() {
			return locality;
		}

		public static MultiTaskSlotLocality of(MultiTaskSlot multiTaskSlot, Locality locality) {
			return new MultiTaskSlotLocality(multiTaskSlot, locality);
		}
	}

	/**
	 * Base class for all task slots.
	 */
	public abstract static class TaskSlot {
		// every TaskSlot has an associated slot request id
		private final SlotRequestId slotRequestId;

		// all task slots except for the root slots have a group id assigned
		@Nullable
		private final AbstractID groupId;

		TaskSlot(SlotRequestId slotRequestId, @Nullable AbstractID groupId) {
			this.slotRequestId = Preconditions.checkNotNull(slotRequestId);
			this.groupId = groupId;
		}

		public SlotRequestId getSlotRequestId() {
			return slotRequestId;
		}

		@Nullable
		public AbstractID getGroupId() {
			return groupId;
		}

		/**
		 * Check whether the task slot contains the given groupId.
		 *
		 * @param groupId which to check whether it is contained
		 * @return true if the task slot contains the given groupId, otherwise false
		 */
		public boolean contains(AbstractID groupId) {
			return Objects.equals(this.groupId, groupId);
		}

		/**
		 * Release the task slot.
		 *
		 * @param cause for the release
		 */
		public abstract void release(Throwable cause);

		/**
		 * Gets the total reserved resources of the slot and its descendants.
		 */
		public abstract ResourceProfile getReservedResources();
	}

	/**
	 * {@link TaskSlot} implementation which can have multiple other task slots assigned as children.
	 * note: 主要针对 shared slot 的情况，与 SingleTaskSlot 相对应
	 */
	public final class MultiTaskSlot extends TaskSlot implements PhysicalSlot.Payload {

		//note: 这个 MultiTaskSlot 包含的 task slot
		private final Map<AbstractID, TaskSlot> children;

		// the root node has its parent set to null
		@Nullable
		//note: 记录这个 slot 父节点，它可能是来自于一个 MultiTaskSlot（也即 parent），有可能是是 root 节点，那这个值就为 null
		private final MultiTaskSlot parent;

		// underlying allocated slot
		private final CompletableFuture<? extends SlotContext> slotContextFuture;

		// slot request id of the allocated slot
		@Nullable
		private final SlotRequestId allocatedSlotRequestId;

		// true if we are currently releasing our children
		private boolean releasingChildren;

		// the total resources reserved by all the descendants.
		//note: 记录它所有 children 的资源
		private ResourceProfile reservedResources;

		private MultiTaskSlot(
				SlotRequestId slotRequestId,
				AbstractID groupId,
				MultiTaskSlot parent) {
			this(
				slotRequestId,
				groupId,
				Preconditions.checkNotNull(parent),
				parent.getSlotContextFuture(),
				null);
		}

		private MultiTaskSlot(
				SlotRequestId slotRequestId,
				CompletableFuture<? extends SlotContext> slotContextFuture,
				SlotRequestId allocatedSlotRequestId) {
			this(
				slotRequestId,
				null,
				null,
				slotContextFuture,
				allocatedSlotRequestId);
		}

		private MultiTaskSlot(
				SlotRequestId slotRequestId,
				@Nullable AbstractID groupId,
				@Nullable MultiTaskSlot parent,
				CompletableFuture<? extends SlotContext> slotContextFuture,
				@Nullable SlotRequestId allocatedSlotRequestId) {
			super(slotRequestId, groupId);
			Preconditions.checkNotNull(slotContextFuture);

			this.parent = parent;
			this.allocatedSlotRequestId = allocatedSlotRequestId;

			this.children = new HashMap<>(16);
			this.releasingChildren = false;

			this.reservedResources = ResourceProfile.ZERO;

			this.slotContextFuture = slotContextFuture.handle((SlotContext slotContext, Throwable throwable) -> {
				if (throwable != null) {
					// If the underlying resource request failed, we currently fail all the requests
					release(throwable);
					throw new CompletionException(throwable);
				}

				if (parent == null) {
					//note: 对于 root 节点才会做这个操作（统计这个 Root TaskSlot 相关信息）
					checkOversubscriptionAndReleaseChildren(slotContext);
				}

				return slotContext;
			});
		}

		CompletableFuture<? extends SlotContext> getSlotContextFuture() {
			return slotContextFuture;
		}

		/**
		 * Allocates a MultiTaskSlot and registers it under the given groupId at
		 * this MultiTaskSlot.
		 * note: 在 MultiTaskSlot 中注册一个 MultiTaskSlot
		 *
		 * @param slotRequestId of the new multi task slot
		 * @param groupId under which the new multi task slot is registered
		 * @return the newly allocated MultiTaskSlot
		 */
		MultiTaskSlot allocateMultiTaskSlot(SlotRequestId slotRequestId, AbstractID groupId) {
			Preconditions.checkState(!super.contains(groupId));

			LOG.debug("Create nested multi task slot [{}] in parent multi task slot [{}] for group [{}].", slotRequestId, getSlotRequestId(), groupId);

			final MultiTaskSlot inner = new MultiTaskSlot(
				slotRequestId,
				groupId,
				this);

			children.put(groupId, inner);

			// register the newly allocated slot also at the SlotSharingManager
			allTaskSlots.put(slotRequestId, inner);

			return inner;
		}

		/**
		 * Allocates a {@link SingleTaskSlot} and registers it under the given groupId at
		 * this MultiTaskSlot.
		 *
		 * @param slotRequestId of the new single task slot
		 * @param groupId under which the new single task slot is registered
		 * @param locality of the allocation
		 * @return the newly allocated {@link SingleTaskSlot}
		 */
		SingleTaskSlot allocateSingleTaskSlot(
				SlotRequestId slotRequestId,
				ResourceProfile resourceProfile,
				AbstractID groupId,
				Locality locality) {
			Preconditions.checkState(!super.contains(groupId));

			LOG.debug("Create single task slot [{}] in multi task slot [{}] for group {}.", slotRequestId, getSlotRequestId(), groupId);

			//note: 创建一个 SingleTaskSlot 对象
			final SingleTaskSlot leaf = new SingleTaskSlot(
				slotRequestId,
				resourceProfile,
				groupId,
				this,
				locality);

			children.put(groupId, leaf);

			// register the newly allocated slot also at the SlotSharingManager
			allTaskSlots.put(slotRequestId, leaf);

			// TODO: 2019-12-27 这里应该在创建 allocateSingleTaskSlot 的时候，应该要判断一下资源是否能满足 
			reserveResource(resourceProfile);

			return leaf;
		}

		/**
		 * Checks whether this slot or any of its children contains the given groupId.
		 *
		 * @param groupId which to check whether it is contained
		 * @return true if this or any of its children contains the given groupId, otherwise false
		 */
		@Override
		public boolean contains(AbstractID groupId) {
			if (super.contains(groupId)) {
				return true;
			} else {
				for (TaskSlot taskSlot : children.values()) {
					if (taskSlot.contains(groupId)) {
						return true;
					}
				}

				return false;
			}
		}

		//note: 释放这个 task slot
		@Override
		public void release(Throwable cause) {
			releasingChildren = true;

			// first release all children and remove them if they could be released immediately
			for (TaskSlot taskSlot : children.values()) {
				taskSlot.release(cause);
				allTaskSlots.remove(taskSlot.getSlotRequestId());
			}

			children.clear();

			releasingChildren = false;

			if (parent != null) {
				// we remove ourselves from our parent if we no longer have children
				parent.releaseChild(getGroupId());
			} else if (allTaskSlots.remove(getSlotRequestId()) != null) {
				// we are the root node --> remove the root node from the list of task slots
				final MultiTaskSlot unresolvedRootSlot = unresolvedRootSlots.remove(getSlotRequestId());

				if (unresolvedRootSlot == null) {
					// the root node should be resolved --> we can access the slot context
					final SlotContext slotContext = slotContextFuture.getNow(null);

					if (slotContext != null) {
						final Map<AllocationID, MultiTaskSlot> multiTaskSlots =
							resolvedRootSlots.get(slotContext.getTaskManagerLocation());

						if (multiTaskSlots != null) {
							MultiTaskSlot removedSlot = multiTaskSlots.remove(slotContext.getAllocationId());
							Preconditions.checkState(removedSlot == this);

							if (multiTaskSlots.isEmpty()) {
								resolvedRootSlots.remove(slotContext.getTaskManagerLocation());
							}
						}
					}
				}

				// release the underlying allocated slot
				allocatedSlotActions.releaseSlot(allocatedSlotRequestId, cause);
			}
		}

		@Override
		public ResourceProfile getReservedResources() {
			return reservedResources;
		}

		/**
		 * Checks if the task slot may have enough resource to fulfill the specific
		 * request. If the underlying slot is not allocated, the check is skipped.
		 *
		 * @param resourceProfile The specific request to check.
		 * @return Whether the slot is possible to fulfill the request in the future.
		 */
		boolean mayHaveEnoughResourcesToFulfill(ResourceProfile resourceProfile) {
			if (!slotContextFuture.isDone()) {
				return true;
			}

			MultiTaskSlot root = this;

			while (root.parent != null) {
				root = root.parent;
			}

			SlotContext slotContext = root.getSlotContextFuture().join();

			return slotContext.getResourceProfile().isMatching(
					resourceProfile.merge(root.getReservedResources()));
		}

		/**
		 * Releases the child with the given childGroupId.
		 *
		 * @param childGroupId identifying the child to release
		 */
		private void releaseChild(AbstractID childGroupId) {
			if (!releasingChildren) {
				TaskSlot child = children.remove(childGroupId);

				if (child != null) {
					allTaskSlots.remove(child.getSlotRequestId());

					// Update the resources of this slot and the parents
					releaseResource(child.getReservedResources());
				}

				if (children.isEmpty()) {
					release(new FlinkException("Release multi task slot because all children have been released."));
				}
			}
		}

		private void reserveResource(ResourceProfile resourceProfile) {
			reservedResources = reservedResources.merge(resourceProfile);

			if (parent != null) {
				parent.reserveResource(resourceProfile);
			}
		}

		private void releaseResource(ResourceProfile resourceProfile) {
			reservedResources = reservedResources.subtract(resourceProfile);

			if (parent != null) {
				parent.releaseResource(resourceProfile);
			}
		}

		//note: 这里主要是为了检查 slot 资源是否满足
		private void checkOversubscriptionAndReleaseChildren(SlotContext slotContext) {
			final ResourceProfile slotResources = slotContext.getResourceProfile();
			final ArrayList<TaskSlot> childrenToEvict = new ArrayList<>();
			ResourceProfile requiredResources = ResourceProfile.ZERO;

			for (TaskSlot slot : children.values()) {
				final ResourceProfile resourcesWithChild = requiredResources.merge(slot.getReservedResources());

				if (slotResources.isMatching(resourcesWithChild)) {
					//note: 能满足资源需要的情况下
					requiredResources = resourcesWithChild;
				} else {
					childrenToEvict.add(slot);
				}
			}

			if (LOG.isDebugEnabled()) {
				LOG.debug("Not all requests are fulfilled due to over-allocated, number of requests is {}, " +
						"number of evicted requests is {}, underlying allocated is {}, fulfilled is {}, " +
						"evicted requests is {},",
					children.size(),
					childrenToEvict.size(),
					slotContext.getResourceProfile(),
					requiredResources,
					childrenToEvict);
			}

			if (childrenToEvict.size() == children.size()) {
				// Since RM always return a slot whose resource is larger than the requested one,
				// The current situation only happens when we request to RM using the resource
				// profile of a task who is belonging to a CoLocationGroup. Similar to dealing
				// with the failure of the underlying request, currently we fail all the requests
				// directly.
				release(new SharedSlotOversubscribedException(
					"The allocated slot does not have enough resource for any task.", false));
			} else {
				for (TaskSlot taskSlot : childrenToEvict) {
					//note: 资源不足，释放了这个资源
					taskSlot.release(new SharedSlotOversubscribedException(
						"The allocated slot does not have enough resource for all the tasks.", true));
				}
			}
		}

		boolean isReleasing() {
			return releasingChildren;
		}

		@Override
		public String toString() {
			String physicalSlotDescription;
			try {
				physicalSlotDescription = String.valueOf(slotContextFuture.getNow(null));
			}
			catch (Exception e) {
				physicalSlotDescription = '(' + ExceptionUtils.stripCompletionException(e).getMessage() + ')';
			}

			return "MultiTaskSlot{"
					+ "requestId=" + getSlotRequestId()
					+ ", allocatedRequestId=" + allocatedSlotRequestId
					+ ", groupId=" + getGroupId()
					+ ", physicalSlot=" + physicalSlotDescription
					+ ", children=" + children.values().toString()
					+ '}';
		}
	}

	/**
	 * {@link TaskSlot} implementation which harbours a {@link LogicalSlot}. The {@link SingleTaskSlot}
	 * cannot have any children assigned.
	 */
	public final class SingleTaskSlot extends TaskSlot {
		private final MultiTaskSlot parent;

		// future containing a LogicalSlot which is completed once the underlying SlotContext future is completed
		private final CompletableFuture<SingleLogicalSlot> singleLogicalSlotFuture;

		// the resource profile of this slot.
		private final ResourceProfile resourceProfile;

		private SingleTaskSlot(
				SlotRequestId slotRequestId,
				ResourceProfile resourceProfile,
				AbstractID groupId,
				MultiTaskSlot parent,
				Locality locality) {
			super(slotRequestId, groupId);

			this.resourceProfile = Preconditions.checkNotNull(resourceProfile);
			//note: 记录其 parent
			this.parent = Preconditions.checkNotNull(parent);

			Preconditions.checkNotNull(locality);
			//note: 创建这个 SingleTaskSlot 对应的 SingleLogicalSlot
			singleLogicalSlotFuture = parent.getSlotContextFuture()
				.thenApply(
					(SlotContext slotContext) -> {
						LOG.trace("Fulfill single task slot [{}] with slot [{}].", slotRequestId, slotContext.getAllocationId());
						return new SingleLogicalSlot(
							slotRequestId,
							slotContext,
							slotSharingGroupId,
							locality,
							slotOwner);
					});
		}

		CompletableFuture<LogicalSlot> getLogicalSlotFuture() {
			return singleLogicalSlotFuture.thenApply(Function.identity());
		}

		@Override
		public void release(Throwable cause) {
			singleLogicalSlotFuture.completeExceptionally(cause);

			if (singleLogicalSlotFuture.isDone() && !singleLogicalSlotFuture.isCompletedExceptionally()) {
				// we have a single task slot which we first have to release
				final SingleLogicalSlot singleLogicalSlot = singleLogicalSlotFuture.getNow(null);

				singleLogicalSlot.release(cause);
			}

			parent.releaseChild(getGroupId());
		}

		@Override
		public ResourceProfile getReservedResources() {
			return resourceProfile;
		}

		@Override
		public String toString() {
			String logicalSlotString = "(pending)";
			try {
				LogicalSlot slot = singleLogicalSlotFuture.getNow(null);
				if (slot != null) {
					logicalSlotString = "(requestId=" + slot.getSlotRequestId()
							+ ", allocationId=" + slot.getAllocationId() + ')';
				}
			}
			catch (Exception e) {
				logicalSlotString = '(' + ExceptionUtils.stripCompletionException(e).getMessage() + ')';
			}

			return "SingleTaskSlot{"
					+ "logicalSlot=" + logicalSlotString
					+ ", request=" + getSlotRequestId()
					+ ", group=" + getGroupId()
					+ '}';
		}
	}

	// ------------------------------------------------------------------------
	// Methods and classes for testing
	// ------------------------------------------------------------------------

	/**
	 * Returns a collection of all resolved root slots.
	 *
	 * @return Collection of all resolved root slots
	 */
	@VisibleForTesting
	public Collection<MultiTaskSlot> getResolvedRootSlots() {
		return new ResolvedRootSlotValues();
	}

	@VisibleForTesting
	Collection<MultiTaskSlot> getUnresolvedRootSlots() {
		return unresolvedRootSlots.values();
	}

	/**
	 * Collection of all resolved {@link MultiTaskSlot} root slots.
	 */
	private final class ResolvedRootSlotValues extends AbstractCollection<MultiTaskSlot> {

		@Override
		public Iterator<MultiTaskSlot> iterator() {
			return new ResolvedRootSlotIterator(resolvedRootSlots.values().iterator());
		}

		@Override
		public int size() {
			int numberResolvedMultiTaskSlots = 0;

			for (Map<AllocationID, MultiTaskSlot> multiTaskSlots : resolvedRootSlots.values()) {
				numberResolvedMultiTaskSlots += multiTaskSlots.size();
			}

			return numberResolvedMultiTaskSlots;
		}
	}

	/**
	 * Iterator over all resolved {@link MultiTaskSlot} root slots.
	 */
	private static final class ResolvedRootSlotIterator implements Iterator<MultiTaskSlot> {
		private final Iterator<Map<AllocationID, MultiTaskSlot>> baseIterator;
		private Iterator<MultiTaskSlot> currentIterator;

		private ResolvedRootSlotIterator(Iterator<Map<AllocationID, MultiTaskSlot>> baseIterator) {
			this.baseIterator = Preconditions.checkNotNull(baseIterator);

			if (baseIterator.hasNext()) {
				currentIterator = baseIterator.next().values().iterator();
			} else {
				currentIterator = Collections.emptyIterator();
			}
		}

		@Override
		public boolean hasNext() {
			progressToNextElement();

			return currentIterator.hasNext();
		}

		@Override
		public MultiTaskSlot next() {
			progressToNextElement();

			return currentIterator.next();
		}

		private void progressToNextElement() {
			while (baseIterator.hasNext() && !currentIterator.hasNext()) {
				currentIterator = baseIterator.next().values().iterator();
			}
		}
	}
}
