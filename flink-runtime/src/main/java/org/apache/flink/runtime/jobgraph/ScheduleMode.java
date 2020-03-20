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

package org.apache.flink.runtime.jobgraph;

/**
 * The ScheduleMode decides how tasks of an execution graph are started.
 * note：调度策略：决定 execution graph 的 task 怎么启动
 */
public enum ScheduleMode {
	//note: LAZY 应该是主要为 批 服务的
	/** Schedule tasks lazily from the sources. Downstream tasks are started once their input data are ready */
	//note: 从 source 端开始调度，一旦输入数据 ready 下游 task 开始启动
	LAZY_FROM_SOURCES(true),

	/**
	 * Same as LAZY_FROM_SOURCES just with the difference that it uses batch slot requests which support the
	 * execution of jobs with fewer slots than requested. However, the user needs to make sure that the job
	 * does not contain any pipelined shuffles (every pipelined region can be executed with a single slot).
	 */
	//note: 与 LAZY_FROM_SOURCES 不同的是，它使用 batch slot request，并且支持作业执行即使使用的 slot 比请求的要少
	LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST(true),

	/** Schedules all tasks immediately. */
	//note: Stream 模式的默认值，立即调度所有的 task
	EAGER(false);

	private final boolean allowLazyDeployment;

	ScheduleMode(boolean allowLazyDeployment) {
		this.allowLazyDeployment = allowLazyDeployment;
	}

	/**
	 * Returns whether we are allowed to deploy consumers lazily.
	 */
	public boolean allowLazyDeployment() {
		return allowLazyDeployment;
	}
}
