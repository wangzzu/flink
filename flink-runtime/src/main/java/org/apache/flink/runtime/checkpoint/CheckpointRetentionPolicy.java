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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.Internal;

/**
 * Policy for whether checkpoints are retained after a job terminates.
 */
@Internal
public enum CheckpointRetentionPolicy {

	/** Checkpoints should be retained on cancellation and failure. */
	//note: 当前作业取消或失败时，checkpoint 应该被继续保存
	RETAIN_ON_CANCELLATION,

	/** Checkpoints should be retained on failure, but not on cancellation. */
	//note: 作业失败的时候，checkpoint 应该被继续持有，但是如果作业取消的话，就不会
	RETAIN_ON_FAILURE,

	/** Checkpoints should always be cleaned up when an application reaches a terminal state. */
	//note: 如果作业进入了一个终止状态，checkpoint 就应该被清除
	NEVER_RETAIN_AFTER_TERMINATION;
}
