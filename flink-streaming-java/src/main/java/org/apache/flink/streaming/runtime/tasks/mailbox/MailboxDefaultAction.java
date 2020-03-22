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

package org.apache.flink.streaming.runtime.tasks.mailbox;

import org.apache.flink.annotation.Internal;

/**
 * Interface for the default action that is repeatedly invoked in the mailbox-loop.
 */
@Internal
public interface MailboxDefaultAction {

	/**
	 * This method implements the default action of the mailbox loop (e.g. processing one event from the input).
	 * Implementations should (in general) be non-blocking.
	 *
	 * @param controller controller object for collaborative interaction between the default action and the mailbox loop.
	 * @throws Exception on any problems in the action.
	 */
	void runDefaultAction(Controller controller) throws Exception;

	/**
	 * Represents the suspended state of a {@link MailboxDefaultAction}, ready to resume.
	 */
	@Internal
	interface Suspension {

		/**
		 * Resume execution of the default action.
		 */
		void resume();
	}

	/**
	 * This context is a feedback interface for the default action to interact with the mailbox execution. In particular
	 * it offers ways to signal that the execution of the default action should be finished or temporarily suspended.
	 */
	@Internal
	interface Controller {

		/**
		 * This method must be called to end the stream task when all actions for the tasks have been performed. This
		 * method can be invoked from any thread.
		 * note: 在 stream task 结束时，会调用这个方法
		 */
		void allActionsCompleted();

		/**
		 * Calling this method signals that the mailbox-thread should (temporarily) stop invoking the default action,
		 * e.g. because there is currently no input available. This method must be invoked from the mailbox-thread only!
		 * note: 这个方法表示 mailbox 线程停止触发默认的 action，因为当前没有可用的 input，它只能由 mailbox 线程自己触发
		 */
		Suspension suspendDefaultAction();
	}
}
