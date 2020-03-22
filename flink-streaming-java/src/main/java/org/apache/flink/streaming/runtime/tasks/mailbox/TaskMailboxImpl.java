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

package org.apache.flink.streaming.runtime.tasks.mailbox;

import org.apache.flink.annotation.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.State.CLOSED;
import static org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.State.OPEN;
import static org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.State.QUIESCED;

/**
 * Implementation of {@link TaskMailbox} in a {@link java.util.concurrent.BlockingQueue} fashion and tailored towards
 * our use case with multiple writers and single reader.
 */
@ThreadSafe
public class TaskMailboxImpl implements TaskMailbox {
	/**
	 * Lock for all concurrent ops.
	 */
	private final ReentrantLock lock = new ReentrantLock();

	/**
	 * Internal queue of mails.
	 */
	@GuardedBy("lock")
	private final Deque<Mail> queue = new ArrayDeque<>();

	/**
	 * Condition that is triggered when the mailbox is no longer empty.
	 */
	@GuardedBy("lock")
	private final Condition notEmpty = lock.newCondition();

	/**
	 * The state of the mailbox in the lifecycle of open, quiesced, and closed.
	 */
	@GuardedBy("lock")
	private State state = OPEN;

	/**
	 * Reference to the thread that executes the mailbox mails.
	 */
	@Nonnull
	private final Thread taskMailboxThread;

	/**
	 * The current batch of mails. A new batch can be created with {@link #createBatch()} and consumed with {@link
	 * #tryTakeFromBatch()}.
	 */
	private final Deque<Mail> batch = new ArrayDeque<>();

	/**
	 * Performance optimization where hasNewMail == !queue.isEmpty(). Will not reflect the state of {@link #batch}.
	 * note: 是否有新的 mail，根据 queue 判断
	 */
	private volatile boolean hasNewMail = false;

	public TaskMailboxImpl(@Nonnull final Thread taskMailboxThread) {
		this.taskMailboxThread = taskMailboxThread;
	}

	@VisibleForTesting
	public TaskMailboxImpl() {
		this(Thread.currentThread());
	}

	//note: 验证当前执行的线程是否是 mail box 线程
	@Override
	public boolean isMailboxThread() {
		return Thread.currentThread() == taskMailboxThread;
	}

	@Override
	public boolean hasMail() {
		checkIsMailboxThread();
		return !batch.isEmpty() || hasNewMail;
	}

	//note: 从 batch 或 queue 中取一个优先级比 priority 高的 mail
	@Override
	public Optional<Mail> tryTake(int priority) {
		//note: 从 batch 里取
		checkIsMailboxThread(); //note: 检查线程
		checkTakeStateConditions(); //note: 检查 Mail 线程的状态
		Mail head = takeOrNull(batch, priority); //note: 从 batch 中选择优先级比 priority 高的 mail
		if (head != null) {
			return Optional.of(head);
		}
		if (!hasNewMail) {
			return Optional.empty();
		}
		//note: 从 queue 里取，需要加锁
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			final Mail value = takeOrNull(queue, priority);
			if (value == null) {
				return Optional.empty();
			}
			hasNewMail = !queue.isEmpty();
			return Optional.ofNullable(value);
		} finally {
			lock.unlock();
		}
	}

	//note: 从 batch 或 queue 中取一个优先级比 priority 高的 mail（如果取不到，这里会等待，直到从 queue 中取到合适的 mail 为止）
	@Override
	public @Nonnull Mail take(int priority) throws InterruptedException, IllegalStateException {
		checkIsMailboxThread();
		checkTakeStateConditions();
		Mail head = takeOrNull(batch, priority);
		if (head != null) {
			return head;
		}
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			Mail headMail;
			while ((headMail = takeOrNull(queue, priority)) == null) {
				notEmpty.await();
			}
			hasNewMail = !queue.isEmpty();
			return headMail;
		} finally {
			lock.unlock();
		}
	}

	//------------------------------------------------------------------------------------------------------------------

	//note: 在 batch 队列中创建一个 mail（实际上是从 queue 中取出放入 batch 队列中的）
	@Override
	public boolean createBatch() {
		checkIsMailboxThread();
		if (!hasNewMail) {
			// batch is usually depleted by previous MailboxProcessor#runMainLoop
			// however, putFirst may add a message directly to the batch if called from mailbox thread
			return !batch.isEmpty();
		}
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			//note: 从 queue 中取出一个 mail 放入 batch 中
			Mail mail;
			while ((mail = queue.pollFirst()) != null) {
				batch.addLast(mail);
			}
			hasNewMail = false;
			return !batch.isEmpty();
		} finally {
			lock.unlock();
		}
	}

	//note: 从 batch 队列中取出一个 mail
	@Override
	public Optional<Mail> tryTakeFromBatch() {
		checkIsMailboxThread();
		checkTakeStateConditions();
		return Optional.ofNullable(batch.pollFirst());
	}

	//------------------------------------------------------------------------------------------------------------------

	//note: 添加到队列中
	@Override
	public void put(@Nonnull Mail mail) {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			checkPutStateConditions();
			queue.addLast(mail);
			hasNewMail = true;
			notEmpty.signal();
		} finally {
			lock.unlock();
		}
	}

	//note: 添加到队列头部，会优先读取
	@Override
	public void putFirst(@Nonnull Mail mail) {
		if (isMailboxThread()) {
			checkPutStateConditions();
			batch.addFirst(mail);
		} else {
			final ReentrantLock lock = this.lock;
			lock.lock();
			try {
				checkPutStateConditions();
				queue.addFirst(mail);
				hasNewMail = true;
				notEmpty.signal();
			} finally {
				lock.unlock();
			}
		}
	}

	//------------------------------------------------------------------------------------------------------------------

	//note: 选择某个优先级更高的 Mail
	@Nullable
	private Mail takeOrNull(Deque<Mail> queue, int priority) {
		if (queue.isEmpty()) {
			return null;
		}

		Iterator<Mail> iterator = queue.iterator();
		while (iterator.hasNext()) {
			Mail mail = iterator.next();
			if (mail.getPriority() >= priority) {
				iterator.remove();
				return mail;
			}
		}
		return null;
	}

	//note: 清除 batch 和 queue 中缓存的 mail
	@Override
	public List<Mail> drain() {
		List<Mail> drainedMails = new ArrayList<>(batch);
		batch.clear();
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			drainedMails.addAll(queue);
			queue.clear();
			hasNewMail = false;
			return drainedMails;
		} finally {
			lock.unlock();
		}
	}

	private void checkIsMailboxThread() {
		if (!isMailboxThread()) {
			throw new IllegalStateException(
				"Illegal thread detected. This method must be called from inside the mailbox thread!");
		}
	}

	private void checkPutStateConditions() {
		if (state != OPEN) {
			throw new IllegalStateException("Mailbox is in state " + state + ", but is required to be in state " +
				OPEN + " for put operations.");
		}
	}

	//note: 检查状态，如果是 closed 状态，将不能读取 mail，这里将会抛出异常
	private void checkTakeStateConditions() {
		if (state == CLOSED) {
			throw new IllegalStateException("Mailbox is in state " + state + ", but is required to be in state " +
				OPEN + " or " + QUIESCED + " for take operations.");
		}
	}

	//note: 将 TaskMail 的状态从 OPEN 转化为 QUIESCED
	@Override
	public void quiesce() {
		checkIsMailboxThread();
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			if (state == OPEN) {
				state = QUIESCED;
			}
		} finally {
			this.lock.unlock();
		}
	}

	@Nonnull
	@Override
	public List<Mail> close() {
		checkIsMailboxThread();
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			if (state == CLOSED) {
				return Collections.emptyList();
			}
			List<Mail> droppedMails = drain();
			state = CLOSED;
			// to unblock all
			notEmpty.signalAll();
			return droppedMails;
		} finally {
			lock.unlock();
		}
	}

	@Nonnull
	@Override
	public State getState() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			return state;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void runExclusively(Runnable runnable) {
		lock.lock();
		try {
			runnable.run();
		} finally {
			lock.unlock();
		}
	}
}
