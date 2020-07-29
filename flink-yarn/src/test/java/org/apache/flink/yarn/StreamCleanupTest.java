package org.apache.flink.yarn;

import lombok.SneakyThrows;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class StreamCleanupTest {

	class CleanupCounter {

		boolean[] cleanupBeforeClose;
		boolean[] cleanupAfterClose;
		boolean[] inputThrowed;
		boolean[] outputThrowed;

		public CleanupCounter(int inputParal, int outputParal) {
			cleanupBeforeClose = new boolean[outputParal];
			cleanupAfterClose = new boolean[outputParal];
			inputThrowed = new boolean[inputParal];
			outputThrowed = new boolean[outputParal];
		}

		public void markInputThrowed(int taskId) {
			inputThrowed[taskId] = true;
		}

		public void markOutputThrowed(int taskId) {
			outputThrowed[taskId] = true;
		}

		public void markCorrect(int taskId) {
			cleanupBeforeClose[taskId] = true;
		}

		public void markWrong(int taskId) {
			cleanupAfterClose[taskId] = true;
		}

		public int throwed() {
			return count(inputThrowed) + count(outputThrowed);
		}

		public int count(boolean[] arr) {
			int num = 0;
			for (boolean e : arr) {
				if (e) {
					num += 1;
				}
			}
			return num;
		}

		public int getCorrect() {
			return count(cleanupBeforeClose);
		}

		public int getWrong() {
			return count(cleanupAfterClose);
		}
	}

	public static CleanupCounter counter;

	public enum ExceptionType {
		NoException, CancelException, IOException, RuntimeException, Throwable;

		public void throwException(int idx, boolean input) throws Throwable {
			switch (this) {
				case NoException:
					return;
				case CancelException:
					markThrowed(idx, input);
					throw new CancelTaskException();
				case IOException:
					markThrowed(idx, input);
					throw new IOException();
				case RuntimeException:
					markThrowed(idx, input);
					throw new RuntimeException();
				case Throwable:
					markThrowed(idx, input);
					throw new Throwable();
			}
		}

		public void markThrowed(int idx, boolean input) {
			System.out.println("markThrowed " + this + " of " + idx + ", is input: " + input);
			if (input) {
				counter.markInputThrowed(idx);
			} else {
				counter.markOutputThrowed(idx);
			}
		}
	}

	class TestInputFormat extends RichInputFormat<String, InputSplit> {

		protected ExceptionType exceptionType = ExceptionType.NoException;
		protected int throwExceptionTaskIndex = 0;
		private int count = 0;

		@Override
		public void configure(Configuration configuration) {
		}

		int splitNum = 0; // 总共多少个split
		int num = 0; // 每个split多少条数据
		int index = 0; // 当前迭代到那一条数据
		int throwAt = 0; // 当迭代到第几条数据的时候抛出异常

		public TestInputFormat(int num, int splitNum) {
			this.num = num;
			this.splitNum = splitNum;
			this.index = 0;
		}


		@Override
		public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
			return new BaseStatistics() {
				@Override
				public long getTotalInputSize() {
					return SIZE_UNKNOWN;
				}

				@Override
				public long getNumberOfRecords() {
					return num * splitNum;
				}

				@Override
				public float getAverageRecordWidth() {
					return AVG_RECORD_BYTES_UNKNOWN;
				}
			};
		}

		@Override
		public InputSplit[] createInputSplits(int i) throws IOException {
			return new InputSplit[]{
				new InputSplit() {
					@Override
					public int getSplitNumber() {
						return splitNum;
					}
				}
			};
		}

		@Override
		public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
			return new DefaultInputSplitAssigner(inputSplits);
		}

		@Override
		public void open(InputSplit inputSplit) throws IOException {
			Random rand = new Random(25);
			throwAt = rand.nextInt(num + 1);
		}

		@Override
		public boolean reachedEnd() throws IOException {
			return this.index >= this.num;
		}

		@lombok.SneakyThrows
		@Override
		public String nextRecord(String s) throws IOException {
			if (throwAt == index) {
				if (count++ > 100) {
					int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
					if (throwExceptionTaskIndex < 0 || throwExceptionTaskIndex == taskIdx) {
						exceptionType.throwException(taskIdx, true);
					}
				}
			}
			this.index += 1;
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return "record_" + this.index;
		}

		@SneakyThrows
		@Override
		public void close() throws IOException {
			if (throwAt == num) {
				int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
				if (throwExceptionTaskIndex < 0 || throwExceptionTaskIndex == taskIdx) {
					exceptionType.throwException(taskIdx, true);
				}
			}
		}
	}

	class TestOutputFormat extends RichOutputFormat<String> implements CleanupWhenUnsuccessful {

		protected boolean cleanUped = false;
		protected boolean closed = false;
		protected ExceptionType exceptionType = ExceptionType.NoException;
		protected int throwExceptionTaskIndex = 0;
		protected Random rand;
		private int count = 0;

		@Override
		public void tryCleanupOnError() {
			System.out
				.println("tryCleanupOnError of " + getRuntimeContext().getIndexOfThisSubtask());
			this.cleanUped = true;
			if (!this.closed) {
				counter.markCorrect(getRuntimeContext().getIndexOfThisSubtask());
			} else {
				System.out.println("sada exception......");
				counter.markWrong(getRuntimeContext().getIndexOfThisSubtask());
			}
		}

		@Override
		public void configure(Configuration configuration) {

		}

		@Override
		public void open(int i, int i1) throws IOException {
			rand = new Random();
		}

		@lombok.SneakyThrows
		@Override
		public void writeRecord(String s) throws IOException {
			if (count++ > 100) {
				int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
				if (throwExceptionTaskIndex < 0 || throwExceptionTaskIndex == taskIdx) {
					exceptionType.throwException(taskIdx, false);
				}
			} else {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		@SneakyThrows
		@Override
		public void close() throws IOException {
			System.out.println("output close of " + getRuntimeContext().getIndexOfThisSubtask());
			if (rand.nextBoolean()) {
				int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
				if (throwExceptionTaskIndex < 0 || throwExceptionTaskIndex == getRuntimeContext()
					.getIndexOfThisSubtask()) {
					exceptionType.throwException(taskIdx, false);
				}
			}
			this.closed = true;
		}
	}

	@Test
	public void testException() throws Exception {
		StreamExecutionEnvironment environment = StreamExecutionEnvironment
			.getExecutionEnvironment();
		environment.getConfig().enableObjectReuse();
		Random rand = new Random();
		for (int inputParal = 1; inputParal <= 2; inputParal++) {
			for (int outputParal = 1; outputParal <= 10; outputParal++) {
				//for (int j = 0; j < 10; j++) {
				counter = new CleanupCounter(outputParal, inputParal);
				TestInputFormat inputFormat = new TestInputFormat(100, inputParal);
				TestOutputFormat outputFormat = new TestOutputFormat();

				int random = rand.nextInt(ExceptionType.values().length);
				inputFormat.exceptionType = ExceptionType.values()[random == 0 ? 1 : random];
				outputFormat.exceptionType = ExceptionType.values()[random == 0 ? 1 : random];
				System.out.println(
					"\n\ninput paral: " + inputParal + ", output paral: " + outputParal
						+ ", inputException: " + inputFormat.exceptionType
						+ ", outputException: " + outputFormat.exceptionType);

				DataStream<String> source = environment.createInput(inputFormat)
					.setParallelism(inputParal);
				source.writeUsingOutputFormat(outputFormat).setParallelism(outputParal);
				Exception exception = null;
				try {
					JobExecutionResult result = environment.execute();
				} catch (Exception e) {
					exception = e;
				}

				System.out.println(
					"counter is " + counter.throwed() + ", Wrong: " + counter.getWrong()
						+ ", Correct: " + counter.getCorrect());
				if (counter.throwed() == 0) {
					Assert.assertNull(exception);
					Assert.assertEquals(0, counter.getWrong());
					Assert.assertEquals(0, counter.getCorrect());
				} else {
					Assert.assertNotNull(exception);
					Assert.assertEquals(0, counter.getWrong());
//						Assert
//							.assertTrue("correct counter: " + counter.getCorrect() + " should > 0",
//								counter.getCorrect() > 0);
				}
				System.out.println("test end....");
				//}
			}
		}
		System.exit(0);
	}
}

