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

package org.apache.flink.yarn;

import java.io.IOException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;


public class CleanupTest {

	static boolean cleanUpBeforeClose = false;

	class TestInputFormat implements InputFormat<String, InputSplit> {

		protected int throwCancel = 0; // 0 不抛出异常， 1 抛出cancel exception， 2 抛出普通exception
		@Override
		public void configure(Configuration configuration) {

		}

		int num = 0;
		int index = 0;

		public TestInputFormat(int num) {
			this.num = num;
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
					return 1;
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
				new FileInputSplit(0,  new Path("sadfa"), 0, -1, "sada".split("s"))
			};
		}

		@Override
		public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
			return new DefaultInputSplitAssigner(inputSplits);
		}

		@Override
		public void open(InputSplit inputSplit) throws IOException {

		}

		@Override
		public boolean reachedEnd() throws IOException {
			return this.index >= this.num;
		}

		@Override
		public String nextRecord(String s) throws IOException {
			switch(throwCancel) {
				case 0:
					break;
				case 1:
					throw new CancelTaskException();
				case 2:
					throw new RuntimeException();
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			this.index += 1;
			return "record";
		}

		@Override
		public void close() throws IOException {

		}
	}

	class TestOutputFormat implements OutputFormat<String>, CleanupWhenUnsuccessful {
		protected boolean cleanUped = false;
		protected boolean closed = false;
		protected int throwCancel = 0; // 0 不抛出异常， 1 抛出cancel exception， 2 抛出普通exception

		@Override
		public void tryCleanupOnError() {
			System.out.println("tryCleanupOnError");
			if (this.closed) {
				cleanUpBeforeClose = false;
			} else {
				cleanUpBeforeClose = true;
			}
			this.cleanUped = true;
		}

		@Override
		public void configure(Configuration configuration) {

		}

		@Override
		public void open(int i, int i1) throws IOException {

		}

		@Override
		public void writeRecord(String s) throws IOException {
			switch(throwCancel) {
				case 0:
					break;
				case 1:
					throw new CancelTaskException();
				case 2:
					throw new RuntimeException();
			}
		}

		@Override
		public void close() throws IOException {
			System.out.println("close");
			this.closed = true;
		}
	}

	@Test
	public void testNoException() throws Exception {

		TestInputFormat inputFormat = new TestInputFormat(10);
		TestOutputFormat outputFormat = new TestOutputFormat();

		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		environment.getConfig().enableObjectReuse();
		DataStream<String> source = environment.createInput(inputFormat).setParallelism(1);
		source.writeUsingOutputFormat(outputFormat).setParallelism(1);
		JobExecutionResult result = environment.execute();
		Assert.assertEquals(false, cleanUpBeforeClose);
	}

	@Test
	public void testCancelException() throws Exception {

		TestInputFormat inputFormat = new TestInputFormat(10);
		TestOutputFormat outputFormat = new TestOutputFormat();
		outputFormat.throwCancel = 1;
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		environment.getConfig().enableObjectReuse();
		DataStream<String> source = environment.createInput(inputFormat).setParallelism(1);
		source.writeUsingOutputFormat(outputFormat).setParallelism(1);
		try {
			JobExecutionResult result = environment.execute();
		} catch (Exception e) {

		}
		Assert.assertEquals(true, cleanUpBeforeClose);
	}

	@Test
	public void testNormalException() throws Exception {
		TestInputFormat inputFormat = new TestInputFormat(10);
		TestOutputFormat outputFormat = new TestOutputFormat();
		outputFormat.throwCancel = 2;
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		environment.getConfig().enableObjectReuse();
		DataStream<String> source = environment.createInput(inputFormat).setParallelism(1);
		source.writeUsingOutputFormat(outputFormat).setParallelism(1);
		try {
			JobExecutionResult result = environment.execute();
		} catch (Exception e) {

		}
		Assert.assertEquals(true, cleanUpBeforeClose);
	}


	@Test
	@Ignore
	public void testInputCancelException() throws Exception {

		TestInputFormat inputFormat = new TestInputFormat(10);
		TestOutputFormat outputFormat = new TestOutputFormat();
		inputFormat.throwCancel = 1;
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		environment.getConfig().enableObjectReuse();
		DataStream<String> source = environment.createInput(inputFormat).setParallelism(1);
		source.writeUsingOutputFormat(outputFormat).setParallelism(1);
		try {
			JobExecutionResult result = environment.execute();
		} catch (Exception e) {

		}
		Assert.assertEquals(true, cleanUpBeforeClose);
	}

	@Test
	public void testInputNormalException() throws Exception {
		TestInputFormat inputFormat = new TestInputFormat(10);
		TestOutputFormat outputFormat = new TestOutputFormat();
		inputFormat.throwCancel = 2;
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		environment.getConfig().enableObjectReuse();
		DataStream<String> source = environment.createInput(inputFormat).setParallelism(1);
		source.writeUsingOutputFormat(outputFormat).setParallelism(1);
		try {
			JobExecutionResult result = environment.execute();
		} catch (Exception e) {

		}
		Assert.assertEquals(true, cleanUpBeforeClose);
	}


}
