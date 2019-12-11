package org.apache.flink.streaming.examples.matt;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @author matt
 * @date 2019-10-07 15:09
 */
public class RandomWordCount {
	public static void main(String[] args) throws Exception {
		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//note 模拟两个数据源，它们会生成一行随机单词组（单词之间是空格分隔）
		DataStream<String> inputStream = env.addSource(new RandomWordCount.RandomStringSource());
		DataStream<String> inputStream2 = env.addSource(new RandomWordCount.RandomStringSource());

		//note: 先对流做 union，然后做一个过滤后，做 word-count
		inputStream.union(inputStream2)
			.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
				@Override
				public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
					for (String word : value.split("\\s")) {
						out.collect(Tuple2.of(word, 1));
					}
				}
			})
			.shuffle()
			.filter(new FilterFunction<Tuple2<String, Integer>>() {
				@Override
				public boolean filter(Tuple2<String, Integer> value) throws Exception {
					if (value.f0.startsWith("a")) {
						return true;
					} else {
						return false;
					}
				}
			}).keyBy(0).sum(1)
			.print()
			.setParallelism(2);

		System.out.println("StreamGraph:\t" + env.getStreamGraph().getStreamingPlanAsJSON());
		System.out.println("JobGraph:\t" + env.getStreamGraph().getJobGraph().toString());
		System.out.println("ExecutionGraph:\t" + env.getExecutionPlan());
		env.execute("Random WordCount");
	}

	/**
	 * Generate BOUND world line
	 */
	private static class RandomStringSource implements SourceFunction<String> {
		private static final long serialVersionUID = 1L;

		private Random rnd = new Random();

		private volatile boolean isRunning = true;
		public static final int BOUND = 100;
		private int counter = 0;

		@Override
		public void run(SourceContext<String> ctx) throws Exception {

			while (isRunning && counter < BOUND) {
				int first = rnd.nextInt(BOUND / 2 - 1) + 1;
				int second = rnd.nextInt(BOUND / 2 - 1) + 1;
				counter++;

				ctx.collect(generatorRandomWorldLine(first, second));
				Thread.sleep(5000L);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		private String generatorRandomWorldLine(int charMaxLimit, int wordMaxLimit) {
			int leftLimit = 97; // letter 'a'
			int rightLimit = 122; // letter 'z'
			StringBuilder stringBuilder = null;
			// 本行单词最多有 wordMaxLimit 个
			for (int i = 0; i < wordMaxLimit; i++) {
				// 这个单词的大小长度
				int targetStringLength = rnd.nextInt(charMaxLimit) + 1;
				StringBuilder buffer = new StringBuilder(targetStringLength);
				for (int j = 0; j < targetStringLength; j++) {
					int randomLimitedInt = leftLimit + (int)
						(rnd.nextFloat() * (rightLimit - leftLimit + 1));
					buffer.append((char) randomLimitedInt);
				}
				String generatedString = buffer.toString();
				if (stringBuilder == null) {
					stringBuilder = new StringBuilder(generatedString);
				} else {
					stringBuilder.append(" " + generatedString);
				}
			}

			return stringBuilder.toString();
		}
	}
}
