package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.examples.statemachine.event.Event;
import org.apache.flink.streaming.examples.statemachine.kafka.EventDeSerializer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author matt
 * @date 2019-04-13 17:16
 */
public class WordCountFromKafka {
	private final static Logger LOG = LoggerFactory.getLogger(WordCountFromKafka.class);

	public static void main(String[] args) throws Exception {
		System.out.println("Usage with Kafka: StateMachineExample --kafka-topic <topic> --brokers <brokers>");
		System.out.println();

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
		// ---- determine whether to use the built-in source, or read from Kafka ----

		final SourceFunction<Event> source;

		// set up the Kafka reader
		String kafkaTopic = "matt_test1";
		String brokers = "localhost:9092";

		System.out.printf("Reading from kafka topic %s @ %s\n", kafkaTopic, brokers);
		System.out.println();

		Properties kafkaProps = new Properties();
		kafkaProps.setProperty("bootstrap.servers", brokers);

		FlinkKafkaConsumer010<Event> kafka = new FlinkKafkaConsumer010<>(kafkaTopic, new EventDeSerializer(),
			kafkaProps);
		kafka.setStartFromLatest();
		kafka.setCommitOffsetsOnCheckpoints(false);
		source = kafka;

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(60000L);
		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);
		DataStream<Event> events = env.addSource(source);

		DataStream<Tuple2<String, Integer>> counts =
			// split up the lines in pairs (2-tuples) containing: (word,1)
			events.flatMap(new Tokenizer()).keyBy(0).sum(1);

		// output the alerts to std-out
		final String outputFile = params.get("output");
		if (outputFile == null) {
			counts.print();
		} else {
			counts
				.writeAsText(outputFile, FileSystem.WriteMode.NO_OVERWRITE)
				.setParallelism(1);
		}

		// execute program
		env.execute("Streaming WordCount");
	}

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined FlatMapFunction. The function
	 * takes a line (String) and splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static final class Tokenizer implements FlatMapFunction<Event, Tuple2<String, Integer>> {

		@Override
		public void flatMap(Event event, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			out.collect(new Tuple2<>(event.toString(), 1));
		}
	}
}
