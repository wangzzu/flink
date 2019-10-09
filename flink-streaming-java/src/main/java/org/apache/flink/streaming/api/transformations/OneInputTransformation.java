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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * This Transformation represents the application of a
 * {@link org.apache.flink.streaming.api.operators.OneInputStreamOperator} to one input
 * {@link Transformation}.
 * note：只有一个输入的 Transformation 的具体实现
 *
 * @param <IN> The type of the elements in the input {@code Transformation}
 * @param <OUT> The type of the elements that result from this {@code OneInputTransformation}
 */
@Internal
public class OneInputTransformation<IN, OUT> extends PhysicalTransformation<OUT> {

	private final Transformation<IN> input;

	private final StreamOperatorFactory<OUT> operatorFactory;

	private KeySelector<IN, ?> stateKeySelector;

	private TypeInformation<?> stateKeyType;

	/**
	 * Creates a new {@code OneInputTransformation} from the given input and operator.
	 *
	 * @param input The input {@code Transformation}
	 * @param name The name of the {@code Transformation}, this will be shown in Visualizations and the Log
	 * @param operator The {@code TwoInputStreamOperator}
	 * @param outputType The type of the elements produced by this {@code OneInputTransformation}
	 * @param parallelism The parallelism of this {@code OneInputTransformation}
	 */
	public OneInputTransformation(
			Transformation<IN> input,
			String name,
			OneInputStreamOperator<IN, OUT> operator,
			TypeInformation<OUT> outputType,
			int parallelism) {
		this(input, name, SimpleOperatorFactory.of(operator), outputType, parallelism);
	}

	public OneInputTransformation(
			Transformation<IN> input,
			String name,
			StreamOperatorFactory<OUT> operatorFactory,
			TypeInformation<OUT> outputType,
			int parallelism) {
		super(name, outputType, parallelism);
		this.input = input;
		this.operatorFactory = operatorFactory;
	}

	/**
	 * Returns the input {@code Transformation} of this {@code OneInputTransformation}.
	 */
	public Transformation<IN> getInput() {
		return input;
	}

	/**
	 * Returns the {@code TypeInformation} for the elements of the input.
	 */
	public TypeInformation<IN> getInputType() {
		return input.getOutputType();
	}

	@VisibleForTesting
	public OneInputStreamOperator<IN, OUT> getOperator() {
		return (OneInputStreamOperator<IN, OUT>) ((SimpleOperatorFactory) operatorFactory).getOperator();
	}

	/**
	 * Returns the {@code StreamOperatorFactory} of this Transformation.
	 */
	public StreamOperatorFactory<OUT> getOperatorFactory() {
		return operatorFactory;
	}

	/**
	 * Sets the {@link KeySelector} that must be used for partitioning keyed state of this operation.
	 *
	 * @param stateKeySelector The {@code KeySelector} to set
	 */
	public void setStateKeySelector(KeySelector<IN, ?> stateKeySelector) {
		this.stateKeySelector = stateKeySelector;
	}

	/**
	 * Returns the {@code KeySelector} that must be used for partitioning keyed state in this
	 * Operation.
	 *
	 * @see #setStateKeySelector
	 */
	public KeySelector<IN, ?> getStateKeySelector() {
		return stateKeySelector;
	}

	public void setStateKeyType(TypeInformation<?> stateKeyType) {
		this.stateKeyType = stateKeyType;
	}

	public TypeInformation<?> getStateKeyType() {
		return stateKeyType;
	}

	@Override
	public Collection<Transformation<?>> getTransitivePredecessors() {
		List<Transformation<?>> result = Lists.newArrayList();
		result.add(this);
		result.addAll(input.getTransitivePredecessors());
		return result;
	}

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		operatorFactory.setChainingStrategy(strategy);
	}
}
