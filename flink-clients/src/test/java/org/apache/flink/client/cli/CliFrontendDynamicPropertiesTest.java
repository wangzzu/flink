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

package org.apache.flink.client.cli;

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders.ParentFirstClassLoader;
import org.apache.flink.util.ChildFirstClassLoader;

import org.apache.commons.cli.Options;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collections;

import static org.apache.flink.client.cli.CliFrontendTestUtils.TEST_JAR_MAIN_CLASS;
import static org.apache.flink.client.cli.CliFrontendTestUtils.getTestJarPath;
import static org.junit.Assert.assertEquals;


/**
 * Tests for the RUN command with Dynamic Properties.
 */
public class CliFrontendDynamicPropertiesTest extends CliFrontendTestBase {

	private Options testOptions;
	private GenericCLI cliUnderTest;
	private Configuration configuration;

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
	}

	@AfterClass
	public static void shutdown() {
		CliFrontendTestUtils.restoreSystemOut();
	}

	@Before
	public void setup() {
		testOptions = new Options();
		configuration = new Configuration();

		cliUnderTest = new GenericCLI(
			configuration,
			tmp.getRoot().getAbsolutePath());

		cliUnderTest.addGeneralOptions(testOptions);
	}

	@Test
	public void testDynamicPropertiesWithParentFirstClassloader() throws Exception {

		String[] args = {
			"-e", "test-executor",
			"-D" + CoreOptions.DEFAULT_PARALLELISM.key() + "=5",
			"-D" + "classloader.resolve-order=parent-first",
			getTestJarPath(), "-a", "--debug", "true", "arg1", "arg2"
		};

		verifyCliFrontend(configuration, args, cliUnderTest,
			ParentFirstClassLoader.class.getName());
	}

	@Test
	public void testDynamicPropertiesWithDefaultChildFirstClassloader() throws Exception {

		String[] args = {
			"-e", "test-executor",
			"-D" + CoreOptions.DEFAULT_PARALLELISM.key() + "=5",
			getTestJarPath(), "-a", "--debug", "true", "arg1", "arg2"
		};

		verifyCliFrontend(configuration, args, cliUnderTest, ChildFirstClassLoader.class.getName());
	}

	@Test
	public void testDynamicPropertiesWithChildFirstClassloader() throws Exception {

		String[] args = {
			"-e", "test-executor",
			"-D" + CoreOptions.DEFAULT_PARALLELISM.key() + "=5",
			"-D" + "classloader.resolve-order=child-first",
			getTestJarPath(), "-a", "--debug", "true", "arg1", "arg2"
		};

		verifyCliFrontend(configuration, args, cliUnderTest, ChildFirstClassLoader.class.getName());
	}

	// --------------------------------------------------------------------------------------------

	public static void verifyCliFrontend(
		Configuration configuration,
		String[] parameters,
		GenericCLI cliUnderTest,
		String userCodeClassLoaderClassName) throws Exception {
		TestingCliFrontend testFrontend =
			new TestingCliFrontend(configuration, cliUnderTest, userCodeClassLoaderClassName);
		testFrontend.run(parameters); // verifies the expected values (see below)
	}

	private static final class TestingCliFrontend extends CliFrontend {

		private final String userCodeClassLoaderClassName;

		private TestingCliFrontend(
			Configuration configuration,
			GenericCLI cliUnderTest,
			String userCodeClassLoaderClassName) {
			super(
				configuration,
				Collections.singletonList(cliUnderTest));
			this.userCodeClassLoaderClassName = userCodeClassLoaderClassName;
		}

		@Override
		protected void executeProgram(Configuration configuration, PackagedProgram program) {
			assertEquals(TEST_JAR_MAIN_CLASS, program.getMainClassName());
			assertEquals(userCodeClassLoaderClassName,
				program.getUserCodeClassLoader().getClass().getName());
		}
	}
}
