/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Apache Flink project (http://flink.incubator.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package org.apache.flink.runtime.io.network.api;

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;

/**
 * 
 */
public interface MutableReader<T extends IOReadableWritable> extends ReaderBase {

	/**
	 * @param target
	 *            the target object to store the next element in
	 * @return <code>true</code> if the next element has been stored in target,
	 *         <code>false</code> if there are no more elements
	 * @throws IOException
	 * @throws InterruptedException
	 */
	boolean next(T target) throws IOException, InterruptedException;
}
