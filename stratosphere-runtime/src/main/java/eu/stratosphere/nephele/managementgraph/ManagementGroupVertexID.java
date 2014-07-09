/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.managementgraph;

import javax.xml.bind.DatatypeConverter;

import eu.stratosphere.nephele.AbstractID;

/**
 * A management group vertex ID uniquely identifies a {@link ManagementGroupVertex}.
 * <p>
 * This class is not thread-safe.
 * 
 */
public final class ManagementGroupVertexID extends AbstractID {
	
	
	/**
	 * Constructs a new ManagementGroupVertexID
	 * 
	 */
	public ManagementGroupVertexID() {
		super();
	}
	
	/**
	 * Constructs a new ManagementGroupVertexID from the given bytes.
	 * 
	 * @param bytes
	 *        the bytes to initialize the job ID with
	 */
	public ManagementGroupVertexID(final byte[] bytes) {
		super(bytes);
	}
	
	/**
	 * Constructs a new management group vertex ID and initializes it with
	 * the given hex string.
	 * 
	 * @param hexString
	 *        the hex string to initialize the management group vertex ID
	 * @return the new management group vertex ID
	 */
	public static ManagementGroupVertexID fromHexString(final String hexString) {

		return new ManagementGroupVertexID(DatatypeConverter.parseHexBinary(hexString));
	}
	
}
