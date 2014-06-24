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

package eu.stratosphere.nephele.protocols;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import eu.stratosphere.core.protocols.VersionedProtocol;
import eu.stratosphere.nephele.deployment.TaskDeploymentDescriptor;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.taskmanager.TaskKillResult;
import eu.stratosphere.runtime.io.channels.ChannelID;
import eu.stratosphere.nephele.taskmanager.TaskCancelResult;
import eu.stratosphere.nephele.taskmanager.TaskSubmissionResult;

/**
 * The task submission protocol is implemented by the task manager and allows the job manager
 * to submit and cancel tasks, as well as to query the task manager for cached libraries and submit
 * these if necessary.
 * 
 */
public interface TaskOperationProtocol extends VersionedProtocol {

	/**
	 * Submits a list of tasks to the task manager.
	 * 
	 * @param tasks
	 *        the tasks to be submitted
	 * @return the result of the task submission
	 * @throws IOException
	 *         thrown if an error occurs during this remote procedure call
	 */
	List<TaskSubmissionResult> submitTasks(List<TaskDeploymentDescriptor> tasks) throws IOException;

	/**
	 * Advises the task manager to cancel the task with the given ID.
	 * 
	 * @param id
	 *        the ID of the task to cancel
	 * @return the result of the task cancel attempt
	 * @throws IOException
	 *         thrown if an error occurs during this remote procedure call
	 */
	TaskCancelResult cancelTask(ExecutionVertexID id) throws IOException;

	/**
	 * Advises the task manager to kill the task with the given ID.
	 *
	 * @param id
	 *        the ID of the task to kill
	 * @return the result of the task kill attempt
	 * @throws IOException
	 *         thrown if an error occurs during this remote procedure call
	 */
	TaskKillResult killTask(ExecutionVertexID id) throws IOException;

	/**
	 * Invalidates the entries identified by the given channel IDs from the task manager's receiver lookup cache.
	 * 
	 * @param channelIDs
	 *        the channel IDs identifying the cache entries to invalidate
	 * @throws IOException
	 *         thrown if an error occurs during this remote procedure call
	 */
	void invalidateLookupCacheEntries(Set<ChannelID> channelIDs) throws IOException;

	/**
	 * Triggers the task manager write the current utilization of its read and write buffers to its logs.
	 * This method is primarily for debugging purposes.
	 * 
	 * @throws IOException
	 *         thrown if an error occurs while transmitting the request
	 */
	void logBufferUtilization();

	/**
	 * Kills the task manager. This method is mainly intended to test and debug Nephele's fault tolerance mechanisms.
	 * 
	 * @throws IOException
	 *         thrown if an error occurs during this remote procedure call
	 */
	void killTaskManager() throws IOException;
}
