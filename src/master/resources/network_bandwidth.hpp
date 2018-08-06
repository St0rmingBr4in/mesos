// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef __MASTER_RESOURCES_NETWORK_BANDWIDTH_HPP__
#define __MASTER_RESOURCES_NETWORK_BANDWIDTH_HPP__

#include <mesos/mesos.hpp>
#include <mesos/resources.hpp>

#include <stout/nothing.hpp>
#include <stout/try.hpp>

namespace mesos {
namespace resources {

/**
 * @brief Enforce network bandwidth reservation for a given task.
 *
 * We ensure every task has a default allocated network bandwidth on slaves
 * declaring network bandwidth.
 * The amount of allocated network bandwidth is either provided by the
 * scheduler via resources or labels. Otherwise it is computed and added to the
 * task. The computation is the following:
 *
 * TaskNetworkBandwidth = TaskCpus / SlaveCpus * SlaveNetworkBandwidth.
 *
 * This computation is definitely Criteo specific and could be anything else.
 *
 * Note: this amount of network bandwidth is taken out from unreserved
 *       resources since we don't take roles into account yet.
 *
 * @param slaveTotalResources The resources declared on the slave.
 * @param task The task to enforce network bandwidth declaration for.
 * @return Nothing if enforcement is not applied or successful otherwise an
 *         Error.
 */
Try<Nothing> enforceNetworkBandwidthAllocation(
  const Resources& slaveTotalResources,
  TaskInfo& task);

} // namespace resources {
} // namespace mesos {

#endif // __MASTER_METRICS_HPP__
