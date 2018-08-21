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
 * TaskNetworkBandwidth = TaskCpus / SlaveCpus * 2000.
 *
 * This computation is definitely Criteo specific and could be anything else.
 * 2000 is equivalent to 2Gbps which is the lowest common amount of network
 * bandwidth available on each agent in the entire Criteo infrastructure.
 * We set a constant here, for 2 reasons: backward compatibility and ensure
 * two instances of the same app have the same limit.
 *
 * TODO(clems4ever): make this value customizable by flag.
 *
 * Note: this amount of network bandwidth is taken out from unreserved
 *       resources since we don't take roles into account yet.
 *
 * @param slaveTotalResources The resources declared on the slave.
 * @param operation The operation for which to enforce network bandwidth
 *   reservation for.
 * @return None if enforcement is not applied or successful otherwise an
 *         Error.
 */
Option<Error> enforceNetworkBandwidthAllocation(
  const Resources& slaveTotalResources,
  Offer::Operation& operation);

} // namespace resources {
} // namespace mesos {

#endif // __MASTER_METRICS_HPP__
