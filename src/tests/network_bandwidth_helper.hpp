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
// limitations under the License

#ifndef __TESTS_NETWORK_BANDWIDTH_HELPER_HPP__
#define __TESTS_NETWORK_BANDWIDTH_HELPER_HPP__

#include <string>

#include <mesos/resources.hpp>

namespace mesos {
namespace internal {
namespace tests {
namespace resources {

const std::string NETWORK_BANDWIDTH_RESOURCE_LABEL =
    "NETWORK_BANDWIDTH_RESOURCE";
const std::string NETWORK_BANDWIDTH_RESOURCE_NAME = "network_bandwidth";
const std::string CPUS_RESOURCE_NAME = "cpus";

mesos::Resource CPU(double amount, const std::string& role = "*");
mesos::Resource NetworkBandwidth(double amount, const std::string& role = "*");
mesos::Resource Memory(double amount, const std::string& role = "*");

} // namespace resources {
} // namespace tests {
} // namespace internal {
} // namespace mesos {

#endif
