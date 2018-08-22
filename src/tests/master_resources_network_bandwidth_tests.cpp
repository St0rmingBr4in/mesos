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

#include <string>

#include "common/resources_utils.hpp"

#include <gmock/gmock.h>

#include <google/protobuf/repeated_field.h>

#include "master/resources/network_bandwidth.hpp"

#include <mesos/resources.hpp>

#include <stout/gtest.hpp>

#include "tests/network_bandwidth_helper.hpp"

namespace mesos {
namespace internal {
namespace tests {

using std::string;

using google::protobuf::RepeatedPtrField;

using mesos::resources::enforceNetworkBandwidthAllocation;

namespace {

void ASSERT_HAS_NETWORK_BANDWIDTH(
  const RepeatedPtrField<Resource>& resources,
  const Resource& expectedNetworkBandwidth) {
  /**
   * To compare resources we need to use Resources which is only supporting
   * resources in post-reservation-refinement format. So we need to convert
   * resources in this format prior to comparison.
   */
  RepeatedPtrField<Resource> adaptedResources(resources);
  convertResourceFormat(
    &adaptedResources,
    ResourceFormat::POST_RESERVATION_REFINEMENT);

  Resource adaptedExpectedNetworkBandwidth(expectedNetworkBandwidth);
  convertResourceFormat(
    &adaptedExpectedNetworkBandwidth,
    ResourceFormat::POST_RESERVATION_REFINEMENT);

  ASSERT_TRUE(
    Resources(adaptedResources).contains(adaptedExpectedNetworkBandwidth))
    << "Network bandwidth should be present.";
}

} // namespace {

// Given a task has declared network bandwidth
// Then enforcement should let the task goes through without update.
TEST(MasterResourcesNetworkBandwidthTest, ConsumeDeclaredNetworkBandwidth) {
  Offer::Operation operation;
  operation.set_type(Offer::Operation::LAUNCH);
  TaskInfo* taskInfo = operation.mutable_launch()->add_task_infos();
  Resources totalSlaveResources;

  totalSlaveResources += resources::CPU(4);

  // Add 1 CPU.
  taskInfo->mutable_resources()->Add()->CopyFrom(
    resources::CPU(1));
  // Add 30Mbps of network bandwidth to the task.
  taskInfo->mutable_resources()->Add()->CopyFrom(
    resources::NetworkBandwidth(30));

  Option<Error> result = enforceNetworkBandwidthAllocation(
    totalSlaveResources, operation);

  ASSERT_NONE(result);
  ASSERT_HAS_NETWORK_BANDWIDTH(
    taskInfo->resources(),
    resources::NetworkBandwidth(30));
}


// Given a task is declaring network bandwidth in a label
// Then the enforcement adds it to the task.
TEST(MasterResourcesNetworkBandwidthTest, ConsumeNetworkBandwidthInLabel) {
  Offer::Operation operation;
  operation.set_type(Offer::Operation::LAUNCH);
  TaskInfo* taskInfo = operation.mutable_launch()->add_task_infos();
  Resources totalSlaveResources;

  // Add 50Mbps of network bandwidth by label.
  Label* label = taskInfo->mutable_labels()->add_labels();
  label->set_key(resources::NETWORK_BANDWIDTH_RESOURCE_LABEL);
  label->set_value("50");

  Option<Error> result = enforceNetworkBandwidthAllocation(
    totalSlaveResources, operation);

  ASSERT_NONE(result);
  ASSERT_HAS_NETWORK_BANDWIDTH(
    taskInfo->resources(),
    resources::NetworkBandwidth(50));
}


// Given a task is declaring network bandwidth in a label with wrong format
// Then the enforcement should fail with an error
TEST(MasterResourcesNetworkBandwidthTest, WrongFormatLabel) {
  Offer::Operation operation;
  operation.set_type(Offer::Operation::LAUNCH);
  TaskInfo* taskInfo = operation.mutable_launch()->add_task_infos();
  Resources totalSlaveResources;

  // Add 50Mbps of network bandwidth by label.
  Label* label = taskInfo->mutable_labels()->add_labels();
  label->set_key(resources::NETWORK_BANDWIDTH_RESOURCE_LABEL);
  label->set_value("a50");

  Option<Error> result = enforceNetworkBandwidthAllocation(
    totalSlaveResources, operation);

  ASSERT_SOME(result);
  ASSERT_EQ("Invalid network bandwidth resource format. "\
            "Should be an integer.", result.get().message);
}


// Given a task is declaring an out of range amount of network bandwidth in a
//       label
// Then the enforcement should fail with an error
TEST(MasterResourcesNetworkBandwidthTest, OutOfRangeLabel) {
  Offer::Operation operation;
  operation.set_type(Offer::Operation::LAUNCH);
  TaskInfo* taskInfo = operation.mutable_launch()->add_task_infos();
  Resources totalSlaveResources;

  // Add 50Mbps of network bandwidth by label.
  Label* label = taskInfo->mutable_labels()->add_labels();
  label->set_key(resources::NETWORK_BANDWIDTH_RESOURCE_LABEL);
  label->set_value("5000000000000000000000000000000000000000000000000000");

  Option<Error> result = enforceNetworkBandwidthAllocation(
    totalSlaveResources, operation);

  ASSERT_SOME(result);
  ASSERT_EQ("Network bandwidth amount is out of range.", result.get().message);
}


// When a task does not declare any network bandwidth and the slave advertised
//      some.
// Then enforcement computes a default value based on share of CPUs and the
//      pool of 2Gbps.
TEST(MasterResourcesNetworkBandwidthTest, AddDefaultNetworkBandwidth) {
  Offer::Operation operation;
  operation.set_type(Offer::Operation::LAUNCH);
  TaskInfo* taskInfo = operation.mutable_launch()->add_task_infos();
  Resources totalSlaveResources;

  // Declare 100Mbps and 4 CPUs on the slave.
  totalSlaveResources += resources::NetworkBandwidth(100);
  totalSlaveResources += resources::CPU(4);

  // Add 1 CPU to the task.
  taskInfo->mutable_resources()->Add()->CopyFrom(resources::CPU(1));

  Option<Error> result = enforceNetworkBandwidthAllocation(
    totalSlaveResources, operation);

  ASSERT_NONE(result);
  ASSERT_HAS_NETWORK_BANDWIDTH(
    taskInfo->resources(),
    resources::NetworkBandwidth(500));
}


// When a task does not declare any network bandwidth and the slave advertised
//      some.
// Then enforcement computes a default value with the correct role.
TEST(MasterResourcesNetworkBandwidthTest,
     ConsumeDefaultNetworkBandwidthCommonResourcesRole) {
  Offer::Operation operation;
  operation.set_type(Offer::Operation::LAUNCH);
  TaskInfo* taskInfo = operation.mutable_launch()->add_task_infos();
  Resources totalSlaveResources;

  // Declare 100Mbps and 4 CPUs on the slave.
  totalSlaveResources += resources::NetworkBandwidth(100);
  totalSlaveResources += resources::CPU(4);

  // Add 1 CPU to the task.
  taskInfo->mutable_resources()->Add()->CopyFrom(resources::CPU(1, "toto"));

  Option<Error> result = enforceNetworkBandwidthAllocation(
    totalSlaveResources, operation);

  ASSERT_NONE(result);
  ASSERT_HAS_NETWORK_BANDWIDTH(
    taskInfo->resources(),
    resources::NetworkBandwidth(500, "toto"));
}


// When a task has no network bandwidth reservation and the slave does not
// declare any either,
// Then the task has a default value taken from 2Gbps pool.
TEST(MasterResourcesNetworkBandwidthTest, SlaveDoesNotDeclareNetworkBandwidth) {
  Offer::Operation operation;
  operation.set_type(Offer::Operation::LAUNCH);
  TaskInfo* taskInfo = operation.mutable_launch()->add_task_infos();
  Resources totalSlaveResources;

  // Declare 4 CPUs but no network bandwidth on the slave.
  totalSlaveResources += resources::CPU(4);

  // Add 1 CPU to the task.
  taskInfo->mutable_resources()->Add()->CopyFrom(resources::CPU(1));

  Option<Error> result = enforceNetworkBandwidthAllocation(
    totalSlaveResources, operation);

  ASSERT_NONE(result);
  ASSERT_HAS_NETWORK_BANDWIDTH(
    taskInfo->resources(),
    resources::NetworkBandwidth(500));
}


// Given a slave does not declare any CPU
// When enforcement mechanism tries to compute network bandwidth based on
//      CPU shares
// Then it raises an error.
TEST(MasterResourcesNetworkBandwidthTest, SlaveHasNoCpu) {
  Offer::Operation operation;
  operation.set_type(Offer::Operation::LAUNCH);
  TaskInfo* taskInfo = operation.mutable_launch()->add_task_infos();
  Resources totalSlaveResources;

  totalSlaveResources += resources::NetworkBandwidth(100);

  // Reserve 1 CPU.
  taskInfo->mutable_resources()->Add()->CopyFrom(resources::CPU(1));

  Option<Error> result = enforceNetworkBandwidthAllocation(
    totalSlaveResources, operation);

  ASSERT_SOME(result);
  ASSERT_EQ("No CPU advertised by the slave. " \
            "Cannot deduce network bandwidth.", result.get().message);
}


// Given a task does not have declared CPU,
// When enforcement mechanism tries to compute network bandwidth based on
//      CPU shares,
// Then it raises an error.
TEST(MasterResourcesNetworkBandwidthTest, TaskHasNoCpu) {
  Offer::Operation operation;
  operation.set_type(Offer::Operation::LAUNCH);
  operation.mutable_launch()->add_task_infos();
  Resources totalSlaveResources;

  totalSlaveResources += resources::CPU(4);
  totalSlaveResources += resources::NetworkBandwidth(100);

  Option<Error> result = enforceNetworkBandwidthAllocation(
    totalSlaveResources, operation);

  ASSERT_SOME(result);
  ASSERT_EQ("No CPU declared in the task. " \
            "Cannot deduce network bandwidth.", result.get().message);
}


// Test case: we are protected from a division by zero when computing the
//   share of CPUs because resources are filtered out when less than 0.0001.
//   (see convertToFixed in src/common/values.cpp)
//
// Given a task declares 0 as amount of CPUs,
// When enforcement mechanism tries to compute network bandwidth based on
//      CPU shares,
// Then it returns 0 for network bandwidth.
TEST(MasterResourcesNetworkBandwidthTest, DivisonByZero) {
  Offer::Operation operation;
  operation.set_type(Offer::Operation::LAUNCH);
  TaskInfo* taskInfo = operation.mutable_launch()->add_task_infos();
  Resources totalSlaveResources;

  // Declare 0.00001 CPUs and 100Mbps of network bandwidth.
  // The CPU is filtered out during the addition.
  totalSlaveResources += resources::CPU(0.00001);
  totalSlaveResources += resources::NetworkBandwidth(100);

  // Add 0 CPU to the task.
  taskInfo->mutable_resources()->Add()->CopyFrom(resources::CPU(1));

  Option<Error> result = enforceNetworkBandwidthAllocation(
    totalSlaveResources, operation);

  ASSERT_SOME(result);
  ASSERT_EQ("No CPU advertised by the slave. "\
            "Cannot deduce network bandwidth.", result.get().message);
}

} // namespace tests {
} // namespace internal {
} // namespace mesos {
