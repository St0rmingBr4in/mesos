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
#include <vector>

#include <gmock/gmock.h>

#include "master/resources/network_bandwidth.hpp"

#include <mesos/executor.hpp>
#include <mesos/scheduler.hpp>

#include <mesos/allocator/allocator.hpp>

#include <mesos/scheduler/scheduler.hpp>

#include <process/clock.hpp>
#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/owned.hpp>

#include "master/contender/zookeeper.hpp"

#include "master/detector/standalone.hpp"

#include "tests/containerizer.hpp"
#include "tests/mesos.hpp"

#include <stout/gtest.hpp>

using mesos::master::detector::MasterDetector;
using mesos::master::detector::StandaloneMasterDetector;

using process::Clock;
using process::Future;
using process::Owned;
using process::Promise;

using std::string;
using std::vector;

using testing::DoAll;
using testing::AtMost;

namespace mesos {
namespace internal {
namespace tests {

class MasterNetworkBandwidthSchedulingTest : public MesosTest {
public:
};

namespace {

// Helper function create any kind of unreserved resource.
Resource createResource(const string& resourceName, double amount) {
  Resource resource;
  resource.set_name(resourceName);
  resource.set_type(mesos::Value::SCALAR);
  resource.mutable_scalar()->set_value(amount);
  resource.mutable_allocation_info()->set_role("*");
  return resource;
}

Resource CPU(double amount) {
  return createResource("cpus", amount);
}

Resource NetworkBandwidth(double amount) {
  return createResource("network_bandwidth", amount);
}

Resource Memory(double amount) {
  return createResource("mem", amount);
}

} // namespace {

// Given a task declares network bandwidth,
// When it is scheduled,
// Then it has a running status.
TEST_F(MasterNetworkBandwidthSchedulingTest, TaskRunningWithNetworkBandwidth)
{
  Option<master::Flags> masterFlags = MesosTest::CreateMasterFlags();
  masterFlags.get().network_bandwidth_enforcement = true;
  Try<Owned<cluster::Master>> master = StartMaster(masterFlags);
  ASSERT_SOME(master);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Owned<MasterDetector> detector = master.get()->createDetector();
  Option<slave::Flags> flags = MesosTest::CreateSlaveFlags();
  flags.get().resources = "cpus:4;mem:1000;network_bandwidth:1000";
  Try<Owned<cluster::Slave>> slave = StartSlave(
    detector.get(),
    &containerizer,
    flags);

  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  ASSERT_NE(0u, offers->size());

  Resources taskDeclaredResources;
  taskDeclaredResources += CPU(1);
  taskDeclaredResources += Memory(100);
  taskDeclaredResources += NetworkBandwidth(50);

  TaskInfo task;
  task.set_name("");
  task.mutable_task_id()->set_value("1");
  task.mutable_slave_id()->MergeFrom(offers.get()[0].slave_id());
  task.mutable_resources()->MergeFrom(taskDeclaredResources);
  task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);

  EXPECT_CALL(exec, registered(_, _, _, _));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  Future<Nothing> update;
  EXPECT_CALL(containerizer,
              update(_, taskDeclaredResources))
    .WillOnce(DoAll(FutureSatisfy(&update),
                    Return(Nothing())));

  Future<TaskStatus> status;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status));

  driver.launchTasks(offers.get()[0].id(), {task});

  AWAIT_READY(status);
  EXPECT_EQ(TASK_RUNNING, status->state());
  EXPECT_TRUE(status->has_executor_id());
  EXPECT_EQ(exec.id, status->executor_id());

  AWAIT_READY(update);

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}

// Given a task declares network bandwidth,
// When a task declares more network bandwidth than the slave,
// Then scheduler receives a TASK_ERROR status.
TEST_F(MasterNetworkBandwidthSchedulingTest,
       TaskErrorBecauseOfTooMuchNetworkBandwidth)
{
  Option<master::Flags> masterFlags = MesosTest::CreateMasterFlags();
  masterFlags.get().network_bandwidth_enforcement = true;
  Try<Owned<cluster::Master>> master = StartMaster(masterFlags);
  ASSERT_SOME(master);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Owned<MasterDetector> detector = master.get()->createDetector();
  Option<slave::Flags> flags = MesosTest::CreateSlaveFlags();
  flags.get().resources = "cpus:4;mem:1000;network_bandwidth:1000";
  Try<Owned<cluster::Slave>> slave = StartSlave(
    detector.get(),
    &containerizer,
    flags);

  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  ASSERT_NE(0u, offers->size());

  Resources taskDeclaredResources;
  taskDeclaredResources += CPU(1);
  taskDeclaredResources += Memory(100);
  taskDeclaredResources += NetworkBandwidth(2000);

  TaskInfo task;
  task.set_name("");
  task.mutable_task_id()->set_value("1");
  task.mutable_slave_id()->MergeFrom(offers.get()[0].slave_id());
  task.mutable_resources()->MergeFrom(taskDeclaredResources);
  task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);

  Future<TaskStatus> status;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status));

  driver.launchTasks(offers.get()[0].id(), {task});

  AWAIT_READY(status);
  EXPECT_EQ(TASK_ERROR, status->state());
  EXPECT_TRUE(status->has_task_id());
  EXPECT_EQ("1", status->task_id().value());
  EXPECT_FALSE(status->has_executor_id());

  driver.stop();
  driver.join();
}

// Given a task declares network bandwidth in a label,
// When it is scheduled,
// Then it is provided with the amount declared in the label
// And the task is running.
TEST_F(MasterNetworkBandwidthSchedulingTest,
       TaskRunningWithNetworkBandwidthInLabel)
{
  Option<master::Flags> masterFlags = MesosTest::CreateMasterFlags();
  masterFlags.get().network_bandwidth_enforcement = true;
  Try<Owned<cluster::Master>> master = StartMaster(masterFlags);
  ASSERT_SOME(master);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Owned<MasterDetector> detector = master.get()->createDetector();
  Option<slave::Flags> flags = MesosTest::CreateSlaveFlags();
  flags.get().resources = "cpus:4;mem:1000;network_bandwidth:1000";
  Try<Owned<cluster::Slave>> slave = StartSlave(
    detector.get(),
    &containerizer,
    flags);

  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  ASSERT_NE(0u, offers->size());

  Resources taskDeclaredResources;
  taskDeclaredResources += CPU(1);
  taskDeclaredResources += Memory(100);

  Resources taskActualResources = taskDeclaredResources;
  taskActualResources += NetworkBandwidth(20);

  TaskInfo task;
  task.set_name("");
  task.mutable_task_id()->set_value("1");
  task.mutable_slave_id()->MergeFrom(offers.get()[0].slave_id());
  task.mutable_resources()->MergeFrom(taskDeclaredResources);
  task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);
  mesos::Label* label = task.mutable_labels()->add_labels();
  label->set_key("NETWORK_BANDWIDTH_RESOURCE");
  label->set_value("20");

  EXPECT_CALL(exec, registered(_, _, _, _));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  Future<Nothing> update;
  EXPECT_CALL(containerizer,
              update(_, taskActualResources))
    .WillOnce(DoAll(FutureSatisfy(&update),
                    Return(Nothing())));

  Future<TaskStatus> status;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status));

  driver.launchTasks(offers.get()[0].id(), {task});

  AWAIT_READY(status);
  EXPECT_EQ(TASK_RUNNING, status->state());
  EXPECT_TRUE(status->has_executor_id());
  EXPECT_EQ(exec.id, status->executor_id());

  AWAIT_READY(update);

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}


// Given a task declares network bandwidth in a label,
// When the format is wrong,
// Then the scheduler receives a TASK_ERROR status.
TEST_F(MasterNetworkBandwidthSchedulingTest,
       TaskErrorWithBadNetworkBandwidthInLabel)
{
  Option<master::Flags> masterFlags = MesosTest::CreateMasterFlags();
  masterFlags.get().network_bandwidth_enforcement = true;
  Try<Owned<cluster::Master>> master = StartMaster(masterFlags);
  ASSERT_SOME(master);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Owned<MasterDetector> detector = master.get()->createDetector();
  Option<slave::Flags> flags = MesosTest::CreateSlaveFlags();
  flags.get().resources = "cpus:4;mem:1000;network_bandwidth:1000";
  Try<Owned<cluster::Slave>> slave = StartSlave(
    detector.get(),
    &containerizer,
    flags);

  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  ASSERT_NE(0u, offers->size());

  Resources taskDeclaredResources;
  taskDeclaredResources += CPU(1);
  taskDeclaredResources += Memory(100);

  TaskInfo task;
  task.set_name("");
  task.mutable_task_id()->set_value("1");
  task.mutable_slave_id()->MergeFrom(offers.get()[0].slave_id());
  task.mutable_resources()->MergeFrom(taskDeclaredResources);
  task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);
  mesos::Label* label = task.mutable_labels()->add_labels();
  label->set_key("NETWORK_BANDWIDTH_RESOURCE");
  label->set_value("bad_20");

  EXPECT_CALL(exec, registered(_, _, _, _));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  Future<TaskStatus> status;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status));

  driver.launchTasks(offers.get()[0].id(), {task});

  AWAIT_READY(status);
  EXPECT_EQ(TASK_ERROR, status->state());
  EXPECT_FALSE(status->has_executor_id());

  driver.stop();
  driver.join();
}


// Given a task does not declare network bandwidth,
// When it is scheduled,
// Then it is provided with a default value for network bandwidth
// And the task is running.
TEST_F(MasterNetworkBandwidthSchedulingTest, TaskRunningWithoutNetworkBandwidth)
{
  Option<master::Flags> masterFlags = MesosTest::CreateMasterFlags();
  masterFlags.get().network_bandwidth_enforcement = true;
  Try<Owned<cluster::Master>> master = StartMaster(masterFlags);
  ASSERT_SOME(master);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);

  Owned<MasterDetector> detector = master.get()->createDetector();
  Option<slave::Flags> flags = MesosTest::CreateSlaveFlags();
  flags.get().resources = "cpus:4;mem:1000;network_bandwidth:1000";
  Try<Owned<cluster::Slave>> slave = StartSlave(
    detector.get(),
    &containerizer,
    flags);

  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, DEFAULT_FRAMEWORK_INFO, master.get()->pid, DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return()); // Ignore subsequent offers.

  driver.start();

  AWAIT_READY(offers);
  ASSERT_NE(0u, offers->size());

  Resources taskDeclaredResources;
  taskDeclaredResources += CPU(1);
  taskDeclaredResources += Memory(100);

  Resources taskActualResources = taskDeclaredResources;
  taskActualResources += NetworkBandwidth(500);

  TaskInfo task;
  task.set_name("");
  task.mutable_task_id()->set_value("1");
  task.mutable_slave_id()->MergeFrom(offers.get()[0].slave_id());
  task.mutable_resources()->MergeFrom(taskDeclaredResources);
  task.mutable_executor()->MergeFrom(DEFAULT_EXECUTOR_INFO);

  EXPECT_CALL(exec, registered(_, _, _, _));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_RUNNING));

  Future<Nothing> update;
  EXPECT_CALL(containerizer,
              update(_, taskActualResources))
    .WillOnce(DoAll(FutureSatisfy(&update),
                    Return(Nothing())));

  Future<TaskStatus> status;
  EXPECT_CALL(sched, statusUpdate(&driver, _))
    .WillOnce(FutureArg<1>(&status));

  driver.launchTasks(offers.get()[0].id(), {task});

  AWAIT_READY(status);
  EXPECT_EQ(TASK_RUNNING, status->state());
  EXPECT_TRUE(status->has_executor_id());
  EXPECT_EQ(exec.id, status->executor_id());

  AWAIT_READY(update);

  EXPECT_CALL(exec, shutdown(_))
    .Times(AtMost(1));

  driver.stop();
  driver.join();
}

} // namespace tests {
} // namespace internal {
} // namespace mesos {
