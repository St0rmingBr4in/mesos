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

#include "master/allocator/mesos/hierarchical.hpp"

#include <algorithm>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <mesos/attributes.hpp>
#include <mesos/resources.hpp>
#include <mesos/roles.hpp>
#include <mesos/type_utils.hpp>

#include <process/after.hpp>
#include <process/delay.hpp>
#include <process/dispatch.hpp>
#include <process/event.hpp>
#include <process/id.hpp>
#include <process/loop.hpp>
#include <process/timeout.hpp>

#include <stout/check.hpp>
#include <stout/hashset.hpp>
#include <stout/set.hpp>
#include <stout/stopwatch.hpp>
#include <stout/stringify.hpp>

#include "common/protobuf_utils.hpp"
#include "common/resource_quantities.hpp"

using std::make_shared;
using std::set;
using std::shared_ptr;
using std::string;
using std::vector;
using std::weak_ptr;

using mesos::allocator::InverseOfferStatus;
using mesos::allocator::Options;

using process::after;
using process::Continue;
using process::ControlFlow;
using process::Failure;
using process::Future;
using process::loop;
using process::Owned;
using process::PID;
using process::Timeout;


namespace mesos {

// Needed to prevent shadowing of template '::operator-<std::set<T>>'
// by non-template '::mesos::operator-'
using ::operator-;

namespace internal {
namespace master {
namespace allocator {
namespace internal {

// Used to represent "filters" for resources unused in offers.
class OfferFilter
{
public:
  virtual ~OfferFilter() {}

  virtual bool filter(const Resources& resources) const = 0;
};


class RefusedOfferFilter : public OfferFilter
{
public:
  RefusedOfferFilter(
      const Resources& _resources,
      const Duration& timeout)
    : _resources(_resources),
      _expired(after(timeout)) {}

  virtual ~RefusedOfferFilter()
  {
    // Cancel the timeout upon destruction to avoid lingering timers.
    _expired.discard();
  }

  Future<Nothing> expired() const { return _expired; };

  bool filter(const Resources& resources) const override
  {
    // NOTE: We do not check for the filter being expired here
    // because `recoverResources()` expects the filter to apply
    // until the filter is removed, see:
    // https://github.com/apache/mesos/commit/2f170f302fe94c4
    //
    // TODO(jieyu): Consider separating the superset check for regular
    // and revocable resources. For example, frameworks might want
    // more revocable resources only or non-revocable resources only,
    // but currently the filter only expires if there is more of both
    // revocable and non-revocable resources.
    return _resources.contains(resources); // Refused resources are superset.
  }

private:
  const Resources _resources;
  Future<Nothing> _expired;
};


// Used to represent "filters" for inverse offers.
//
// NOTE: Since this specific allocator implementation only sends inverse offers
// for maintenance primitives, and those are at the whole slave level, we only
// need to filter based on the time-out.
// If this allocator implementation starts sending out more resource specific
// inverse offers, then we can capture the `unavailableResources` in the filter
// function.
class InverseOfferFilter
{
public:
  virtual ~InverseOfferFilter() {}

  virtual bool filter() const = 0;
};


// NOTE: See comment above `InverseOfferFilter` regarding capturing
// `unavailableResources` if this allocator starts sending fine-grained inverse
// offers.
class RefusedInverseOfferFilter : public InverseOfferFilter
{
public:
  RefusedInverseOfferFilter(const Duration& timeout)
    : _expired(after(timeout)) {}

  virtual ~RefusedInverseOfferFilter()
  {
    // Cancel the timeout upon destruction to avoid lingering timers.
    _expired.discard();
  }

  Future<Nothing> expired() const { return _expired; };

  bool filter() const override
  {
    // See comment above why we currently don't do more fine-grained filtering.
    return _expired.isPending();
  }

private:
  Future<Nothing> _expired;
};


// Helper function to unpack a map of per-role `OfferFilters` to the
// format used by the allocator.
static hashmap<string, vector<ResourceQuantities>> unpackFrameworkOfferFilters(
    const ::google::protobuf::Map<string, OfferFilters>& roleOfferFilters)
{
  hashmap<string, vector<ResourceQuantities>> result;

  // Use `auto` in place of `protobuf::MapPair<string, AllocatableResources>`
  // below since `foreach` is a macro and cannot contain angle brackets.
  foreach (auto&& offerFilters, roleOfferFilters) {
    const string& role = offerFilters.first;
    const OfferFilters& allocatableResources = offerFilters.second;

    if (allocatableResources.has_min_allocatable_resources()) {
      result.insert({role, {}});

      vector<ResourceQuantities>& allocatableResourcesRole = result[role];

      foreach (
          const OfferFilters::ResourceQuantities& quantities,
          allocatableResources.min_allocatable_resources().quantities()) {
        allocatableResourcesRole.push_back(
          ResourceQuantities(quantities.quantities()));
      }
    }
  }

  return result;
}


Framework::Framework(
    const FrameworkInfo& frameworkInfo,
    const set<string>& _suppressedRoles,
    bool _active,
    bool publishPerFrameworkMetrics)
  : roles(protobuf::framework::getRoles(frameworkInfo)),
    suppressedRoles(_suppressedRoles),
    capabilities(frameworkInfo.capabilities()),
    active(_active),
    metrics(new FrameworkMetrics(frameworkInfo, publishPerFrameworkMetrics)),
    minAllocatableResources(
        unpackFrameworkOfferFilters(frameworkInfo.offer_filters())) {}


void HierarchicalAllocatorProcess::initialize(
    const Options& _options,
    const lambda::function<
        void(const FrameworkID&,
             const hashmap<string, hashmap<SlaveID, Resources>>&)>&
      _offerCallback,
    const lambda::function<
        void(const FrameworkID&,
             const hashmap<SlaveID, UnavailableResources>&)>&
      _inverseOfferCallback)
{
  options = _options;
  offerCallback = _offerCallback;
  inverseOfferCallback = _inverseOfferCallback;
  initialized = true;
  paused = false;

  completedFrameworkMetrics =
    BoundedHashMap<FrameworkID, process::Owned<FrameworkMetrics>>(
        options.maxCompletedFrameworks);

  // Resources for quota'ed roles are allocated separately and prior to
  // non-quota'ed roles, hence a dedicated sorter for quota'ed roles is
  // necessary.
  roleSorter->initialize(options.fairnessExcludeResourceNames);
  quotaRoleSorter->initialize(options.fairnessExcludeResourceNames);

  VLOG(1) << "Initialized hierarchical allocator process";

  // Start a loop to run allocation periodically.
  PID<HierarchicalAllocatorProcess> _self = self();

  // Set a temporary variable for the lambda capture.
  Duration allocationInterval = options.allocationInterval;
  loop(
      None(), // Use `None` so we iterate outside the allocator process.
      [allocationInterval]() {
        return after(allocationInterval);
      },
      [_self](const Nothing&) {
        return dispatch(_self, &HierarchicalAllocatorProcess::allocate)
          .then([]() -> ControlFlow<Nothing> { return Continue(); });
      });
}


void HierarchicalAllocatorProcess::recover(
    const int _expectedAgentCount,
    const hashmap<string, Quota>& quotas)
{
  // Recovery should start before actual allocation starts.
  CHECK(initialized);
  CHECK(slaves.empty());
  CHECK_EQ(0u, quotaRoleSorter->count());
  CHECK(_expectedAgentCount >= 0);

  // If there is no quota, recovery is a no-op. Otherwise, we need
  // to delay allocations while agents are reregistering because
  // otherwise we perform allocations on a partial view of resources!
  // We would consequently perform unnecessary allocations to satisfy
  // quota constraints, which can over-allocate non-revocable resources
  // to roles using quota. Then, frameworks in roles without quota can
  // be unnecessarily deprived of resources. We may also be unable to
  // satisfy all of the quota constraints. Repeated master failovers
  // exacerbate the issue.

  if (quotas.empty()) {
    VLOG(1) << "Skipping recovery of hierarchical allocator:"
            << " nothing to recover";

    return;
  }

  // NOTE: `quotaRoleSorter` is updated implicitly in `setQuota()`.
  foreachpair (const string& role, const Quota& quota, quotas) {
    setQuota(role, quota);
  }

  // TODO(alexr): Consider exposing these constants.
  const Duration ALLOCATION_HOLD_OFF_RECOVERY_TIMEOUT = Minutes(10);
  const double AGENT_RECOVERY_FACTOR = 0.8;

  // Record the number of expected agents.
  expectedAgentCount =
    static_cast<int>(_expectedAgentCount * AGENT_RECOVERY_FACTOR);

  // Skip recovery if there are no expected agents. This is not strictly
  // necessary for the allocator to function correctly, but maps better
  // to expected behavior by the user: the allocator is not paused until
  // a new agent is added.
  if (expectedAgentCount.get() == 0) {
    VLOG(1) << "Skipping recovery of hierarchical allocator:"
            << " no reconnecting agents to wait for";

    return;
  }

  // Pause allocation until after a sufficient amount of agents reregister
  // or a timer expires.
  pause();

  // Setup recovery timer.
  delay(ALLOCATION_HOLD_OFF_RECOVERY_TIMEOUT, self(), &Self::resume);

  LOG(INFO) << "Triggered allocator recovery: waiting for "
            << expectedAgentCount.get() << " agents to reconnect or "
            << ALLOCATION_HOLD_OFF_RECOVERY_TIMEOUT << " to pass";
}


void HierarchicalAllocatorProcess::addFramework(
    const FrameworkID& frameworkId,
    const FrameworkInfo& frameworkInfo,
    const hashmap<SlaveID, Resources>& used,
    bool active,
    const set<string>& suppressedRoles)
{
  CHECK(initialized);
  CHECK(!frameworks.contains(frameworkId));

  frameworks.insert(
      {frameworkId, Framework(frameworkInfo,
          suppressedRoles,
          active,
          options.publishPerFrameworkMetrics)});

  const Framework& framework = frameworks.at(frameworkId);

  foreach (const string& role, framework.roles) {
    trackFrameworkUnderRole(frameworkId, role);

    CHECK(frameworkSorters.contains(role));

    if (suppressedRoles.count(role)) {
      frameworkSorters.at(role)->deactivate(frameworkId.value());
      framework.metrics->suppressRole(role);
    } else {
      frameworkSorters.at(role)->activate(frameworkId.value());
      framework.metrics->reviveRole(role);
    }
  }

  // Update the allocation for this framework.
  foreachpair (const SlaveID& slaveId, const Resources& resources, used) {
    // TODO(bmahler): The master won't tell us about resources
    // allocated to agents that have not yet been added, consider
    // CHECKing this case.
    if (!slaves.contains(slaveId)) {
      continue;
    }

    // The slave struct will already be aware of the allocated
    // resources, so we only need to track them in the sorters.
    trackAllocatedResources(slaveId, frameworkId, resources);
  }

  LOG(INFO) << "Added framework " << frameworkId;

  if (active) {
    allocate();
  } else {
    deactivateFramework(frameworkId);
  }
}


void HierarchicalAllocatorProcess::removeFramework(
    const FrameworkID& frameworkId)
{
  CHECK(initialized);
  CHECK(frameworks.contains(frameworkId)) << frameworkId;

  Framework& framework = frameworks.at(frameworkId);

  foreach (const string& role, framework.roles) {
    // Might not be in 'frameworkSorters[role]' because it
    // was previously deactivated and never re-added.
    //
    // TODO(mzhu): This check may no longer be necessary.
    if (!frameworkSorters.contains(role) ||
        !frameworkSorters.at(role)->contains(frameworkId.value())) {
      continue;
    }

    hashmap<SlaveID, Resources> allocation =
      frameworkSorters.at(role)->allocation(frameworkId.value());

    // Update the allocation for this framework.
    foreachpair (const SlaveID& slaveId,
                 const Resources& allocated,
                 allocation) {
      untrackAllocatedResources(slaveId, frameworkId, allocated);
    }

    untrackFrameworkUnderRole(frameworkId, role);
  }

  // Transfer ownership of this framework's metrics to
  // `completedFrameworkMetrics`.
  completedFrameworkMetrics.set(
      frameworkId,
      Owned<FrameworkMetrics>(framework.metrics.release()));

  frameworks.erase(frameworkId);

  LOG(INFO) << "Removed framework " << frameworkId;
}


void HierarchicalAllocatorProcess::activateFramework(
    const FrameworkID& frameworkId)
{
  CHECK(initialized);
  CHECK(frameworks.contains(frameworkId));

  Framework& framework = frameworks.at(frameworkId);

  framework.active = true;

  // Activate all roles for this framework except the roles that
  // are marked as deactivated.
  // Note: A subset of framework roles can be deactivated if the
  // role is specified in `suppressed_roles` during framework
  // (re)registration, or via a subsequent `SUPPRESS` call.
  foreach (const string& role, framework.roles) {
    CHECK(frameworkSorters.contains(role));

    if (!framework.suppressedRoles.count(role)) {
      frameworkSorters.at(role)->activate(frameworkId.value());
    }
  }

  LOG(INFO) << "Activated framework " << frameworkId;

  allocate();
}


void HierarchicalAllocatorProcess::deactivateFramework(
    const FrameworkID& frameworkId)
{
  CHECK(initialized);
  CHECK(frameworks.contains(frameworkId)) << frameworkId;

  Framework& framework = frameworks.at(frameworkId);

  foreach (const string& role, framework.roles) {
    CHECK(frameworkSorters.contains(role));
    frameworkSorters.at(role)->deactivate(frameworkId.value());

    // Note that the Sorter *does not* remove the resources allocated
    // to this framework. For now, this is important because if the
    // framework fails over and is activated, we still want a record
    // of the resources that it is using. We might be able to collapse
    // the added/removed and activated/deactivated in the future.
  }

  framework.active = false;

  framework.offerFilters.clear();
  framework.inverseOfferFilters.clear();

  LOG(INFO) << "Deactivated framework " << frameworkId;
}


void HierarchicalAllocatorProcess::updateFramework(
    const FrameworkID& frameworkId,
    const FrameworkInfo& frameworkInfo,
    const set<string>& suppressedRoles)
{
  CHECK(initialized);
  CHECK(frameworks.contains(frameworkId));

  Framework& framework = frameworks.at(frameworkId);

  const set<string> oldRoles = framework.roles;
  const set<string> newRoles = protobuf::framework::getRoles(frameworkInfo);
  const set<string> oldSuppressedRoles = framework.suppressedRoles;

  foreach (const string& role, newRoles - oldRoles) {
    framework.metrics->addSubscribedRole(role);

    // NOTE: It's possible that we're already tracking this framework
    // under the role because a framework can unsubscribe from a role
    // while it still has resources allocated to the role.
    if (!isFrameworkTrackedUnderRole(frameworkId, role)) {
      trackFrameworkUnderRole(frameworkId, role);
    }
  }

  foreach (const string& role, oldRoles - newRoles) {
    CHECK(frameworkSorters.contains(role));

    frameworkSorters.at(role)->deactivate(frameworkId.value());

    // Stop tracking the framework under this role if there are
    // no longer any resources allocated to it.
    if (frameworkSorters.at(role)->allocation(frameworkId.value()).empty()) {
      untrackFrameworkUnderRole(frameworkId, role);
    }

    if (framework.offerFilters.contains(role)) {
      framework.offerFilters.erase(role);
    }

    framework.metrics->removeSubscribedRole(role);
    framework.suppressedRoles.erase(role);
  }

  framework.roles = newRoles;
  framework.capabilities = frameworkInfo.capabilities();
  framework.minAllocatableResources =
    unpackFrameworkOfferFilters(frameworkInfo.offer_filters());

  suppressRoles(frameworkId, suppressedRoles);
  unsuppressRoles(frameworkId, newRoles - suppressedRoles);

  CHECK(framework.suppressedRoles == suppressedRoles)
    << "After updating framework " << frameworkId
    << " its set of suppressed roles " << stringify(framework.suppressedRoles)
    << " differs from required " << stringify(suppressedRoles);
}


void HierarchicalAllocatorProcess::addSlave(
    const SlaveID& slaveId,
    const SlaveInfo& slaveInfo,
    const vector<SlaveInfo::Capability>& capabilities,
    const Option<Unavailability>& unavailability,
    const Resources& total,
    const hashmap<FrameworkID, Resources>& used)
{
  CHECK(initialized);
  CHECK(!slaves.contains(slaveId));
  CHECK_EQ(slaveId, slaveInfo.id());
  CHECK(!paused || expectedAgentCount.isSome());

  slaves.insert({slaveId,
                 Slave(
                     slaveInfo,
                     protobuf::slave::Capabilities(capabilities),
                     true,
                     total,
                     Resources::sum(used))});

  Slave& slave = slaves.at(slaveId);

  // NOTE: We currently implement maintenance in the allocator to be able to
  // leverage state and features such as the FrameworkSorter and OfferFilter.
  if (unavailability.isSome()) {
    slave.maintenance = Slave::Maintenance(unavailability.get());
  }

  trackReservations(total.reservations());

  roleSorter->add(slaveId, total);

  foreachvalue (const Owned<Sorter>& sorter, frameworkSorters) {
    sorter->add(slaveId, total);
  }

  // See comment at `quotaRoleSorter` declaration regarding non-revocable.
  quotaRoleSorter->add(slaveId, total.nonRevocable());

  foreachpair (const FrameworkID& frameworkId,
               const Resources& allocation,
               used) {
    // There are two cases here:
    //
    //   (1) The framework has already been added to the allocator.
    //       In this case, we track the allocation in the sorters.
    //
    //   (2) The framework has not yet been added to the allocator.
    //       The master will imminently add the framework using
    //       the `FrameworkInfo` recovered from the agent, and in
    //       the interim we do not track the resources allocated to
    //       this framework. This leaves a small window where the
    //       role sorting will under-account for the roles belonging
    //       to this framework.
    //
    // TODO(bmahler): Fix the issue outlined in (2).
    if (!frameworks.contains(frameworkId)) {
      continue;
    }

    trackAllocatedResources(slaveId, frameworkId, allocation);
  }

  // If we have just a number of recovered agents, we cannot distinguish
  // between "old" agents from the registry and "new" ones joined after
  // recovery has started. Because we do not persist enough information
  // to base logical decisions on, any accounting algorithm here will be
  // crude. Hence we opted for checking whether a certain amount of cluster
  // capacity is back online, so that we are reasonably confident that we
  // will not over-commit too many resources to quota that we will not be
  // able to revoke.
  if (paused &&
      expectedAgentCount.isSome() &&
      (static_cast<int>(slaves.size()) >= expectedAgentCount.get())) {
    VLOG(1) << "Recovery complete: sufficient amount of agents added; "
            << slaves.size() << " agents known to the allocator";

    expectedAgentCount = None();
    resume();
  }

  LOG(INFO) << "Added agent " << slaveId << " (" << slave.info.hostname() << ")"
            << " with " << slave.getTotal()
            << " (allocated: " << slave.getAllocated() << ")";

  allocate(slaveId);
}


void HierarchicalAllocatorProcess::removeSlave(
    const SlaveID& slaveId)
{
  CHECK(initialized);
  CHECK(slaves.contains(slaveId));

  // TODO(bmahler): Per MESOS-621, this should remove the allocations
  // that any frameworks have on this slave. Otherwise the caller may
  // "leak" allocated resources accidentally if they forget to recover
  // all the resources. Fixing this would require more information
  // than what we currently track in the allocator.

  roleSorter->remove(slaveId, slaves.at(slaveId).getTotal());

  foreachvalue (const Owned<Sorter>& sorter, frameworkSorters) {
    sorter->remove(slaveId, slaves.at(slaveId).getTotal());
  }

  // See comment at `quotaRoleSorter` declaration regarding non-revocable.
  quotaRoleSorter->remove(
      slaveId, slaves.at(slaveId).getTotal().nonRevocable());

  untrackReservations(slaves.at(slaveId).getTotal().reservations());

  slaves.erase(slaveId);
  allocationCandidates.erase(slaveId);

  removeFilters(slaveId);

  LOG(INFO) << "Removed agent " << slaveId;
}


void HierarchicalAllocatorProcess::updateSlave(
    const SlaveID& slaveId,
    const SlaveInfo& info,
    const Option<Resources>& total,
    const Option<vector<SlaveInfo::Capability>>& capabilities)
{
  CHECK(initialized);
  CHECK(slaves.contains(slaveId));
  CHECK_EQ(slaveId, info.id());

  Slave& slave = slaves.at(slaveId);

  bool updated = false;

  // Remove all offer filters for this slave if it was restarted with changed
  // attributes. We do this because schedulers might have decided that they're
  // not interested in offers from this slave based on the non-presence of some
  // required attributes, and right now they have no other way of learning
  // about this change.
  // TODO(bennoe): Once the agent lifecycle design is implemented, there is a
  // better way to notify frameworks about such changes and let them make this
  // decision. We should think about ways to safely remove this check at that
  // point in time.
  if (!(Attributes(info.attributes()) == Attributes(slave.info.attributes()))) {
    updated = true;
    removeFilters(slaveId);
  }

  if (!(slave.info == info)) {
    updated = true;

    // We unconditionally overwrite the old domain and hostname: Even though
    // the master places some restrictions on this (i.e. agents are not allowed
    // to reregister with a different hostname) inside the allocator it
    // doesn't matter, as the algorithm will work correctly either way.
    slave.info = info;
  }

  // Update agent capabilities.
  if (capabilities.isSome()) {
    protobuf::slave::Capabilities newCapabilities(capabilities.get());
    protobuf::slave::Capabilities oldCapabilities(slave.capabilities);

    slave.capabilities = newCapabilities;

    if (newCapabilities != oldCapabilities) {
      updated = true;

      LOG(INFO) << "Agent " << slaveId << " (" << slave.info.hostname() << ")"
                << " updated with capabilities " << slave.capabilities;
    }
  }

  if (total.isSome()) {
    updated = updateSlaveTotal(slaveId, total.get()) || updated;

    LOG(INFO) << "Agent " << slaveId << " (" << slave.info.hostname() << ")"
              << " updated with total resources " << total.get();
  }

  if (updated) {
    allocate(slaveId);
  }
}


void HierarchicalAllocatorProcess::addResourceProvider(
    const SlaveID& slaveId,
    const Resources& total,
    const hashmap<FrameworkID, Resources>& used)
{
  CHECK(initialized);
  CHECK(slaves.contains(slaveId));

  foreachpair (const FrameworkID& frameworkId,
               const Resources& allocation,
               used) {
    // There are two cases here:
    //
    //   (1) The framework has already been added to the allocator.
    //       In this case, we track the allocation in the sorters.
    //
    //   (2) The framework has not yet been added to the allocator.
    //       We do not track the resources allocated to this
    //       framework. This leaves a small window where the role
    //       sorting will under-account for the roles belonging
    //       to this framework. This case should never occur since
    //       the master will always add the framework first.
    if (!frameworks.contains(frameworkId)) {
      continue;
    }

    trackAllocatedResources(slaveId, frameworkId, allocation);
  }

  Slave& slave = slaves.at(slaveId);
  updateSlaveTotal(slaveId, slave.getTotal() + total);
  slave.allocate(Resources::sum(used));

  VLOG(1)
    << "Grew agent " << slaveId << " by "
    << total << " (total), "
    << used << " (used)";
}


void HierarchicalAllocatorProcess::removeFilters(const SlaveID& slaveId)
{
  CHECK(initialized);

  foreachvalue (Framework& framework, frameworks) {
    framework.inverseOfferFilters.erase(slaveId);

    // Need a typedef here, otherwise the preprocessor gets confused
    // by the comma in the template argument list.
    typedef hashmap<SlaveID, hashset<shared_ptr<OfferFilter>>> Filters;
    foreachvalue (Filters& filters, framework.offerFilters) {
      filters.erase(slaveId);
    }
  }

  LOG(INFO) << "Removed all filters for agent " << slaveId;
}


void HierarchicalAllocatorProcess::activateSlave(
    const SlaveID& slaveId)
{
  CHECK(initialized);
  CHECK(slaves.contains(slaveId));

  slaves.at(slaveId).activated = true;

  LOG(INFO) << "Agent " << slaveId << " reactivated";
}


void HierarchicalAllocatorProcess::deactivateSlave(
    const SlaveID& slaveId)
{
  CHECK(initialized);
  CHECK(slaves.contains(slaveId));

  slaves.at(slaveId).activated = false;

  LOG(INFO) << "Agent " << slaveId << " deactivated";
}


void HierarchicalAllocatorProcess::updateWhitelist(
    const Option<hashset<string>>& _whitelist)
{
  CHECK(initialized);

  whitelist = _whitelist;

  if (whitelist.isSome()) {
    LOG(INFO) << "Updated agent whitelist: " << stringify(whitelist.get());

    if (whitelist->empty()) {
      LOG(WARNING) << "Whitelist is empty, no offers will be made!";
    }
  } else {
    LOG(INFO) << "Advertising offers for all agents";
  }
}


void HierarchicalAllocatorProcess::requestResources(
    const FrameworkID& frameworkId,
    const vector<Request>& requests)
{
  CHECK(initialized);

  LOG(INFO) << "Received resource request from framework " << frameworkId;
}


void HierarchicalAllocatorProcess::updateAllocation(
    const FrameworkID& frameworkId,
    const SlaveID& slaveId,
    const Resources& offeredResources,
    const vector<ResourceConversion>& conversions)
{
  CHECK(initialized);
  CHECK(slaves.contains(slaveId));
  CHECK(frameworks.contains(frameworkId));

  Slave& slave = slaves.at(slaveId);

  // We require that an allocation is tied to a single role.
  //
  // TODO(bmahler): The use of `Resources::allocations()` induces
  // unnecessary copying of `Resources` objects (which is expensive
  // at the time this was written).
  hashmap<string, Resources> allocations = offeredResources.allocations();

  CHECK_EQ(1u, allocations.size());

  string role = allocations.begin()->first;

  CHECK(frameworkSorters.contains(role));

  const Owned<Sorter>& frameworkSorter = frameworkSorters.at(role);
  const Resources frameworkAllocation =
    frameworkSorter->allocation(frameworkId.value(), slaveId);

  // We keep a copy of the offered resources here and it is updated
  // by the specified resource conversions.
  //
  // The resources in the resource conversions should have been
  // normalized by the master (contains proper AllocationInfo).
  //
  // TODO(bmahler): Check that the resources in the resource
  // conversions have AllocationInfo set. The master should enforce
  // this. E.g.
  //
  //  foreach (const ResourceConversion& conversion, conversions) {
  //    CHECK_NONE(validateConversionOnAllocatedResources(conversion));
  //  }
  Try<Resources> _updatedOfferedResources = offeredResources.apply(conversions);
  CHECK_SOME(_updatedOfferedResources);

  const Resources& updatedOfferedResources = _updatedOfferedResources.get();

  // Update the per-slave allocation.
  slave.unallocate(offeredResources);
  slave.allocate(updatedOfferedResources);

  // Update the allocation in the framework sorter.
  frameworkSorter->update(
      frameworkId.value(),
      slaveId,
      offeredResources,
      updatedOfferedResources);

  // Update the allocation in the role sorter.
  roleSorter->update(
      role,
      slaveId,
      offeredResources,
      updatedOfferedResources);

  // Update the allocated resources in the quota sorter. We only update
  // the allocated resources if this role has quota set.
  if (quotaGuarantees.contains(role)) {
    // See comment at `quotaRoleSorter` declaration regarding non-revocable.
    quotaRoleSorter->update(
        role,
        slaveId,
        offeredResources.nonRevocable(),
        updatedOfferedResources.nonRevocable());
  }

  // Update the agent total resources so they are consistent with the updated
  // allocation. We do not directly use `updatedOfferedResources` here because
  // the agent's total resources shouldn't contain:
  // 1. The additionally allocated shared resources.
  // 2. `AllocationInfo` as set in `updatedOfferedResources`.
  //
  // We strip `AllocationInfo` from conversions in order to apply them
  // successfully, since agent's total is stored as unallocated resources.
  vector<ResourceConversion> strippedConversions;
  Resources removedResources;
  foreach (const ResourceConversion& conversion, conversions) {
    // TODO(jieyu): Ideally, we should make sure agent's total
    // resources are consistent with agent's allocation in terms of
    // shared resources. In other words, we should increase agent's
    // total resources as well for those additional allocation we did
    // for shared resources. However, that means we need to update the
    // agent's total resources when performing allocation for shared
    // resources (in `__allocate()`). For now, we detect "additional"
    // allocation for shared resources by checking if a conversion has
    // an empty `consumed` field.
    if (conversion.consumed.empty()) {
      continue;
    }

    // NOTE: For now, a resource conversion must either not change the resource
    // quantities, or completely remove the consumed resources. See MESOS-8825.
    if (conversion.converted.empty()) {
      removedResources += conversion.consumed;
    }

    Resources consumed = conversion.consumed;
    Resources converted = conversion.converted;

    consumed.unallocate();
    converted.unallocate();

    strippedConversions.emplace_back(consumed, converted);
  }

  Try<Resources> updatedTotal = slave.getTotal().apply(strippedConversions);
  CHECK_SOME(updatedTotal);

  updateSlaveTotal(slaveId, updatedTotal.get());

  const Resources updatedFrameworkAllocation =
    frameworkSorter->allocation(frameworkId.value(), slaveId);

  // Check that the changed quantities of the framework's allocation is exactly
  // the same as the resources removed by the resource conversions.
  //
  // TODO(chhsiao): Revisit this constraint if we want to support other type of
  // resource conversions. See MESOS-9015.
  const Resources removedAllocationQuantities =
    frameworkAllocation.createStrippedScalarQuantity() -
    updatedFrameworkAllocation.createStrippedScalarQuantity();
  CHECK_EQ(
      removedAllocationQuantities,
      removedResources.createStrippedScalarQuantity());

  LOG(INFO) << "Updated allocation of framework " << frameworkId
            << " on agent " << slaveId
            << " from " << frameworkAllocation
            << " to " << updatedFrameworkAllocation;
}


Future<Nothing> HierarchicalAllocatorProcess::updateAvailable(
    const SlaveID& slaveId,
    const vector<Offer::Operation>& operations)
{
  // Note that the operations may contain allocated resources,
  // however such operations can be applied to unallocated
  // resources unambiguously, so we don't have a strict CHECK
  // for the operations to contain only unallocated resources.

  CHECK(initialized);
  CHECK(slaves.contains(slaveId));

  Slave& slave = slaves.at(slaveId);

  // It's possible for this 'apply' to fail here because a call to
  // 'allocate' could have been enqueued by the allocator itself
  // just before master's request to enqueue 'updateAvailable'
  // arrives to the allocator.
  //
  //   Master -------R------------
  //                  \----+
  //                       |
  //   Allocator --A-----A-U---A--
  //                \___/ \___/
  //
  //   where A = allocate, R = reserve, U = updateAvailable
  Try<Resources> updatedAvailable = slave.getAvailable().apply(operations);
  if (updatedAvailable.isError()) {
    VLOG(1) << "Failed to update available resources on agent " << slaveId
            << ": " << updatedAvailable.error();
    return Failure(updatedAvailable.error());
  }

  // Update the total resources.
  Try<Resources> updatedTotal = slave.getTotal().apply(operations);
  CHECK_SOME(updatedTotal);

  // Update the total resources in the allocator and role and quota sorters.
  updateSlaveTotal(slaveId, updatedTotal.get());

  return Nothing();
}


void HierarchicalAllocatorProcess::updateUnavailability(
    const SlaveID& slaveId,
    const Option<Unavailability>& unavailability)
{
  CHECK(initialized);
  CHECK(slaves.contains(slaveId));

  Slave& slave = slaves.at(slaveId);

  // NOTE: We currently implement maintenance in the allocator to be able to
  // leverage state and features such as the FrameworkSorter and OfferFilter.

  // We explicitly remove all filters for the inverse offers of this slave. We
  // do this because we want to force frameworks to reassess the calculations
  // they have made to respond to the inverse offer. Unavailability of a slave
  // can have a large effect on failure domain calculations and inter-leaved
  // unavailability schedules.
  foreachvalue (Framework& framework, frameworks) {
    framework.inverseOfferFilters.erase(slaveId);
  }

  // Remove any old unavailability.
  slave.maintenance = None();

  // If we have a new unavailability.
  if (unavailability.isSome()) {
    slave.maintenance = Slave::Maintenance(unavailability.get());
  }

  allocate(slaveId);
}


void HierarchicalAllocatorProcess::updateInverseOffer(
    const SlaveID& slaveId,
    const FrameworkID& frameworkId,
    const Option<UnavailableResources>& unavailableResources,
    const Option<InverseOfferStatus>& status,
    const Option<Filters>& filters)
{
  CHECK(initialized);
  CHECK(frameworks.contains(frameworkId));
  CHECK(slaves.contains(slaveId));

  Framework& framework = frameworks.at(frameworkId);
  Slave& slave = slaves.at(slaveId);

  CHECK(slave.maintenance.isSome())
    << "Agent " << slaveId
    << " (" << slave.info.hostname() << ") should have maintenance scheduled";

  // NOTE: We currently implement maintenance in the allocator to be able to
  // leverage state and features such as the FrameworkSorter and OfferFilter.

  // We use a reference by alias because we intend to modify the
  // `maintenance` and to improve readability.
  Slave::Maintenance& maintenance = slave.maintenance.get();

  // Only handle inverse offers that we currently have outstanding. If it is not
  // currently outstanding this means it is old and can be safely ignored.
  if (maintenance.offersOutstanding.contains(frameworkId)) {
    // We always remove the outstanding offer so that we will send a new offer
    // out the next time we schedule inverse offers.
    maintenance.offersOutstanding.erase(frameworkId);

    // If the response is `Some`, this means the framework responded. Otherwise
    // if it is `None` the inverse offer timed out or was rescinded.
    if (status.isSome()) {
      // For now we don't allow frameworks to respond with `UNKNOWN`. The caller
      // should guard against this. This goes against the pattern of not
      // checking external invariants; however, the allocator and master are
      // currently so tightly coupled that this check is valuable.
      CHECK_NE(status->status(), InverseOfferStatus::UNKNOWN);

      // If the framework responded, we update our state to match.
      maintenance.statuses[frameworkId].CopyFrom(status.get());
    }
  }

  // No need to install filters if `filters` is none.
  if (filters.isNone()) {
    return;
  }

  // Create a refused inverse offer filter.
  Try<Duration> timeout = Duration::create(Filters().refuse_seconds());

  if (filters->refuse_seconds() > Days(365).secs()) {
    LOG(WARNING) << "Using 365 days to create the refused inverse offer"
                 << " filter because the input value is too big";

    timeout = Days(365);
  } else if (filters->refuse_seconds() < 0) {
    LOG(WARNING) << "Using the default value of 'refuse_seconds' to create"
                 << " the refused inverse offer filter because the input"
                 << " value is negative";

    timeout = Duration::create(Filters().refuse_seconds());
  } else {
    timeout = Duration::create(filters->refuse_seconds());

    if (timeout.isError()) {
      LOG(WARNING) << "Using the default value of 'refuse_seconds' to create"
                   << " the refused inverse offer filter because the input"
                   << " value is invalid: " + timeout.error();

      timeout = Duration::create(Filters().refuse_seconds());
    }
  }

  CHECK_SOME(timeout);

  if (timeout.get() != Duration::zero()) {
    VLOG(1) << "Framework " << frameworkId
            << " filtered inverse offers from agent " << slaveId
            << " for " << timeout.get();

    // Create a new inverse offer filter and delay its expiration.
    shared_ptr<RefusedInverseOfferFilter> inverseOfferFilter =
      make_shared<RefusedInverseOfferFilter>(*timeout);

    framework.inverseOfferFilters[slaveId].insert(inverseOfferFilter);

    weak_ptr<InverseOfferFilter> weakPtr = inverseOfferFilter;

    inverseOfferFilter->expired()
      .onReady(defer(self(), [=](Nothing) {
        expire(frameworkId, slaveId, weakPtr);
      }));
  }
}


Future<hashmap<SlaveID, hashmap<FrameworkID, InverseOfferStatus>>>
HierarchicalAllocatorProcess::getInverseOfferStatuses()
{
  CHECK(initialized);

  hashmap<SlaveID, hashmap<FrameworkID, InverseOfferStatus>> result;

  // Make a copy of the most recent statuses.
  foreachpair (const SlaveID& id, const Slave& slave, slaves) {
    if (slave.maintenance.isSome()) {
      result[id] = slave.maintenance->statuses;
    }
  }

  return result;
}


void HierarchicalAllocatorProcess::recoverResources(
    const FrameworkID& frameworkId,
    const SlaveID& slaveId,
    const Resources& resources,
    const Option<Filters>& filters)
{
  CHECK(initialized);

  if (resources.empty()) {
    return;
  }

  // For now, we require that resources are recovered within a single
  // allocation role (since filtering in the same manner across roles
  // seems undesirable).
  //
  // TODO(bmahler): The use of `Resources::allocations()` induces
  // unnecessary copying of `Resources` objects (which is expensive
  // at the time this was written).
  hashmap<string, Resources> allocations = resources.allocations();

  CHECK_EQ(1u, allocations.size());

  string role = allocations.begin()->first;

  // Updated resources allocated to framework (if framework still
  // exists, which it might not in the event that we dispatched
  // Master::offer before we received
  // MesosAllocatorProcess::removeFramework or
  // MesosAllocatorProcess::deactivateFramework, in which case we will
  // have already recovered all of its resources).
  if (frameworks.contains(frameworkId)) {
    CHECK(frameworkSorters.contains(role));

    const Owned<Sorter>& frameworkSorter = frameworkSorters.at(role);

    if (frameworkSorter->contains(frameworkId.value())) {
      untrackAllocatedResources(slaveId, frameworkId, resources);

      // Stop tracking the framework under this role if it's no longer
      // subscribed and no longer has resources allocated to the role.
      if (frameworks.at(frameworkId).roles.count(role) == 0 &&
          frameworkSorter->allocation(frameworkId.value()).empty()) {
        untrackFrameworkUnderRole(frameworkId, role);
      }
    }
  }

  // Update resources allocated on slave (if slave still exists,
  // which it might not in the event that we dispatched Master::offer
  // before we received Allocator::removeSlave).
  if (slaves.contains(slaveId)) {
    Slave& slave = slaves.at(slaveId);

    CHECK(slave.getAllocated().contains(resources))
      << slave.getAllocated() << " does not contain " << resources;

    slave.unallocate(resources);

    VLOG(1) << "Recovered " << resources
            << " (total: " << slave.getTotal()
            << ", allocated: " << slave.getAllocated() << ")"
            << " on agent " << slaveId
            << " from framework " << frameworkId;
  }

  // No need to install the filter if 'filters' is none.
  if (filters.isNone()) {
    return;
  }

  // No need to install the filter if slave/framework does not exist.
  if (!frameworks.contains(frameworkId) || !slaves.contains(slaveId)) {
    return;
  }

  // Create a refused resources filter.
  Try<Duration> timeout = Duration::create(Filters().refuse_seconds());

  if (filters->refuse_seconds() > Days(365).secs()) {
    LOG(WARNING) << "Using 365 days to create the refused resources offer"
                 << " filter because the input value is too big";

    timeout = Days(365);
  } else if (filters->refuse_seconds() < 0) {
    LOG(WARNING) << "Using the default value of 'refuse_seconds' to create"
                 << " the refused resources offer filter because the input"
                 << " value is negative";

    timeout = Duration::create(Filters().refuse_seconds());
  } else {
    timeout = Duration::create(filters->refuse_seconds());

    if (timeout.isError()) {
      LOG(WARNING) << "Using the default value of 'refuse_seconds' to create"
                   << " the refused resources offer filter because the input"
                   << " value is invalid: " + timeout.error();

      timeout = Duration::create(Filters().refuse_seconds());
    }
  }

  CHECK_SOME(timeout);

  if (timeout.get() != Duration::zero()) {
    VLOG(1) << "Framework " << frameworkId
            << " filtered agent " << slaveId
            << " for " << timeout.get();

    // Expire the filter after both an `allocationInterval` and the
    // `timeout` have elapsed. This ensures that the filter does not
    // expire before we perform the next allocation for this agent,
    // see MESOS-4302 for more information.
    //
    // Because the next periodic allocation goes through a dispatch
    // after `allocationInterval`, we do the same for `expire()`
    // (with a helper `_expire()`) to achieve the above.
    //
    // TODO(alexr): If we allocated upon resource recovery
    // (MESOS-3078), we would not need to increase the timeout here.
    timeout = std::max(options.allocationInterval, timeout.get());

    // Create a new filter. Note that we unallocate the resources
    // since filters are applied per-role already.
    Resources unallocated = resources;
    unallocated.unallocate();

    shared_ptr<RefusedOfferFilter> offerFilter =
      make_shared<RefusedOfferFilter>(unallocated, *timeout);

    frameworks.at(frameworkId)
      .offerFilters[role][slaveId].insert(offerFilter);

    weak_ptr<OfferFilter> weakPtr = offerFilter;

    offerFilter->expired()
      .onReady(defer(self(), [=](Nothing) {
        expire(frameworkId, role, slaveId, weakPtr);
      }));
  }
}


void HierarchicalAllocatorProcess::suppressRoles(
    const FrameworkID& frameworkId,
    const set<string>& roles)
{
  CHECK(initialized);
  CHECK(frameworks.contains(frameworkId));

  Framework& framework = frameworks.at(frameworkId);

  // Deactivating the framework in the sorter is fine as long as
  // SUPPRESS is not parameterized. When parameterization is added,
  // we have to differentiate between the cases here.

  foreach (const string& role, roles) {
    CHECK(frameworkSorters.contains(role));

    frameworkSorters.at(role)->deactivate(frameworkId.value());
    framework.suppressedRoles.insert(role);
    framework.metrics->suppressRole(role);
  }

  // TODO(bmahler): This logs roles that were already suppressed,
  // only log roles that transitioned from unsuppressed -> suppressed.
  LOG(INFO) << "Suppressed offers for roles " << stringify(roles)
            << " of framework " << frameworkId;
}


void HierarchicalAllocatorProcess::suppressOffers(
    const FrameworkID& frameworkId,
    const set<string>& roles_)
{
  CHECK(initialized);
  CHECK(frameworks.contains(frameworkId));

  Framework& framework = frameworks.at(frameworkId);

  const set<string>& roles = roles_.empty() ? framework.roles : roles_;
  suppressRoles(frameworkId, roles);
}


void HierarchicalAllocatorProcess::unsuppressRoles(
    const FrameworkID& frameworkId,
    const set<string>& roles)
{
  CHECK(initialized);
  CHECK(frameworks.contains(frameworkId));

  Framework& framework = frameworks.at(frameworkId);

  // Activating the framework in the sorter is fine as long as
  // SUPPRESS is not parameterized. When parameterization is added,
  // we may need to differentiate between the cases here.
  foreach (const string& role, roles) {
    CHECK(frameworkSorters.contains(role));

    frameworkSorters.at(role)->activate(frameworkId.value());
    framework.suppressedRoles.erase(role);
    framework.metrics->reviveRole(role);
  }

  // TODO(bmahler): This logs roles that were already unsuppressed,
  // only log roles that transitioned from suppressed -> unsuppressed.
  LOG(INFO) << "Unsuppressed offers for roles " << stringify(roles)
            << " of framework " << frameworkId;
}


void HierarchicalAllocatorProcess::reviveOffers(
    const FrameworkID& frameworkId,
    const set<string>& roles_)
{
  CHECK(initialized);
  CHECK(frameworks.contains(frameworkId));

  Framework& framework = frameworks.at(frameworkId);
  framework.inverseOfferFilters.clear();

  const set<string>& roles = roles_.empty() ? framework.roles : roles_;
  foreach (const string& role, roles) {
    framework.offerFilters.erase(role);
  }

  unsuppressRoles(frameworkId, roles);

  LOG(INFO) << "Revived roles " << stringify(roles)
            << " of framework " << frameworkId;

  allocate();
}


void HierarchicalAllocatorProcess::setQuota(
    const string& role,
    const Quota& quota)
{
  CHECK(initialized);

  // This method should be called by the master only if the quota for
  // the role is not set. Setting quota differs from updating it because
  // the former moves the role to a different allocation group with a
  // dedicated sorter, while the later just updates the actual quota.
  CHECK(!quotaGuarantees.contains(role));

  // Persist quota in memory and add the role into the corresponding
  // allocation group.
  quotaGuarantees[role] =
    ResourceQuantities::fromScalarResources(quota.info.guarantee());
  quotaRoleSorter->add(role);
  quotaRoleSorter->activate(role);

  // Copy allocation information for the quota'ed role.
  if (roleSorter->contains(role)) {
    foreachpair (
        const SlaveID& slaveId,
        const Resources& resources,
        roleSorter->allocation(role)) {
      // See comment at `quotaRoleSorter` declaration regarding non-revocable.
      quotaRoleSorter->allocated(role, slaveId, resources.nonRevocable());
    }
  }

  metrics.setQuota(role, quota);

  // TODO(alexr): Print all quota info for the role.
  LOG(INFO) << "Set quota " << quota.info.guarantee() << " for role '" << role
            << "'";

  // NOTE: Since quota changes do not result in rebalancing of
  // offered resources, we do not trigger an allocation here; the
  // quota change will be reflected in subsequent allocations.
  //
  // If we add the ability for quota changes to incur a rebalancing
  // of offered resources, then we should trigger that here.
}


void HierarchicalAllocatorProcess::removeQuota(
    const string& role)
{
  CHECK(initialized);

  // Do not allow removing quota if it is not set.
  CHECK(quotaGuarantees.contains(role));
  CHECK(quotaRoleSorter->contains(role));

  // TODO(alexr): Print all quota info for the role.
  LOG(INFO) << "Removed quota " << quotaGuarantees[role] << " for role '"
            << role << "'";

  // Remove the role from the quota'ed allocation group.
  quotaGuarantees.erase(role);
  quotaRoleSorter->remove(role);

  metrics.removeQuota(role);

  // NOTE: Since quota changes do not result in rebalancing of
  // offered resources, we do not trigger an allocation here; the
  // quota change will be reflected in subsequent allocations.
  //
  // If we add the ability for quota changes to incur a rebalancing
  // of offered resources, then we should trigger that here.
}


void HierarchicalAllocatorProcess::updateWeights(
    const vector<WeightInfo>& weightInfos)
{
  CHECK(initialized);

  foreach (const WeightInfo& weightInfo, weightInfos) {
    CHECK(weightInfo.has_role());

    quotaRoleSorter->updateWeight(weightInfo.role(), weightInfo.weight());
    roleSorter->updateWeight(weightInfo.role(), weightInfo.weight());
  }

  // NOTE: Since weight changes do not result in rebalancing of
  // offered resources, we do not trigger an allocation here; the
  // weight change will be reflected in subsequent allocations.
  //
  // If we add the ability for weight changes to incur a rebalancing
  // of offered resources, then we should trigger that here.
}


void HierarchicalAllocatorProcess::pause()
{
  if (!paused) {
    VLOG(1) << "Allocation paused";

    paused = true;
  }
}


void HierarchicalAllocatorProcess::resume()
{
  if (paused) {
    VLOG(1) << "Allocation resumed";

    paused = false;
  }
}


Future<Nothing> HierarchicalAllocatorProcess::allocate()
{
  return allocate(slaves.keys());
}


Future<Nothing> HierarchicalAllocatorProcess::allocate(
    const SlaveID& slaveId)
{
  hashset<SlaveID> slaves({slaveId});
  return allocate(slaves);
}


Future<Nothing> HierarchicalAllocatorProcess::allocate(
    const hashset<SlaveID>& slaveIds)
{
  if (paused) {
    VLOG(2) << "Skipped allocation because the allocator is paused";

    return Nothing();
  }

  allocationCandidates |= slaveIds;

  if (allocation.isNone() || !allocation->isPending()) {
    metrics.allocation_run_latency.start();
    allocation = dispatch(self(), &Self::_allocate);
  }

  return allocation.get();
}


Nothing HierarchicalAllocatorProcess::_allocate()
{
  metrics.allocation_run_latency.stop();

  if (paused) {
    VLOG(2) << "Skipped allocation because the allocator is paused";

    return Nothing();
  }

  ++metrics.allocation_runs;

  Stopwatch stopwatch;
  stopwatch.start();
  metrics.allocation_run.start();

  __allocate();

  // NOTE: For now, we implement maintenance inverse offers within the
  // allocator. We leverage the existing timer/cycle of offers to also do any
  // "deallocation" (inverse offers) necessary to satisfy maintenance needs.
  deallocate();

  metrics.allocation_run.stop();

  VLOG(1) << "Performed allocation for " << allocationCandidates.size()
          << " agents in " << stopwatch.elapsed();

  // Clear the candidates on completion of the allocation run.
  allocationCandidates.clear();

  return Nothing();
}


// TODO(alexr): Consider factoring out the quota allocation logic.
void HierarchicalAllocatorProcess::__allocate()
{
  // Compute the offerable resources, per framework:
  //   (1) For reserved resources on the slave, allocate these to a
  //       framework having the corresponding role.
  //   (2) For unreserved resources on the slave, allocate these
  //       to a framework of any role.
  hashmap<FrameworkID, hashmap<string, hashmap<SlaveID, Resources>>> offerable;

  // NOTE: This function can operate on a small subset of
  // `allocationCandidates`, we have to make sure that we don't
  // assume cluster knowledge when summing resources from that set.

  vector<SlaveID> slaveIds;
  slaveIds.reserve(allocationCandidates.size());

  // Filter out non-whitelisted, removed, and deactivated slaves
  // in order not to send offers for them.
  foreach (const SlaveID& slaveId, allocationCandidates) {
    if (isWhitelisted(slaveId) &&
        slaves.contains(slaveId) &&
        slaves.at(slaveId).activated) {
      slaveIds.push_back(slaveId);
    }
  }

  // Randomize the order in which slaves' resources are allocated.
  //
  // TODO(vinod): Implement a smarter sorting algorithm.
  std::random_shuffle(slaveIds.begin(), slaveIds.end());

  // Returns the result of shrinking the provided resources down to the
  // target resource quantities.
  //
  // Note that some resources are indivisible (e.g. MOUNT volume) and
  // may be excluded in entirety in order to achieve the target size
  // (this may lead to the result size being smaller than the target size).
  //
  // Note also that there may be more than one result that satisfies
  // the target sizes (e.g. need to exclude 1 of 2 disks); this function
  // will make a random choice in these cases.
  auto shrinkResources = [](const Resources& resources,
                            ResourceQuantities target) {
    if (target.empty()) {
      return Resources();
    }

    google::protobuf::RepeatedPtrField<Resource> resourceVector = resources;

    random_shuffle(resourceVector.begin(), resourceVector.end());

    Resources result;
    foreach (Resource& resource, resourceVector) {
      Value::Scalar scalar = target.get(resource.name());

      if (scalar == Value::Scalar()) {
        // Resource that has zero quantity is dropped (shrunk to zero).
        continue;
      }

      if (Resources::shrink(&resource, scalar)) {
        target -= ResourceQuantities::fromScalarResources(resource);
        result += std::move(resource);
      }
    }

    return result;
  };

  // To enforce quota, we keep track of consumed quota for roles with a
  // non-default quota.
  //
  // NOTE: We build the map here to avoid repetitive aggregation in the
  // allocation loop. But this map will still need to be updated in the
  // allocation loop as we make new allocations.
  //
  // TODO(mzhu): Build and persist this information across allocation cycles in
  // track/untrackAllocatedResources().
  //
  // TODO(mzhu): Ideally, we want the sorter to track consumed quota. It then
  // could use consumed quota instead of allocated resources (the former
  // includes unallocated reservations while the latter does not) to calculate
  // the DRF share. This would help to:
  //
  //   (1) Solve the fairness issue when roles with unallocated
  //       reservations may game the allocator (See MESOS-8299).
  //
  //   (2) Simplify the quota enforcement logic -- the allocator
  //       would no longer need to track reservations separately.
  hashmap<string, ResourceQuantities> rolesConsumedQuota;

  // We charge a role against its quota by considering its allocation
  // (including all subrole allocations) as well as any unallocated
  // reservations (including all subrole reservations) since reservations
  // are bound to the role. In other words, we always consider reservations
  // as consuming quota, regardless of whether they are allocated.
  // It is calculated as:
  //
  //   Consumed Quota = reservations + unreserved allocation

  // First add reservations.
  foreachkey (const string& role, quotaGuarantees) {
    rolesConsumedQuota[role] +=
      reservationScalarQuantities.get(role).getOrElse(ResourceQuantities());
  }

  // Then add the unreserved allocation.
  foreachkey (const string& role, roles) {
    const string& topLevelRole = strings::contains(role, "/") ?
      role.substr(0, role.find('/')) : role;

    if (!quotaGuarantees.contains(topLevelRole)) {
      continue;
    }

    if (roleSorter->contains(role)) {
      foreachvalue (const Resources& resources, roleSorter->allocation(role)) {
        rolesConsumedQuota[topLevelRole] +=
          ResourceQuantities::fromScalarResources(
              resources.unreserved().nonRevocable().scalars());
      }
    }
  }
  foreachkey(const string& role, quotaGuarantees) {
      VLOG(2) << "[TOTO] 1724 " + role + ": " << rolesConsumedQuota[role];
  }

  // We need to constantly make sure that we are holding back enough
  // unreserved resources that the remaining quota guarantee can later
  // be satisfied when needed:
  //
  //   Required unreserved headroom =
  //     sum (guarantee - consumed quota) for each role.
  //
  // Given the above, if a role has more reservations (which count towards
  // consumed quota) than quota guarantee, we don't need to hold back any
  // unreserved headroom for it.
  ResourceQuantities requiredHeadroom;
  foreachpair (
      const string& role,
      const ResourceQuantities& guarantee,
      quotaGuarantees) {
    requiredHeadroom +=
      guarantee - rolesConsumedQuota.get(role).getOrElse(ResourceQuantities());
  }

  // We will allocate resources while ensuring that the required
  // unreserved non-revocable headroom is still available. Otherwise,
  // we will not be able to satisfy the quota guarantee later.
  //
  //   available headroom = unallocated unreserved non-revocable resources
  //
  // We compute this as:
  //
  //   available headroom = total resources -
  //                        allocated resources -
  //                        unallocated reservations -
  //                        unallocated revocable resources
  ResourceQuantities availableHeadroom = roleSorter->totalScalarQuantities();

  // NOTE: The role sorter does not return aggregated allocation
  // information whereas `reservationScalarQuantities` does, so
  // we need to loop over only top level roles for the latter.

  // Subtract allocated resources from the total.
  availableHeadroom -= roleSorter->allocationScalarQuantities();

  ResourceQuantities totalAllocatedReservation;
  foreachkey (const string& role, roles) {
    if (!roleSorter->contains(role)) {
      continue; // This role has no allocation.
    }

    foreachvalue (const Resources& resources, roleSorter->allocation(role)) {
      totalAllocatedReservation +=
        ResourceQuantities::fromScalarResources(resources.reserved().scalars());
    }
  }

  ResourceQuantities totalReservation;
  foreachkey (const string& role, reservationScalarQuantities) {
    if (!strings::contains(role, "/")) {
      totalReservation += reservationScalarQuantities.at(role);
    }
  }

  // Subtract total unallocated reservations.
  availableHeadroom -= totalReservation - totalAllocatedReservation;

  // Subtract revocable resources.
  foreachvalue (const Slave& slave, slaves) {
    availableHeadroom -= ResourceQuantities::fromScalarResources(
        slave.getAvailable().revocable().scalars());
  }

  if (!quotaGuarantees.empty()) {
    LOG(INFO) << "Before allocation, required quota headroom is "
              << requiredHeadroom
              << " and available quota headroom is " << availableHeadroom;
  }

  // Due to the two stages in the allocation algorithm and the nature of
  // shared resources being re-offerable even if already allocated, the
  // same shared resources can appear in two (and not more due to the
  // `allocatable` check in each stage) distinct offers in one allocation
  // cycle. This is undesirable since the allocator API contract should
  // not depend on its implementation details. For now we make sure a
  // shared resource is only allocated once in one offer cycle. We use
  // `offeredSharedResources` to keep track of shared resources already
  // allocated in the current cycle.
  hashmap<SlaveID, Resources> offeredSharedResources;

  // Quota guarantee comes first and bursting above the quota guarantee
  // up to the quota limit comes second. Here we process only those
  // roles for that have a non-empty quota guarantee.
  //
  // NOTE: Even though we keep track of the available headroom, we still
  // dedicate the first stage to satisfy role's quota guarantee. The reason
  // is that quota guarantee headroom only acts as a quantity guarantee.
  // Frameworks might have filters or capabilities such that those resources
  // set aside for the headroom cannot be used by these frameworks, resulting
  // in unsatisfied quota guarantee (despite enough quota headroom). Thus
  // we try to satisfy the quota guarantee in this first stage so that those
  // roles with unsatisfied guarantee can have more choices and higher
  // probability in getting their guarantee satisfied.
  foreach (const SlaveID& slaveId, slaveIds) {
    CHECK(slaves.contains(slaveId));
    Slave& slave = slaves.at(slaveId);

    VLOG(2) << "TOTO 1829 Here is the list of roles in order for slave " << slaveId;
    foreach (const string& role, quotaRoleSorter->sort()) {
        VLOG(2) << "TOTO 1829 " << role;
    }
    foreach (const string& role, quotaRoleSorter->sort()) {
      CHECK(quotaGuarantees.contains(role));

      const ResourceQuantities& quotaGuarantee = quotaGuarantees.at(role);

      // If there are no active frameworks in this role, we do not
      // need to do any allocations for this role.
      if (!roles.contains(role)) {
        VLOG(2) << "TOTO 1837 " + role + " is not in 'roles' (no active framework)";
        continue;
      }

      // TODO(bmahler): Handle shared volumes, which are always available but
      // should be excluded here based on `offeredSharedResources`.
      if (slave.getAvailable().empty()) {
        VLOG(2) << "TOTO 1848 " << slaveId << " has no resource anymore, skipping";
        break; // Nothing left on this agent.
      }
      VLOG(2) << "TOTO 1851 " << slaveId << " has resources";

      // Fetch frameworks according to their fair share.
      // NOTE: Suppressed frameworks are not included in the sort.
      CHECK(frameworkSorters.contains(role));
      const Owned<Sorter>& frameworkSorter = frameworkSorters.at(role);

      foreach (const string& frameworkId_, frameworkSorter->sort()) {
        Resources available = slave.getAvailable();
        VLOG(2) << "TOTO 1860 On slave " << slaveId << " here are available resources: " << available;

        // Offer a shared resource only if it has not been offered in this
        // offer cycle to a framework.
        available -= offeredSharedResources.get(slaveId).getOrElse(Resources());

        if (available.allocatableTo(role).empty()) {
          VLOG(2) << "TOTO 1860 " + role + " nothing left for the role on " << slaveId;
          break; // Nothing left for the role.
        }

        FrameworkID frameworkId;
        frameworkId.set_value(frameworkId_);

        CHECK(frameworks.contains(frameworkId));

        const Framework& framework = frameworks.at(frameworkId);
        CHECK(framework.active) << frameworkId;

        if (!isCapableOfReceivingAgent(framework.capabilities, slave)) {
          continue;
        }

        available = stripIncapableResources(available, framework.capabilities);

        // In this first stage, we allocate the role's reservations as well as
        // any unreserved resources while ensuring the role stays within its
        // quota guarantee. This means that we'll "chop" the unreserved
        // resources up to the quota guarantee if necessary.
        //
        // E.g. A role has no allocations or reservations yet and a 10 cpu
        //      quota limit. We'll chop a 15 cpu agent down to only
        //      allocate 10 cpus to the role to keep it within its guarantee.
        //
        // In the case that the role needs some of the resources on this
        // agent to make progress towards its quota, or the role is being
        // allocated some reservation(s), we'll *also* allocate all of
        // the resources for which it does not have quota guarantee.
        //
        // E.g. The agent has 1 cpu, 1024 mem, 1024 disk, 1 gpu, 5 ports
        //      and the role has quota for 1 cpu, 1024 mem. We'll include
        //      the disk, gpu, and ports in the allocation, despite the
        //      role not having any quota guarantee for them.
        //
        // We have to do this for now because it's not possible to set
        // quota on non-scalar resources, like ports. For scalar resources
        // that this role has no quota for, it can be allocated as long
        // as the quota headroom is not violated.
        //
        // TODO(mzhu): Since we're treating the resources with unset
        // quota as having no guarantee and no limit, these should be
        // also be allocated further in the second allocation "phase"
        // below (above guarantee up to limit).

        // NOTE: Currently, frameworks are allowed to have '*' role.
        // Calling reserved('*') returns an empty Resources object.
        //
        // NOTE: Since we currently only support top-level roles to
        // have quota, there are no ancestor reservations involved here.
        Resources toAllocate = available.reserved(role).nonRevocable();

        ResourceQuantities unsatisfiedQuotaGuarantee =
          quotaGuarantee -
          rolesConsumedQuota.get(role).getOrElse(ResourceQuantities());

        VLOG(2) << "TOTO 1925 " << role << " has following unsatisfied guarantees " << unsatisfiedQuotaGuarantee;

        Resources unreserved = available.nonRevocable().unreserved();

        // First, allocate resources up to a role's quota guarantee.
        Resources newQuotaAllocation =
          shrinkResources(unreserved, unsatisfiedQuotaGuarantee);

        toAllocate += newQuotaAllocation;

        // We only include the non-quota guarantee resources (with headroom
        // taken into account) if this role is getting any other resources
        // as well i.e. it is getting either some quota guarantee resources or
        // a reservation. Otherwise, this role is not going to get any
        // allocation. We can safely `continue` here.
        if (toAllocate.empty()) {
          VLOG(2) << "TOTO 1932 " + role + " has neither reserved resources and there is no unreserved resources on that slave " << slaveId;
          continue;
        }

        // Second, allocate scalar resources with unset quota while maintaining
        // the quota headroom.

        Resources nonQuotaGuaranteeResources =
          unreserved.filter([&](const Resource& resource) {
            return quotaGuarantee.get(resource.name()) == Value::Scalar();
          });

        ResourceQuantities surplusHeadroom =
          availableHeadroom - requiredHeadroom;

        nonQuotaGuaranteeResources =
          shrinkResources(nonQuotaGuaranteeResources, surplusHeadroom);

        toAllocate += nonQuotaGuaranteeResources;

        // Lastly, allocate non-scalar resources--we currently do not support
        // setting quota for non-scalar resources. They are always allocated
        // in full.
        toAllocate +=
          unreserved.filter([] (const Resource& resource) {
            return resource.type() != Value::SCALAR;
          });

        // If the framework filters these resources, ignore.
        if (!allocatable(toAllocate, role, framework) ||
            isFiltered(frameworkId, role, slaveId, toAllocate)) {
          VLOG(2) << "TOTO 1963 " + role + " filters these resources " << slaveId;
          continue;
        }

        VLOG(2) << "Allocating " << toAllocate << " on agent " << slaveId
                << " to role " << role << " of framework " << frameworkId
                << " as part of its role quota";

        toAllocate.allocate(role);

        offerable[frameworkId][role][slaveId] += toAllocate;
        offeredSharedResources[slaveId] += toAllocate.shared();

        ResourceQuantities allocatedUnreserved =
          ResourceQuantities::fromScalarResources(
              toAllocate.unreserved().scalars());

        // Update role consumed quota.
        rolesConsumedQuota[role] += allocatedUnreserved;
        for (const string& ancestor : roles::ancestors(role)) {
          rolesConsumedQuota[ancestor] += allocatedUnreserved;
        }

        // Track quota guarantee headroom change.

        // `requiredHeadroom` counts total unsatisfied quota guarantee. Thus
        // only the part of the allocated resources that satisfy some of the
        // role's guarantee should be subtracted. Allocation of reserved
        // resources or resources that this role has unset guarantee do not
        // affect `requiredHeadroom`.
        requiredHeadroom -=
          ResourceQuantities::fromScalarResources(newQuotaAllocation.scalars());

        // `availableHeadroom` counts total unreserved non-revocable resources
        // in the cluster.
        availableHeadroom -= allocatedUnreserved;

        slave.allocate(toAllocate);

        trackAllocatedResources(slaveId, frameworkId, toAllocate);
      }
    }
  }

  // Similar to the first stage, we will allocate resources while ensuring
  // that the required unreserved non-revocable headroom is still available
  // for unsatisfied quota guarantees. Otherwise, we will not be able to
  // satisfy quota guarantees later. Reservations to non-quota roles and
  // revocable resources will always be included in the offers since these
  // are not part of the headroom (and therefore can't be used to satisfy
  // quota guarantees).

  // For logging purposes, we track the number of agents that had resources
  // held back for quota headroom, as well as how many resources in total
  // were held back.
  //
  // While we also held resources back for quota headroom in the first stage,
  // we do not track it there. This is because in the second stage, we try to
  // allocate all resources (including the ones held back in the first stage).
  // Thus only resources held back in the second stage are truly held back for
  // the whole allocation cycle.
  ResourceQuantities heldBackForHeadroom;
  size_t heldBackAgentCount = 0;

  foreach (const SlaveID& slaveId, slaveIds) {
    CHECK(slaves.contains(slaveId));
    Slave& slave = slaves.at(slaveId);

    foreach (const string& role, roleSorter->sort()) {
      // In the second allocation stage, we only allocate
      // for non-quota roles.
      if (quotaGuarantees.contains(role)) {
        VLOG(2) << "TOTO 2035 " + role + " has quota, ignoring (but this is normal) " << slaveId;
        continue;
      }

      // TODO(bmahler): Handle shared volumes, which are always available but
      // should be excluded here based on `offeredSharedResources`.
      if (slave.getAvailable().empty()) {
        break; // Nothing left on this agent.
      }

      // NOTE: Suppressed frameworks are not included in the sort.
      CHECK(frameworkSorters.contains(role));
      const Owned<Sorter>& frameworkSorter = frameworkSorters.at(role);

      foreach (const string& frameworkId_, frameworkSorter->sort()) {
        Resources available = slave.getAvailable();

        // Offer a shared resource only if it has not been offered in this
        // offer cycle to a framework.
        available -= offeredSharedResources.get(slaveId).getOrElse(Resources());

        if (available.allocatableTo(role).empty()) {
          break; // Nothing left for the role.
        }

        FrameworkID frameworkId;
        frameworkId.set_value(frameworkId_);

        CHECK(frameworks.contains(frameworkId));

        const Framework& framework = frameworks.at(frameworkId);

        if (!isCapableOfReceivingAgent(framework.capabilities, slave)) {
          continue;
        }

        available = stripIncapableResources(available, framework.capabilities);

        // The resources we offer are the unreserved resources as well as the
        // reserved resources for this particular role and all its ancestors
        // in the role hierarchy.
        //
        // NOTE: Currently, frameworks are allowed to have '*' role.
        // Calling reserved('*') returns an empty Resources object.
        //
        // TODO(mpark): Offer unreserved resources as revocable beyond quota.
        Resources toAllocate = available.allocatableTo(role);

        // If allocating these resources would reduce the headroom
        // below what is required, we will hold them back.
        const Resources headroomResources =
          toAllocate.scalars().unreserved().nonRevocable();
        const ResourceQuantities headroomToAllocate =
          ResourceQuantities::fromScalarResources(headroomResources);

        bool sufficientHeadroom =
          (availableHeadroom - headroomToAllocate)
            .contains(requiredHeadroom);

        if (!sufficientHeadroom) {
          toAllocate -= headroomResources;
          heldBackForHeadroom += headroomToAllocate;
          ++heldBackAgentCount;
        }

        // If the framework filters these resources, ignore.
        if (!allocatable(toAllocate, role, framework) ||
            isFiltered(frameworkId, role, slaveId, toAllocate)) {
          continue;
        }

        VLOG(2) << "Allocating " << toAllocate << " on agent " << slaveId
                << " to role " << role << " of framework " << frameworkId;

        toAllocate.allocate(role);

        // NOTE: We perform "coarse-grained" allocation, meaning that we always
        // allocate the entire remaining slave resources to a single framework.
        offerable[frameworkId][role][slaveId] += toAllocate;
        offeredSharedResources[slaveId] += toAllocate.shared();

        if (sufficientHeadroom) {
          availableHeadroom -= headroomToAllocate;
        }

        slave.allocate(toAllocate);

        trackAllocatedResources(slaveId, frameworkId, toAllocate);
      }
    }
  }

  if (!quotaGuarantees.empty()) {
    LOG(INFO) << "After allocation, " << requiredHeadroom
              << " are required for quota headroom, "
              << heldBackForHeadroom << " were held back from "
              << heldBackAgentCount
              << " agents to ensure sufficient quota headroom";
  }

  if (offerable.empty()) {
    VLOG(2) << "No allocations performed";
  } else {
    // Now offer the resources to each framework.
    foreachkey (const FrameworkID& frameworkId, offerable) {
      offerCallback(frameworkId, offerable.at(frameworkId));
    }
  }
}


void HierarchicalAllocatorProcess::deallocate()
{
  // If no frameworks are currently registered, no work to do.
  if (roles.empty()) {
    return;
  }
  CHECK(!frameworkSorters.empty());

  // In this case, `offerable` is actually the slaves and/or resources that we
  // want the master to create `InverseOffer`s from.
  hashmap<FrameworkID, hashmap<SlaveID, UnavailableResources>> offerable;

  // For maintenance, we use the framework sorters to determine which frameworks
  // have (1) reserved and / or (2) unreserved resource on the specified
  // slaveIds. This way we only send inverse offers to frameworks that have the
  // potential to lose something. We keep track of which frameworks already have
  // an outstanding inverse offer for the given slave in the
  // UnavailabilityStatus of the specific slave using the `offerOutstanding`
  // flag. This is equivalent to the accounting we do for resources when we send
  // regular offers. If we didn't keep track of outstanding offers then we would
  // keep generating new inverse offers even though the framework had not
  // responded yet.

  foreachvalue (const Owned<Sorter>& frameworkSorter, frameworkSorters) {
    foreach (const SlaveID& slaveId, allocationCandidates) {
      CHECK(slaves.contains(slaveId));

      Slave& slave = slaves.at(slaveId);

      if (slave.maintenance.isSome()) {
        // We use a reference by alias because we intend to modify the
        // `maintenance` and to improve readability.
        Slave::Maintenance& maintenance = slave.maintenance.get();

        hashmap<string, Resources> allocation =
          frameworkSorter->allocation(slaveId);

        foreachkey (const string& frameworkId_, allocation) {
          FrameworkID frameworkId;
          frameworkId.set_value(frameworkId_);

          CHECK(frameworks.contains(frameworkId)) << frameworkId;

          const Framework& framework = frameworks.at(frameworkId);

          // No need to deallocate for an inactive framework as the master
          // will not send it inverse offers.
          if (!framework.active) {
            continue;
          }

          // If this framework doesn't already have inverse offers for the
          // specified slave.
          if (!offerable[frameworkId].contains(slaveId)) {
            // If there isn't already an outstanding inverse offer to this
            // framework for the specified slave.
            if (!maintenance.offersOutstanding.contains(frameworkId)) {
              // Ignore in case the framework filters inverse offers for this
              // slave.
              //
              // NOTE: Since this specific allocator implementation only sends
              // inverse offers for maintenance primitives, and those are at the
              // whole slave level, we only need to filter based on the
              // time-out.
              if (isFiltered(frameworkId, slaveId)) {
                continue;
              }

              const UnavailableResources unavailableResources =
                UnavailableResources{
                    Resources(),
                    maintenance.unavailability};

              // For now we send inverse offers with empty resources when the
              // inverse offer represents maintenance on the machine. In the
              // future we could be more specific about the resources on the
              // host, as we have the information available.
              offerable[frameworkId][slaveId] = unavailableResources;

              // Mark this framework as having an offer outstanding for the
              // specified slave.
              maintenance.offersOutstanding.insert(frameworkId);
            }
          }
        }
      }
    }
  }

  if (offerable.empty()) {
    VLOG(2) << "No inverse offers to send out!";
  } else {
    // Now send inverse offers to each framework.
    foreachkey (const FrameworkID& frameworkId, offerable) {
      inverseOfferCallback(frameworkId, offerable[frameworkId]);
    }
  }
}


void HierarchicalAllocatorProcess::_expire(
    const FrameworkID& frameworkId,
    const string& role,
    const SlaveID& slaveId,
    const weak_ptr<OfferFilter>& offerFilter)
{
  // The filter might have already been removed (e.g., if the
  // framework no longer exists or in `reviveOffers()`) but
  // we may land here if the cancelation of the expiry timeout
  // did not succeed (due to the dispatch already being in the
  // queue).
  shared_ptr<OfferFilter> filter = offerFilter.lock();

  if (filter.get() == nullptr) {
    return;
  }

  // Since this is a performance-sensitive piece of code,
  // we use find to avoid the doing any redundant lookups.
  auto frameworkIterator = frameworks.find(frameworkId);
  CHECK(frameworkIterator != frameworks.end());

  Framework& framework = frameworkIterator->second;

  auto roleFilters = framework.offerFilters.find(role);
  CHECK(roleFilters != framework.offerFilters.end());

  auto agentFilters = roleFilters->second.find(slaveId);
  CHECK(agentFilters != roleFilters->second.end());

  // Erase the filter (may be a no-op per the comment above).
  agentFilters->second.erase(filter);
  if (agentFilters->second.empty()) {
    roleFilters->second.erase(slaveId);
  }
  if (roleFilters->second.empty()) {
    framework.offerFilters.erase(role);
  }
}


void HierarchicalAllocatorProcess::expire(
    const FrameworkID& frameworkId,
    const string& role,
    const SlaveID& slaveId,
    const weak_ptr<OfferFilter>& offerFilter)
{
  dispatch(
      self(),
      &Self::_expire,
      frameworkId,
      role,
      slaveId,
      offerFilter);
}


void HierarchicalAllocatorProcess::expire(
    const FrameworkID& frameworkId,
    const SlaveID& slaveId,
    const weak_ptr<InverseOfferFilter>& inverseOfferFilter)
{
  // The filter might have already been removed (e.g., if the
  // framework no longer exists or in
  // HierarchicalAllocatorProcess::reviveOffers) but
  // we may land here if the cancelation of the expiry timeout
  // did not succeed (due to the dispatch already being in the
  // queue).
  shared_ptr<InverseOfferFilter> filter = inverseOfferFilter.lock();

  if (filter.get() == nullptr) {
    return;
  }

  // Since this is a performance-sensitive piece of code,
  // we use find to avoid the doing any redundant lookups.
  auto frameworkIterator = frameworks.find(frameworkId);
  CHECK(frameworkIterator != frameworks.end());

  Framework& framework = frameworkIterator->second;

  auto filters = framework.inverseOfferFilters.find(slaveId);
  CHECK(filters != framework.inverseOfferFilters.end());

  filters->second.erase(filter);
  if (filters->second.empty()) {
    framework.inverseOfferFilters.erase(slaveId);
  }
}


bool HierarchicalAllocatorProcess::isWhitelisted(
    const SlaveID& slaveId) const
{
  CHECK(slaves.contains(slaveId));

  const Slave& slave = slaves.at(slaveId);

  return whitelist.isNone() || whitelist->contains(slave.info.hostname());
}


bool HierarchicalAllocatorProcess::isFiltered(
    const FrameworkID& frameworkId,
    const string& role,
    const SlaveID& slaveId,
    const Resources& resources) const
{
  CHECK(frameworks.contains(frameworkId));
  CHECK(slaves.contains(slaveId));

  const Framework& framework = frameworks.at(frameworkId);
  const Slave& slave = slaves.at(slaveId);

  // TODO(mpark): Consider moving these filter logic out and into the master,
  // since they are not specific to the hierarchical allocator but rather are
  // global allocation constraints.

  // Prevent offers from non-MULTI_ROLE agents to be allocated
  // to MULTI_ROLE frameworks.
  if (framework.capabilities.multiRole &&
      !slave.capabilities.multiRole) {
    LOG(WARNING) << "Implicitly filtering agent " << slaveId
                 << " from framework " << frameworkId
                 << " because the framework is MULTI_ROLE capable"
                 << " but the agent is not";

    return true;
  }

  // Prevent offers from non-HIERARCHICAL_ROLE agents to be allocated
  // to hierarchical roles.
  if (!slave.capabilities.hierarchicalRole && strings::contains(role, "/")) {
    LOG(WARNING) << "Implicitly filtering agent " << slaveId
                 << " from role " << role
                 << " because the role is hierarchical but the agent is not"
                 << " HIERARCHICAL_ROLE capable";

    return true;
  }

  // Since this is a performance-sensitive piece of code,
  // we use find to avoid the doing any redundant lookups.
  auto roleFilters = framework.offerFilters.find(role);
  if (roleFilters == framework.offerFilters.end()) {
    return false;
  }

  auto agentFilters = roleFilters->second.find(slaveId);
  if (agentFilters == roleFilters->second.end()) {
    return false;
  }

  foreach (const shared_ptr<OfferFilter>& offerFilter, agentFilters->second) {
    if (offerFilter->filter(resources)) {
      VLOG(1) << "Filtered offer with " << resources
              << " on agent " << slaveId
              << " for role " << role
              << " of framework " << frameworkId;

      return true;
    }
  }

  return false;
}


bool HierarchicalAllocatorProcess::isFiltered(
    const FrameworkID& frameworkId,
    const SlaveID& slaveId) const
{
  CHECK(frameworks.contains(frameworkId));
  CHECK(slaves.contains(slaveId));

  const Framework& framework = frameworks.at(frameworkId);

  if (framework.inverseOfferFilters.contains(slaveId)) {
    foreach (const shared_ptr<InverseOfferFilter>& inverseOfferFilter,
             framework.inverseOfferFilters.at(slaveId)) {
      if (inverseOfferFilter->filter()) {
        VLOG(1) << "Filtered unavailability on agent " << slaveId
                << " for framework " << frameworkId;

        return true;
      }
    }
  }

  return false;
}


bool HierarchicalAllocatorProcess::allocatable(
    const Resources& resources,
    const string& role,
    const Framework& framework) const
{
  if (resources.empty()) {
      if (role == "demo") {
          VLOG(2) << "TOTO 2446 Resource offer for demo is empty, returning false";
      }
    return false;
  }

  // By default we check against the globally configured minimal
  // allocatable resources.
  //
  // NOTE: We use a pointer instead of `Option` semantics here to
  // avoid copying vectors in code in the hot path of the allocator.
  const vector<ResourceQuantities>* _minAllocatableResources =
    options.minAllocatableResources.isSome()
      ? &options.minAllocatableResources.get()
      : nullptr;



  if (framework.minAllocatableResources.contains(role)) {
    _minAllocatableResources = &framework.minAllocatableResources.at(role);
  }

  // If no minimal requirements or an empty set of requirments are
  // configured any resource is allocatable.
  if (_minAllocatableResources == nullptr ||
      _minAllocatableResources->empty()) {
      if (role == "demo") {
          VLOG(2) << "TOTO 2472 No min allocatable resource, returning true";
      }
    return true;
  }
      if (role == "demo") {
          foreach (const ResourceQuantities& cur, *_minAllocatableResources) {
              if (!resources.contains(cur)) {
                      VLOG(2) << "TOTO 2477 for demo, offer does not contain minimal requirements for: " << cur << " (offer was: " << resources << ")";
              }
          }
      }

  return std::any_of(
      _minAllocatableResources->begin(),
      _minAllocatableResources->end(),
      [&](const ResourceQuantities& qs) { return resources.contains(qs); });
}


double HierarchicalAllocatorProcess::_resources_offered_or_allocated(
    const string& resource)
{
  double offered_or_allocated = 0;

  foreachvalue (const Slave& slave, slaves) {
    Option<Value::Scalar> value =
      slave.getAllocated().get<Value::Scalar>(resource);

    if (value.isSome()) {
      offered_or_allocated += value->value();
    }
  }

  return offered_or_allocated;
}


double HierarchicalAllocatorProcess::_resources_total(
    const string& resource)
{
  return roleSorter->totalScalarQuantities().get(resource).value();
}


double HierarchicalAllocatorProcess::_quota_allocated(
    const string& role,
    const string& resource)
{
  if (!roleSorter->contains(role)) {
    // This can occur when execution of this callback races with removal of the
    // metric for a role which does not have any associated frameworks.
    return 0.;
  }

  return roleSorter->allocationScalarQuantities(role).get(resource).value();
}


double HierarchicalAllocatorProcess::_offer_filters_active(
    const string& role)
{
  double result = 0;

  foreachvalue (const Framework& framework, frameworks) {
    if (!framework.offerFilters.contains(role)) {
      continue;
    }

    foreachkey (const SlaveID& slaveId, framework.offerFilters.at(role)) {
      result += framework.offerFilters.at(role).at(slaveId).size();
    }
  }

  return result;
}


bool HierarchicalAllocatorProcess::isFrameworkTrackedUnderRole(
    const FrameworkID& frameworkId,
    const string& role) const
{
  return roles.contains(role) &&
         roles.at(role).contains(frameworkId);
}


void HierarchicalAllocatorProcess::trackFrameworkUnderRole(
    const FrameworkID& frameworkId,
    const string& role)
{
  CHECK(initialized);

  // If this is the first framework to subscribe to this role, or have
  // resources allocated to this role, initialize state as necessary.
  if (!roles.contains(role)) {
    roles[role] = {};
    CHECK(!roleSorter->contains(role));
    roleSorter->add(role);
    roleSorter->activate(role);

    CHECK(!frameworkSorters.contains(role));
    frameworkSorters.insert({role, Owned<Sorter>(frameworkSorterFactory())});
    frameworkSorters.at(role)->initialize(options.fairnessExcludeResourceNames);

    foreachvalue (const Slave& slave, slaves) {
      frameworkSorters.at(role)->add(slave.info.id(), slave.getTotal());
    }

    metrics.addRole(role);
  }

  CHECK(!roles.at(role).contains(frameworkId));
  roles.at(role).insert(frameworkId);

  CHECK(!frameworkSorters.at(role)->contains(frameworkId.value()));
  frameworkSorters.at(role)->add(frameworkId.value());
}


void HierarchicalAllocatorProcess::untrackFrameworkUnderRole(
    const FrameworkID& frameworkId,
    const string& role)
{
  CHECK(initialized);

  CHECK(roles.contains(role));
  CHECK(roles.at(role).contains(frameworkId));
  CHECK(frameworkSorters.contains(role));
  CHECK(frameworkSorters.at(role)->contains(frameworkId.value()));

  roles.at(role).erase(frameworkId);
  frameworkSorters.at(role)->remove(frameworkId.value());

  // If no more frameworks are subscribed to this role or have resources
  // allocated to this role, cleanup associated state. This is not necessary
  // for correctness (roles with no registered frameworks will not be offered
  // any resources), but since many different role names might be used over
  // time, we want to avoid leaking resources for no-longer-used role names.
  // Note that we don't remove the role from `quotaRoleSorter` if it exists
  // there, since roles with a quota set still influence allocation even if
  // they don't have any registered frameworks.

  if (roles.at(role).empty()) {
    CHECK_EQ(frameworkSorters.at(role)->count(), 0u);

    roles.erase(role);
    roleSorter->remove(role);

    frameworkSorters.erase(role);

    metrics.removeRole(role);
  }
}


void HierarchicalAllocatorProcess::trackReservations(
    const hashmap<std::string, Resources>& reservations)
{
  foreachpair (const string& role,
               const Resources& resources, reservations) {
    const ResourceQuantities quantities =
      ResourceQuantities::fromScalarResources(resources.scalars());

    if (quantities.empty()) {
      continue; // Do not insert an empty entry.
    }

    // Track it hierarchically up to the top level role.
    reservationScalarQuantities[role] += quantities;
    for (const string& ancestor : roles::ancestors(role)) {
      reservationScalarQuantities[ancestor] += quantities;
    }
  }
}


void HierarchicalAllocatorProcess::untrackReservations(
    const hashmap<std::string, Resources>& reservations)
{
  foreachpair (const string& role,
               const Resources& resources, reservations) {
    const ResourceQuantities quantities =
      ResourceQuantities::fromScalarResources(resources.scalars());

    if (quantities.empty()) {
      continue; // Do not CHECK for the role if there's nothing to untrack.
    }

    // Untrack it hierarchically up to the top level role.
    vector<string> roles = roles::ancestors(role);
    roles.insert(roles.begin(), role);

    for (const string& r : roles) {
      CHECK(reservationScalarQuantities.contains(r));
      ResourceQuantities& currentReservationQuantities =
        reservationScalarQuantities.at(r);

      CHECK(currentReservationQuantities.contains(quantities));
      currentReservationQuantities -= quantities;

      if (currentReservationQuantities.empty()) {
        reservationScalarQuantities.erase(r);
      }
    }
  }
}


bool HierarchicalAllocatorProcess::updateSlaveTotal(
    const SlaveID& slaveId,
    const Resources& total)
{
  CHECK(slaves.contains(slaveId));

  Slave& slave = slaves.at(slaveId);

  const Resources oldTotal = slave.getTotal();

  if (oldTotal == total) {
    return false;
  }

  slave.updateTotal(total);

  hashmap<std::string, Resources> oldReservations = oldTotal.reservations();
  hashmap<std::string, Resources> newReservations = total.reservations();

  if (oldReservations != newReservations) {
    untrackReservations(oldReservations);
    trackReservations(newReservations);
  }

  // Update the totals in the sorters.
  roleSorter->remove(slaveId, oldTotal);
  roleSorter->add(slaveId, total);

  foreachvalue (const Owned<Sorter>& sorter, frameworkSorters) {
    sorter->remove(slaveId, oldTotal);
    sorter->add(slaveId, total);
  }

  // See comment at `quotaRoleSorter` declaration regarding non-revocable.
  quotaRoleSorter->remove(slaveId, oldTotal.nonRevocable());
  quotaRoleSorter->add(slaveId, total.nonRevocable());

  return true;
}


bool HierarchicalAllocatorProcess::isRemoteSlave(const Slave& slave) const
{
  // If the slave does not have a configured domain, assume it is not remote.
  if (!slave.info.has_domain()) {
    return false;
  }

  // The current version of the Mesos agent refuses to startup if a
  // domain is specified without also including a fault domain. That
  // might change in the future, if more types of domains are added.
  // For forward compatibility, we treat agents with a configured
  // domain but no fault domain as having no configured domain.
  if (!slave.info.domain().has_fault_domain()) {
    return false;
  }

  // If the slave has a configured domain (and it has been allowed to
  // register with the master), the master must also have a configured
  // domain.
  CHECK(options.domain.isSome());

  // The master will not startup if configured with a domain but no
  // fault domain.
  CHECK(options.domain->has_fault_domain());

  const DomainInfo::FaultDomain::RegionInfo& masterRegion =
    options.domain->fault_domain().region();
  const DomainInfo::FaultDomain::RegionInfo& slaveRegion =
    slave.info.domain().fault_domain().region();

  return masterRegion != slaveRegion;
}


bool HierarchicalAllocatorProcess::isCapableOfReceivingAgent(
    const protobuf::framework::Capabilities& frameworkCapabilities,
    const Slave& slave) const
{
  // Only offer resources from slaves that have GPUs to
  // frameworks that are capable of receiving GPUs.
  // See MESOS-5634.
  if (options.filterGpuResources && !frameworkCapabilities.gpuResources &&
      slave.hasGpu()) {
    return false;
  }

  // If this framework is not region-aware, don't offer it
  // resources on agents in remote regions.
  if (!frameworkCapabilities.regionAware && isRemoteSlave(slave)) {
    return false;
  }

  return true;
}


Resources HierarchicalAllocatorProcess::stripIncapableResources(
    const Resources& resources,
    const protobuf::framework::Capabilities& frameworkCapabilities) const
{
  return resources.filter([&](const Resource& resource) {
    if (!frameworkCapabilities.sharedResources &&
        Resources::isShared(resource)) {
      return false;
    }

    if (!frameworkCapabilities.revocableResources &&
        Resources::isRevocable(resource)) {
      return false;
    }

    // When reservation refinements are present, old frameworks without the
    // RESERVATION_REFINEMENT capability won't be able to understand the
    // new format. While it's possible to translate the refined reservations
    // into the old format by "hiding" the intermediate reservations in the
    // "stack", this leads to ambiguity when processing RESERVE / UNRESERVE
    // operations. This is due to the loss of information when we drop the
    // intermediate reservations. Therefore, for now we simply filter out
    // resources with refined reservations if the framework does not have
    // the capability.
    if (!frameworkCapabilities.reservationRefinement &&
        Resources::hasRefinedReservations(resource)) {
      return false;
    }

    return true;
  });
}


void HierarchicalAllocatorProcess::trackAllocatedResources(
    const SlaveID& slaveId,
    const FrameworkID& frameworkId,
    const Resources& allocated)
{
  CHECK(slaves.contains(slaveId));
  CHECK(frameworks.contains(frameworkId));

  // TODO(bmahler): Calling allocations() is expensive since it has
  // to construct a map. Avoid this.
  foreachpair (const string& role,
               const Resources& allocation,
               allocated.allocations()) {
    // The framework has resources allocated to this role but it may
    // or may not be subscribed to the role. Either way, we need to
    // track the framework under the role.
    if (!isFrameworkTrackedUnderRole(frameworkId, role)) {
      trackFrameworkUnderRole(frameworkId, role);
    }

    CHECK(roleSorter->contains(role));
    CHECK(frameworkSorters.contains(role));
    CHECK(frameworkSorters.at(role)->contains(frameworkId.value()));

    roleSorter->allocated(role, slaveId, allocation);
    frameworkSorters.at(role)->allocated(
        frameworkId.value(), slaveId, allocation);

    if (quotaGuarantees.contains(role)) {
      // See comment at `quotaRoleSorter` declaration regarding non-revocable.
      quotaRoleSorter->allocated(role, slaveId, allocation.nonRevocable());
    }
  }
}


void HierarchicalAllocatorProcess::untrackAllocatedResources(
    const SlaveID& slaveId,
    const FrameworkID& frameworkId,
    const Resources& allocated)
{
  // TODO(mzhu): Add a `CHECK(slaves.contains(slaveId));`
  // here once MESOS-621 is resolved. Ideally, `removeSlave()`
  // should unallocate resources in the framework sorters.
  // But currently, a slave is removed first via `removeSlave()`
  // and later a call to `recoverResources()` occurs to recover
  // the framework's resources.
  CHECK(frameworks.contains(frameworkId));

  // TODO(bmahler): Calling allocations() is expensive since it has
  // to construct a map. Avoid this.
  foreachpair (const string& role,
               const Resources& allocation,
               allocated.allocations()) {
    CHECK(roleSorter->contains(role));
    CHECK(frameworkSorters.contains(role));
    CHECK(frameworkSorters.at(role)->contains(frameworkId.value()));

    frameworkSorters.at(role)->unallocated(
        frameworkId.value(), slaveId, allocation);

    roleSorter->unallocated(role, slaveId, allocation);

    if (quotaGuarantees.contains(role)) {
      // See comment at `quotaRoleSorter` declaration regarding non-revocable.
      quotaRoleSorter->unallocated(role, slaveId, allocation.nonRevocable());
    }
  }
}


} // namespace internal {
} // namespace allocator {
} // namespace master {
} // namespace internal {
} // namespace mesos {
