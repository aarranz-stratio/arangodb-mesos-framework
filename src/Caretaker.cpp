///////////////////////////////////////////////////////////////////////////////
/// @brief caretaker for resources and instances
///
/// @file
///
/// DISCLAIMER
///
/// Copyright 2015 ArangoDB GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Dr. Frank Celler
/// @author Copyright 2015, ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "Caretaker.h"

#include "ArangoState.h"
#include "Global.h"
#include "utils.h"
#include "ArangoScheduler.h"
#include "ArangoManager.h"

#include <unordered_set>
#include <random>

#include "pbjson.hpp"

#include <stout/uuid.hpp>

using namespace arangodb;
using namespace std;

// -----------------------------------------------------------------------------
// --SECTION--                                                 private functions
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief checks the number of ports, if the given role is empty, all ports
/// in the offer are taken, otherwise, only ports with that role are counted.
////////////////////////////////////////////////////////////////////////////////

static bool checkPorts (size_t numberOfPorts, const mesos::Offer& offer,
                        std::string const& role) {
  if (numberPorts(offer, role) < numberOfPorts) {
    return false;
  }

  return true;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if the minimum resources are satisfied
/// the offer as well as the minimum resources are
/// flattened to our role before the comparison and ports for all roles
/// in the offer are counted. 
/// For the ports we do not care about reservations, we simply see whether
/// any ports for our role or "*" are included in the offer.
////////////////////////////////////////////////////////////////////////////////

static bool isSuitableOffer (Target const& target,
                             mesos::Offer const& offer) {
  // Note that we do not care whether or not ports are reserved for us
  // or are role "*".
  std::string offerString;
  if (! checkPorts(target.number_ports(), offer, "" )) {
    pbjson::pb2json(&offer, offerString);
    LOG(INFO) 
    << "DEBUG isSuitableOffer: "
    << "offer " << offer.id().value() << " does not have " 
    << target.number_ports() << " ports"
    << "\noffer: " << offerString;
    return false;
  }

  // Never need to flatten the offered resources, since we use find:
  mesos::Resources offered = offer.resources();

  // Always flatten the minimal resources with our role, because find is 
  // flexible:
  mesos::Resources minimum = target.minimal_resources();
  minimum = minimum.flatten(Global::role());

  Option<mesos::Resources> found = offered.find(minimum);
  if (! found.isSome()) {
    pbjson::pb2json(&offer, offerString);
     
    LOG(INFO) 
    << "DEBUG isSuitableOffer: "
    << "offer " << offer.id().value() << " does not have " 
    << "minimal resource requirements " << minimum
    << "\noffer: " << offerString;

    return false;
  }
  return true;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if we have enough reserved resources to get a persistent vol
////////////////////////////////////////////////////////////////////////////////

static bool isSuitableReservedOffer (mesos::Offer const& offer,
                                     Target const& target,
                                     mesos::Resources& toMakePersistent) {
  // mop: this will check the ports
  if (!isSuitableOffer(target, offer)) {
    return false;
  }

  // mop: now we check our reserved resources (role dependent)
  mesos::Resources offered = offer.resources();
  mesos::Resources reserved = offered.reserved(Global::role());
  mesos::Resources required = target.minimal_resources();
  required = required.flatten(Global::role());

  LOG(INFO) << "Reserved: " << reserved;
  LOG(INFO) << "Target: " << required;
  
  auto found = reserved.find(required);
  bool result = found.isSome();
  LOG(INFO) << "isSuitableReservedResult: " << result;

  if (result) {
    toMakePersistent = filterIsDisk(found.get());
  }
  return result;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief finds free ports from a ranges
///////////////////////////////////////////////////////////////////////////////

static void findFreePortsFromRange (mesos::Resources& result,
                                    vector<mesos::Value::Range> const& ranges,
                                    vector<bool> const& isDynRes,
                                    bool reserved,
                                    size_t& found,
                                    size_t len) {
  for (size_t rangeChoice = 0; rangeChoice < ranges.size(); rangeChoice++) {

    const auto& resource = ranges.at(rangeChoice);

    for (uint32_t port = resource.begin(); port <= resource.end(); port++) {
      if (found >= len) {
        return;
      }
      mesos::Resource onePort;
      onePort.set_name("ports");
      onePort.set_type(mesos::Value::RANGES);
      auto* r = onePort.mutable_ranges()->add_range();
      r->set_begin(port);
      r->set_end(port);
      if (! reserved) {
        onePort.set_role("*");
      }
      else {
        onePort.set_role(Global::role());
        if (isDynRes[rangeChoice]) {
          onePort.mutable_reservation()->CopyFrom(Global::principal());
        }
      }
      result += onePort;
      found++;
    }
  }
}

///////////////////////////////////////////////////////////////////////////////
/// @brief finds free ports from an offer
///////////////////////////////////////////////////////////////////////////////

static mesos::Resources findFreePorts (const mesos::Offer& offer, size_t len) {
  vector<mesos::Value::Range> resources;
  vector<mesos::Value::Range> reserved;
  vector<bool>                isDynamicallyReserved;

  auto const& principal = Global::principal();

  for (int i = 0; i < offer.resources_size(); ++i) {
    const auto& resource = offer.resources(i);

    if (resource.name() == "ports" && resource.type() == mesos::Value::RANGES) {
      const auto& ranges = resource.ranges();

      for (int j = 0; j < ranges.range_size(); ++j) {
        const auto& range = ranges.range(j);

        // reserved resources: they must either be statically or
        // dynamically with matching principal
        if (mesos::Resources::isReserved(resource, Option<std::string>())) {
          if (mesos::Resources::isDynamicallyReserved(resource)) {
            if (resource.reservation().principal() == principal.principal()) {
              reserved.push_back(range);
              isDynamicallyReserved.push_back(true);
            }
          }
          else {
            reserved.push_back(range);
            isDynamicallyReserved.push_back(false);
          }
        }

        // unreserved
        else if (mesos::Resources::isUnreserved(resource)) {
          resources.push_back(range);
        }
      }
    }
  }

  mesos::Resources result;
  size_t found = 0;
  findFreePortsFromRange(result, reserved, isDynamicallyReserved, true, 
                         found, len);
  findFreePortsFromRange(result, resources, isDynamicallyReserved, false, 
                         found, len);

  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief resources required for the start of an ephemeral task
////////////////////////////////////////////////////////////////////////////////

static mesos::Resources resourcesForStartEphemeral (mesos::Offer const& offer,
                                                    Target const& target) {
  mesos::Resources offered = offer.resources();
  mesos::Resources minimum = target.minimal_resources();
  
  // We know that the minimal resources fit into the offered resources,
  // when we ignore roles. We now have to grab as much as the minimal 
  // resources prescribe (always with role "*"), but prefer the role
  // specific resources and only turn to the "*" resources if the others
  // are not enough.
#if 0  
  // Old approach without find:
  minimum = minimum.flatten(Global.role());
  mesos::Resources roleSpecificPart 
      = arangodb::intersectResources(offered, minimum);
  mesos::Resources defaultPart = minimum - roleSpecificPart;
  defaultPart = defaultPart.flatten();
  mesos::Resources toUse = roleSpecificPart + defaultPart;
#endif
  Option<mesos::Resources> toUseOpt = offered.find(minimum);
  mesos::Resources toUse;
  if (toUseOpt.isSome()) {
    toUse = toUseOpt.get();
  }
  // toUse will be empty, when it does not fit, we will run into an error later.

  // Add ports with the role we actually found in the resource offer:
  toUse += findFreePorts(offer, target.number_ports());

  // TODO(fc) check if we could use additional resources

  return toUse;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief resources required for a dynamic reservation
////////////////////////////////////////////////////////////////////////////////

static mesos::Resources resourcesForRequestReservation (
                                    mesos::Offer const& offer,
                                    Target const& target) {
  mesos::Resources offered = offer.resources();
  mesos::Resources minimum = target.minimal_resources();
  
  // We know that the minimal resources fit into the offered resources,
  // when we ignore roles. We now have to reserve that part of the 
  // resources with role "*" that is necessary to have all of the minimal
  // resources with our role.
  minimum = minimum.flatten(Global::role());
  mesos::Resources roleSpecificPart 
      = arangodb::intersectResources(offered, minimum);
  mesos::Resources defaultPart = minimum - roleSpecificPart;
  defaultPart = defaultPart.flatten(Global::role(), Global::principal());

  // Now add a port reservation:
  mesos::Resources ports = findFreePorts(offer, 1);
  ports = ports.flatten(Global::role(), Global::principal());
  defaultPart += ports;

  // TODO(fc) check if we could use additional resources

  return defaultPart;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief resources required for starts with persistent volume
////////////////////////////////////////////////////////////////////////////////

static mesos::Resources suitablePersistent (string const& name,
                                            mesos::Offer const& offer,
                                            Target const& target,
                                            string const& persistenceId,
                                            string& containerPath) {

  // However, we have to check that there is a single disk resource that
  // is large enough and has the right persistent ID for us. Therefore
  // we have to separate disk and non-disk resources and proceed similar
  // to resourcesForStartEphemeral for the non-disk resources and
  // special for the disk-resources:

  // For logging:
  std::string offerString;

  mesos::Resources offered = offer.resources();
  mesos::Resources offeredDisk = filterIsDisk(offered);
  offered = filterNotIsDisk(offered);

  mesos::Resources minimum = target.minimal_resources();
  minimum = minimum.flatten(Global::role());
  mesos::Resources minimumDisk = filterIsDisk(minimum);
  minimum = filterNotIsDisk(minimum);

  Option<mesos::Resources> toUseOpt = offered.find(minimum);
  if (! toUseOpt.isSome()) {
    pbjson::pb2json(&offer, offerString);
    LOG(INFO) 
    << "DEBUG suitablePersistent(" << name << "): "
    << "offer " << offer.id().value() << " [" << offer.resources()
    << "] does not have minimal resource requirements "
    << minimum
    << "\noffer: " << offerString;
    return mesos::Resources();    // this indicates an error, ignore offer
  }
  mesos::Resources toUse = toUseOpt.get();

  // Now look at the disk resources:
  size_t mds = diskspace(minimumDisk);

  bool found = false;

  for (const auto& res : offeredDisk) {
    if (res.role() != Global::role()) {
      continue;
    }

    if (diskspace(res) < mds) {
      continue;
    }

    if (! res.has_disk()) {
      continue;
    }

    if (! res.disk().has_persistence()) {
      continue;
    }

    if (persistenceId != res.disk().persistence().id()) {
      continue;
    }

    containerPath = "myPersistentVolume";

    toUse += res;
    found = true;
    break;
  }

  if (! found) {
    pbjson::pb2json(&offer, offerString);
    LOG(INFO) 
    << "DEBUG suitablePersistent(" << name << "): "
    << "offer " << offer.id().value() << " [" << offer.resources()
    << "] does not have enough persistent disk resources "
    << minimumDisk
    << "\noffer: " << offerString;
    return mesos::Resources();  // indicates failure
  }

  // Add ports with the role we actually found in the resource offer:
  toUse += findFreePorts(offer, target.number_ports());

  LOG(INFO)
  << "DEBUG suitablePersistent(" << name << "): SUCCESS";

  return toUse;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief helper to get rid of an offer
////////////////////////////////////////////////////////////////////////////////

static bool notInterested (mesos::Offer const& offer, bool doDecline) {
  if (doDecline) {
    LOG(INFO) << "Declining offer " << offer.id().value();
    Global::scheduler().declineOffer(offer.id());
    return true;
  }
  else {
    return false;
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief do IP address lookup
////////////////////////////////////////////////////////////////////////////////

static string getIPAddress (string hostname) {
  struct addrinfo hints;
  struct addrinfo* ai;
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = 0;
  hints.ai_flags = AI_ADDRCONFIG;
  int res = getaddrinfo(hostname.c_str(), nullptr, &hints, &ai);

  if (res != 0) {
    LOG(WARNING) << "Alarm: res=" << res;
    return hostname;
  }

  struct addrinfo* b = ai;
  std::string result = hostname;

  while (b != nullptr) {
    auto q = reinterpret_cast<struct sockaddr_in*>(ai->ai_addr);
    char buffer[INET_ADDRSTRLEN+5];
    char const* p = inet_ntop(AF_INET, &q->sin_addr, buffer, sizeof(buffer));

    if (p != nullptr) {
      if (p[0] != '1' || p[1] != '2' || p[2] != '7') {
        result = p;
      }
    }
    else {
      LOG(WARNING) << "error in inet_ntop";
    }

    b = b->ai_next;
  }

  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief starts a new arangodb task
////////////////////////////////////////////////////////////////////////////////

static void startArangoDBTask (ArangoState::Lease& lease,
                               TaskType taskType, int pos,
                               TaskPlan const& task,
                               TaskCurrent const& info) {
  
  LOG(INFO) << "Starting number " << pos;
  string taskId = UUID::random().toString();

  if (info.ports_size() != 1) {
    LOG(WARNING)
    << "expected one port, got " << info.ports_size();
    return;
  }

  // use docker to run the task
  mesos::ContainerInfo container;
  container.set_type(mesos::ContainerInfo::DOCKER);

  string myInternalName = task.name();
  string myName = Global::frameworkName() + "-" + myInternalName;

  // command to execute
  mesos::Environment environment;
  mesos::Environment::Variable* roleEnv = environment.add_variables();
  roleEnv->set_name("CLUSTER_ROLE");

  auto& state = lease.state();

  switch (taskType) {
    case TaskType::AGENT: {
        roleEnv->set_value("agency");
        break;
      }
    case TaskType::PRIMARY_DBSERVER: {
        roleEnv->set_value("primary");
        break;
      }
    case TaskType::SECONDARY_DBSERVER: {
        roleEnv->set_value("secondary");
        break;
      }
    case TaskType::COORDINATOR: {
        roleEnv->set_value("coordinator");
        break;
      }
    case TaskType::UNKNOWN: {
      assert(false);
      break;
    }
  }

  if (!task.server_id().empty()) {
    mesos::Environment::Variable* clusterId = environment.add_variables();
    clusterId->set_name("CLUSTER_ID");
    clusterId->set_value(task.server_id());
  }

  switch (taskType) {
    case TaskType::AGENT: {
      auto current = state.current();
      auto plan = state.plan();
      auto agencyId = environment.add_variables();

      agencyId->set_name("AGENCY_ID");
      agencyId->set_value(std::to_string(pos));
      
      auto agencySize = environment.add_variables();
      agencySize->set_name("AGENCY_SIZE");
      agencySize->set_value(std::to_string(plan.agents().entries().size()));
      
      // mop: it is the last agency...this may happen during startup
      // or upon task restart..in any case this is the one that should
      // notify the rest
      if (state.current().agents().entries().size() == state.plan().agents().entries().size()) {
        auto endpoints = environment.add_variables();
        endpoints->set_name("AGENCY_ENDPOINTS");

        std::string endpointsList;

        auto& agentList = state.current().agents().entries();
        bool isFirst = true;
        bool doNotify = true;
        for (const auto &it : agentList) {
          if (it.ports().size() == 0) {
            doNotify = false;
            continue;
          }
          if (!isFirst) {
            endpointsList += " ";
          } else {
            isFirst = false;
          }

          endpointsList += "tcp://" + getIPAddress(it.hostname()) + ":" + to_string(it.ports(0));
        }
        endpoints->set_value(endpointsList);
        
        auto notify = environment.add_variables();
        notify->set_name("AGENCY_NOTIFY");
        notify->set_value(std::to_string(doNotify));
      }
      break;
    }

    case TaskType::SECONDARY_DBSERVER: {
      // mop: intentional fall through
      auto p = environment.add_variables();
      p->set_name("CLUSTER_ID");
      p->set_value(myInternalName);
    }
    case TaskType::PRIMARY_DBSERVER:
    case TaskType::COORDINATOR: {
      // mop: standalone will simply execute the image in default mode
      if (Global::mode() != OperationMode::STANDALONE) {
        auto agents = state.current().agents();
        string hostname = agents.entries(0).hostname();
        uint32_t port = agents.entries(0).ports(0);
      
        auto agencyEndpoint = environment.add_variables();
        agencyEndpoint->set_name("AGENCY_ENDPOINT");
        agencyEndpoint->set_value("tcp://" + getIPAddress(hostname) + ":" + to_string(port));
      }
      break;
    }

    case TaskType::UNKNOWN: {
      assert(false);
      break;
    }
  }
  
  mesos::CommandInfo command;
  command.set_shell(false);

  // Find out the IP address:

  auto p = environment.add_variables();
  p->set_name("HOST");
  p->set_value(getIPAddress(info.hostname()));
  p = environment.add_variables();
  p->set_name("PORT0");
  p->set_value(std::to_string(info.ports(0)));
  command.mutable_environment()->CopyFrom(environment);

  // docker info
  mesos::ContainerInfo::DockerInfo* docker = container.mutable_docker();
  docker->set_image(Global::arangoDBImage());
  docker->set_privileged(Global::arangoDBPrivilegedImage());
  docker->set_network(mesos::ContainerInfo::DockerInfo::BRIDGE);
  docker->set_force_pull_image(Global::arangoDBForcePullImage());

  // port mapping
  mesos::ContainerInfo::DockerInfo::PortMapping* mapping = docker->add_port_mappings();
  mapping->set_host_port(info.ports(0));

  switch (taskType) {
    case TaskType::AGENT:
      mapping->set_container_port(4001);
      break;

    case TaskType::PRIMARY_DBSERVER:
      mapping->set_container_port(8529);
      break;

    case TaskType::COORDINATOR:
      mapping->set_container_port(8529);
      break;

    case TaskType::SECONDARY_DBSERVER:
      mapping->set_container_port(8529);
      break;

    case TaskType::UNKNOWN:
      assert(false);
      break;
  }

  mapping->set_protocol("tcp");

  // volume
  mesos::Resources res = info.resources();
  res = arangodb::filterIsDisk(res);
  const mesos::Resource& disk = *(res.begin());
  if (disk.has_disk() && disk.disk().has_volume()) {
    mesos::Volume* volume = container.add_volumes();
    volume->set_container_path("/var/lib/arangodb3");
    volume->set_host_path("myPersistentVolume");
    volume->set_mode(mesos::Volume::RW);
  }

  mesos::TaskID tid;
  tid.set_value(taskId);

  Global::caretaker().setTaskId(lease, taskType, pos, tid);

  // and start
  mesos::TaskInfo taskInfo = Global::scheduler().startInstance(
    taskId,
    myName,
    info,
    container,
    command);

  Global::manager().registerNewTask(taskId, taskType, pos);

  Global::caretaker().setTaskInfo(lease, taskType, pos, taskInfo);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief request to make offer persistent
////////////////////////////////////////////////////////////////////////////////

static bool requestPersistent (string const& upper,
                               mesos::Offer const& offer,
                               Target const& target,
                               TaskPlan* task,
                               TaskCurrent* taskCur,
                               bool doDecline,
                               TaskType taskType,
                               int pos) {
  mesos::Resources resources;

  if (! isSuitableReservedOffer(offer, target, resources)) {
    return notInterested(offer, doDecline);
  }

  double now = chrono::duration_cast<chrono::seconds>(
    chrono::steady_clock::now().time_since_epoch()).count();

  string persistentId = upper + "_" + UUID::random().toString();

  task->set_state(TASK_STATE_TRYING_TO_PERSIST);
  task->set_timestamp(now);
  task->set_persistence_id(persistentId);

  taskCur->mutable_offer_id()->CopyFrom(offer.id());

  // resources is a single disk resource with our role and principal
  mesos::Resource disk = *resources.begin();
  mesos::Resource::DiskInfo diskInfo;
  diskInfo.mutable_persistence()->set_id(persistentId);

  mesos::Volume volume;
  volume.set_container_path("myPersistentVolume");
  volume.set_mode(mesos::Volume::RW);

  diskInfo.mutable_volume()->CopyFrom(volume);
  disk.mutable_disk()->CopyFrom(diskInfo);

  mesos::Resources persistent;
  persistent += disk;

  LOG(INFO)
  << "DEBUG requestPersistent(" << upper << "): "
  << "trying to make " << offer.id().value()
  << " persistent for " << persistent;

  // Store for later:
  taskCur->mutable_resources()->CopyFrom(persistent);

  Global::scheduler().makePersistent(offer, persistent);

  return true;  // Offer was used
}

////////////////////////////////////////////////////////////////////////////////
/// @brief request to make a reservation
////////////////////////////////////////////////////////////////////////////////

static bool requestReservation (std::string const& upper,
                                mesos::Offer const& offer,
                                Target const& target,
                                TaskPlan* task,
                                TaskCurrent* taskCur,
                                bool doDecline,
                                TaskType taskType,
                                int pos) {
  mesos::Resources resources
        = resourcesForRequestReservation(offer, target);

  if (resources.empty()) {
    // We have everything needed reserved for our role, so we can
    // directly move on to the persistent volume:
    return requestPersistent(upper, offer, target, task, taskCur, doDecline,
                             taskType, pos);
  }

  // First update our own state with the intention of making 
  // a dynamic reservation:
  double now = chrono::duration_cast<chrono::seconds>(
    chrono::steady_clock::now().time_since_epoch()).count();

  task->set_state(TASK_STATE_TRYING_TO_RESERVE);
  task->set_timestamp(now);

  taskCur->mutable_slave_id()->CopyFrom(offer.slave_id());
  taskCur->mutable_offer_id()->CopyFrom(offer.id());
  taskCur->mutable_resources()->CopyFrom(resources);
  taskCur->set_hostname(offer.hostname());

  taskCur->clear_ports();

  // Now use the scheduler to actually try to make the dynamic reservation:
  LOG(INFO)
  << "DEBUG requestReservation: "
  << "trying to reserve " << offer.id().value()
  << " with " << resources;

  Global::scheduler().reserveDynamically(offer, resources);
  return true;  // offer was used
}

////////////////////////////////////////////////////////////////////////////////
/// @brief request to start with persistent volume
////////////////////////////////////////////////////////////////////////////////

static bool requestStartPersistent (ArangoState::Lease& lease,
                                    string const& upper,
                                    mesos::Offer const& offer,
                                    Target const& target,
                                    TaskPlan* task,
                                    TaskCurrent* taskCur,
                                    bool doDecline,
                                    TaskType taskType,
                                    int pos) {
  string persistenceId = task->persistence_id();
  string containerPath;

  mesos::Resources resources = suitablePersistent(
    upper, offer, target, persistenceId, containerPath);

  if (! resources.empty()) {
    double now = chrono::duration_cast<chrono::seconds>(
      chrono::steady_clock::now().time_since_epoch()).count();

    task->set_state(TASK_STATE_TRYING_TO_START);
    task->set_timestamp(now);

    taskCur->mutable_offer_id()->CopyFrom(offer.id());
    taskCur->mutable_resources()->CopyFrom(resources);
    taskCur->set_container_path(containerPath);

    taskCur->clear_ports();

    for (auto& res : resources) {
      if (res.name() == "ports" && res.type() == mesos::Value::RANGES) {
        auto const& ranges = res.ranges();
        for (int r = 0; r < ranges.range_size(); r++) {
          for (uint64_t i = ranges.range(r).begin();
               i <= ranges.range(r).end(); i++) {
            taskCur->add_ports(i);
          }
        }
      }
    }

    LOG(INFO) << "Trying to start with resources:\n"
              << resources;

    startArangoDBTask(lease, taskType, pos, *task, *taskCur);

    return true;  // offer was used
  }

  return notInterested(offer, doDecline);
}                                  

////////////////////////////////////////////////////////////////////////////////
/// @brief request to start without persistent volume
////////////////////////////////////////////////////////////////////////////////

static bool requestStartEphemeral (ArangoState::Lease& lease,
                                   mesos::Offer const& offer,
                                   Target const& target,
                                   TaskPlan* task,
                                   TaskCurrent* taskCur,
                                   TaskType taskType,
                                   int pos) {

  mesos::Resources resources 
      = resourcesForStartEphemeral(offer, target);

  double now = chrono::duration_cast<chrono::seconds>(
    chrono::steady_clock::now().time_since_epoch()).count();

  task->set_state(TASK_STATE_TRYING_TO_START);
  task->set_timestamp(now);

  taskCur->mutable_slave_id()->CopyFrom(offer.slave_id());
  taskCur->mutable_offer_id()->CopyFrom(offer.id());
  taskCur->mutable_resources()->CopyFrom(resources);
  taskCur->set_hostname(offer.hostname());

  taskCur->clear_ports();

  for (auto& res : resources) {
    if (res.name() == "ports" && res.type() == mesos::Value::RANGES) {
      auto const& ranges = res.ranges();
      for (int r = 0; r < ranges.range_size(); r++) {
        for (uint64_t i = ranges.range(r).begin();
             i <= ranges.range(r).end(); i++) {
          taskCur->add_ports(i);
        }
      }
    }
  }

  startArangoDBTask(lease, taskType, pos, *task, *taskCur);

  return true;   // offer was used
}                                  

////////////////////////////////////////////////////////////////////////////////
/// @brief request to restart
////////////////////////////////////////////////////////////////////////////////

static bool requestRestartPersistent (ArangoState::Lease& lease,
                                      string const& upper,
                                      mesos::Offer const& offer,
                                      Target const& target,
                                      TaskPlan* task,
                                      TaskCurrent* taskCur,
                                      bool doDecline,
                                      TaskType taskType,
                                      int pos) {

  string persistenceId = task->persistence_id();
  string containerPath;

  mesos::Resources resources = suitablePersistent(
    upper, offer, target, persistenceId, containerPath);

  if (! resources.empty()) {
    double now = chrono::duration_cast<chrono::seconds>(
      chrono::steady_clock::now().time_since_epoch()).count();

    task->set_state(TASK_STATE_TRYING_TO_RESTART);
    task->set_timestamp(now);

    taskCur->mutable_offer_id()->CopyFrom(offer.id());
    taskCur->mutable_resources()->CopyFrom(resources);
    taskCur->set_container_path(containerPath);

    taskCur->clear_ports();

    for (auto& res : resources) {
      if (res.name() == "ports" && res.type() == mesos::Value::RANGES) {
        auto const& ranges = res.ranges();
        for (int r = 0; r < ranges.range_size(); r++) {
          for (uint64_t i = ranges.range(r).begin();
               i <= ranges.range(r).end(); i++) {
            taskCur->add_ports(i);
          }
        }
      }
    }

    LOG(INFO) << "Trying to restart with resources:\n"
              << resources;

    startArangoDBTask(lease, taskType, pos, *task, *taskCur);

    return true;  // offer was used
  }

  return notInterested(offer, doDecline);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief request to restart
////////////////////////////////////////////////////////////////////////////////

static bool requestRestartEphemeral (ArangoState::Lease& lease,
                                     string const& upper,
                                     mesos::Offer const& offer,
                                     Target const& target,
                                     TaskPlan* task,
                                     TaskCurrent* taskCur,
                                     TaskType taskType,
                                     int pos) {

  mesos::Resources resources 
      = resourcesForStartEphemeral(offer, target);

  double now = chrono::duration_cast<chrono::seconds>(
    chrono::steady_clock::now().time_since_epoch()).count();

  task->set_state(TASK_STATE_TRYING_TO_RESTART);
  task->set_timestamp(now);

  taskCur->mutable_slave_id()->CopyFrom(offer.slave_id());
  taskCur->mutable_offer_id()->CopyFrom(offer.id());
  taskCur->mutable_resources()->CopyFrom(resources);
  taskCur->set_hostname(offer.hostname());

  taskCur->clear_ports();

  for (auto& res : resources) {
    if (res.name() == "ports" && res.type() == mesos::Value::RANGES) {
      auto const& ranges = res.ranges();
      for (int r = 0; r < ranges.range_size(); r++) {
        for (uint64_t i = ranges.range(r).begin();
             i <= ranges.range(r).end(); i++) {
          taskCur->add_ports(i);
        }
      }
    }
  }

  startArangoDBTask(lease, taskType, pos, *task, *taskCur);

  return true;   // offer was used
}

// -----------------------------------------------------------------------------
// --Section--                                                   class Caretaker
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

Caretaker::Caretaker () {
}

////////////////////////////////////////////////////////////////////////////////
/// @brief destructor
////////////////////////////////////////////////////////////////////////////////

Caretaker::~Caretaker () {
}

// -----------------------------------------------------------------------------
// --SECTION--                                                   private methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief isPartnerOnThisSlave, checks, whether the partner of a given
/// secondary dbserver happens to be on a certain slave
////////////////////////////////////////////////////////////////////////////////

static bool isPartnerOnThisSlave (ArangoState::Lease& lease,
                                  int secondaryPosition,
                                  std::string slaveId) {

  Plan const& plan = lease.state().plan();
  TasksPlan const& secondaries = plan.secondaries();

  // Find id of partner of this secondary:
  std::string partner;
  TaskPlan const& tp = secondaries.entries(secondaryPosition);
  if (tp.has_sync_partner()) {
    partner = tp.sync_partner();
  }
  // Find the actual partner among the primaries:
  TasksPlan const& dbservers = plan.dbservers();
  int j;
  for (j = 0; j < dbservers.entries_size(); j++) {
    if (dbservers.entries(j).name() == partner) {
      break;
    }
  }
  if (j < dbservers.entries_size()) {
    // Found him:
    Current const& current = lease.state().current();
    TaskCurrent const& primaryResEntry
      = current.dbservers().entries(j);

    if (primaryResEntry.has_slave_id() &&
        slaveId == primaryResEntry.slave_id().value()) {
      return true;
    }
  }
  return false;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if an offer fits, returns true if the offer was put to some
/// use (or declined) and false, if somebody else can have a go.
////////////////////////////////////////////////////////////////////////////////

bool Caretaker::checkOfferOneType (ArangoState::Lease& lease,
                                   const string& name,
                                   bool persistent,
                                   Target const& target,
                                   TasksPlan* tasks,
                                   TasksCurrent* current,
                                   mesos::Offer const& offer,
                                   bool doDecline,
                                   TaskType taskType) {
  string upper = name;
  for (auto& c : upper) { 
    c = toupper(c);
  }
          
  // ...........................................................................
  // check that the minimal resources are satisfied, here we ignore
  // roles, even if we are after a persistent volume, since we can
  // always reserve more resources for our role dynamically. If we are
  // after ephemeral resources, we do not have to look for the role
  // either.
  // ...........................................................................

  if ((Global::ignoreOffers() & 2) == 2) {
    LOG(INFO) << "Ignoring offer because of 0x2 flag.";
    return notInterested(offer, doDecline);
  }

  int p = tasks->entries_size();

  if (p == 0) {
    LOG(INFO) << "nothing planned for " << name;
    return notInterested(offer, doDecline);
  }

  // ...........................................................................
  // we do not want to start two instances of the same type on the same
  // slave; if we get an offer for the same slave, check if we are
  // currently trying to reserve or persist for this slave; if not,
  // ignore the offer.
  // ...........................................................................

  string const& offerSlaveId = offer.slave_id().value();
  std::vector<int> required;
  
  std::string offerPersistenceId;
  {
    mesos::Resources res = offer.resources();
    res = arangodb::filterIsDisk(res);
    const mesos::Resource& disk = *(res.begin());
    if (disk.has_disk()
        && disk.disk().has_persistence()
       ) {
      offerPersistenceId = disk.disk().persistence().id();
    }
  }

  for (int i = 0; i < p; ++i) {
    TaskPlan* task = tasks->mutable_entries(i);
    TaskCurrent* taskCur = current->mutable_entries(i);

    if (task->state() == TASK_STATE_NEW) {
      required.push_back(i);
      continue;
    }

    // mop: evil edge case...slave went down and was restarted after
    // it was declared as lost...task did not yet find a suitable new
    // slave and we are reoffered our original persistent volume...
    // Upon restart the slave will have a new slave_id...Sync the
    // new slave_id back to our internal structure so we can restart
    // it on the new slave
    if (!offerPersistenceId.empty() && task->persistence_id() == offerPersistenceId) {
      taskCur->mutable_slave_id()->CopyFrom(offer.slave_id());
    }

    if (taskCur->slave_id().value() == offerSlaveId) {
      switch (task->state()) {
        case TASK_STATE_TRYING_TO_RESERVE:
          if ((Global::ignoreOffers() & 4) == 4) {
            LOG(INFO) << "Ignoring offer because of 0x4 flag.";
            return notInterested(offer, doDecline);
          }
          return requestPersistent(upper, offer, target, task, taskCur, 
                                   doDecline, taskType, i);

        case TASK_STATE_TRYING_TO_PERSIST:
          if ((Global::ignoreOffers() & 8) == 8) {
            LOG(INFO) << "Ignoring offer because of 0x8 flag.";
            return notInterested(offer, doDecline);
          }
          return requestStartPersistent(lease, upper, offer, target, task,
                                        taskCur, doDecline, taskType, i);

        case TASK_STATE_KILLED:
        case TASK_STATE_FAILED_OVER:
          if ((Global::ignoreOffers() & 0x10) == 0x10) {
            LOG(INFO) << "Ignoring offer because of 0x10 flag.";
            return notInterested(offer, doDecline);
          }
          if (taskType == TaskType::COORDINATOR) {
            return requestRestartEphemeral(lease, upper, offer, target, task,
                                           taskCur, taskType, i);
          }
          else {
            return requestRestartPersistent(lease, upper, offer, target, task, 
                                            taskCur, doDecline, taskType, i);
          }

        case TASK_STATE_RUNNING:
          // This is a corner case: There is a resource offer for "our" slave
          // we are running, so presumably we are not interested. However,
          // there is a chance (is there?) that we are actually dead but have
          // not yet received this information. In that case, the offer might
          // contain "our" resources and thus the resources must not fall
          // through and be destroyed. Therefore, we check whether our
          // reserved disk is in there and if so, we decline the offer
          // and pretend that we have used it.
          if (! doDecline && taskType != TaskType::COORDINATOR) {
            std::string containerPath;
            mesos::Resources resources = suitablePersistent(
                upper, offer, target, task->persistence_id(), containerPath);
            if (! resources.empty()) {
              // OK, it seems that our resources have been offered to us,
              // this only leaves the conclusion that we are in fact dead.
              // Decline the offer for now, it will come back later.
              // We have to pretend having used the offer, though, otherwise
              // any persistent volumes would get destroyed!
              LOG(INFO) << "Have been offered our own resources, conclusion: "
                        << "WE ARE IN FACT DEAD. Declining offer for now.";
              return notInterested(offer, true);
            }
          }

          return notInterested(offer, doDecline);
        default:
          return notInterested(offer, doDecline);
      }
    }
  }

  // ...........................................................................
  // check if we need an offer
  // ...........................................................................

  if (required.size() == 0) {
    LOG(INFO) << "nothing required";
    return notInterested(offer, doDecline);
  }

  // ...........................................................................
  // check whether the offer is suitable:
  // ...........................................................................
  if (! isSuitableOffer(target, offer)) {
    return notInterested(offer, doDecline);
  }

  // ...........................................................................
  // now decide for whom to use the offer:
  // ...........................................................................
  int decision = required[0];   // this is the default
  if (required.size() == 2) {
    // Here we have to be a bit cleverer: We must not make it so that the
    // last instance requiring a task would be forced to run on the same
    // slave as its primary.
    if (! Global::secondarySameServer() && name == "secondary") {
      // To this end we take the set of slave ids of all primaries and
      // subtract the set of slave ids of secondaries and the proposed
      // new one. If only one remains and that happens to be the
      // slave the partner of required[1] lives on, then required[0]
      // is a bad choice:
      std::unordered_set<std::string> s_ids;
      Current const& current = lease.state().current();
      TasksCurrent const& dbservers = current.dbservers();
      for (int j = 0; j < dbservers.entries_size(); j++) {
        TaskCurrent const& tc = dbservers.entries(j);
        if (tc.has_slave_id()) {
          s_ids.insert(tc.slave_id().value());
        }
      }
      TasksCurrent const& secondaries = current.secondaries();
      for (int j = 0; j < secondaries.entries_size(); j++) {
        TaskCurrent const& tc = secondaries.entries(j);
        if (tc.has_slave_id()) {
          std::string id = tc.slave_id().value();
          auto it = s_ids.find(id);
          if (it != s_ids.end()) {
            s_ids.erase(it);
          }
        }
      }
      auto it = s_ids.find(offer.slave_id().value());
      if (it != s_ids.end()) {
        s_ids.erase(it);
      }
      if (s_ids.size() == 1) {
        std::string id = *s_ids.begin();
        if (isPartnerOnThisSlave(lease, required[1], id)) {
          // Oops, we must not take required[0] otherwise required[1]
          // would get stuck!
          decision = required[1];
        }
      }
    }
  }

  // ...........................................................................
  // do not put a secondary on the same slave than its primary unless 
  // instructed to do so.
  // ...........................................................................

  if (! Global::secondarySameServer() && name == "secondary") {
    if (isPartnerOnThisSlave(lease, decision, offer.slave_id().value())) {
      // we decline this offer, there will be another one
      LOG(INFO) << "secondary not on same slave as its primary";
      return notInterested(offer, doDecline);
    }
  }

  // ...........................................................................
  // do not put a secondary on a slave that we have not yet used at all for a
  // primary, if the user asks us
  // ...........................................................................

  if (Global::secondariesWithDBservers() && name == "secondary") {
    Current globalCurrent = lease.state().current();
    TasksCurrent const& primaryResEntries = globalCurrent.dbservers();

    int found = -1;

    for (int i = 0; i < primaryResEntries.entries_size(); i++) {
      if (primaryResEntries.entries(i).has_slave_id() &&
          offer.slave_id().value()
          == primaryResEntries.entries(i).slave_id().value()) {
        found = i;
        break;
      }
    }

    if (found == -1) {
      // we decline this offer, there will be another one
      LOG(INFO) << "secondary not alone on a slave";
      return notInterested(offer, doDecline);
    }
  }

  // ...........................................................................
  // do not put a coordinator on a slave that we have not yet used at all for a
  // primary, if the user asks us
  // ...........................................................................

  if (Global::coordinatorsWithDBservers() && name == "coordinator") {
    Current globalCurrent = lease.state().current();
    TasksCurrent const& primaryResEntries = globalCurrent.dbservers();

    int found = -1;

    for (int i = 0; i < primaryResEntries.entries_size(); i++) {
      if (primaryResEntries.entries(i).has_slave_id() &&
          offer.slave_id().value()
          == primaryResEntries.entries(i).slave_id().value()) {
        found = i;
        break;
      }
    }

    if (found == -1) {
      // we decline this offer, there will be another one
      LOG(INFO) << "coordinator not alone on a slave";
      return notInterested(offer, doDecline);
    }
  }

  // ...........................................................................
  // try to start directly, if we do not need a reservation
  // ...........................................................................

  TaskPlan* task = tasks->mutable_entries(decision);
  TaskCurrent* taskCur = current->mutable_entries(decision);

  if (! persistent) {
    if ((Global::ignoreOffers() & 0x20) == 0x20) {
      LOG(INFO) << "Ignoring offer because of 0x20 flag.";
      return notInterested(offer, doDecline);
    }
    return requestStartEphemeral(lease, offer, target, task, taskCur, 
                                 taskType, decision);
  }

  // ...........................................................................
  // make a reservation, if we need a persistent volume
  // ...........................................................................

  if ((Global::ignoreOffers() & 0x40) == 0x40) {
    LOG(INFO) << "Ignoring offer because of 0x40 flag.";
    return notInterested(offer, doDecline);
  }
  return requestReservation(upper, offer, target, task, taskCur, doDecline,
                            taskType, decision);
}

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief checks if we can use a resource offer
////////////////////////////////////////////////////////////////////////////////

void Caretaker::checkOffer (const mesos::Offer& offer) {
  auto lease = Global::state().lease();

  Targets targets = lease.state().targets();
  Plan plan = lease.state().plan();
  Current current = lease.state().current();

  checkOfferOneType(lease, "primary", true,
                    targets.dbservers(),
                    plan.mutable_dbservers(),
                    current.mutable_dbservers(),
                    offer, true, TaskType::PRIMARY_DBSERVER);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the task id, clears the task info and status
////////////////////////////////////////////////////////////////////////////////

void Caretaker::setTaskId (ArangoState::Lease& lease,
                           TaskType taskType, int p,
                           mesos::TaskID const& taskId) {
  Current* current = lease.state().mutable_current();

  mesos::SlaveID slaveId;
  slaveId.set_value("");

  mesos::TaskInfo info;
  info.set_name("embryo");
  info.mutable_task_id()->CopyFrom(taskId);
  info.mutable_slave_id()->CopyFrom(slaveId);

  TaskCurrent* taskCur = nullptr;

  switch (taskType) {
    case TaskType::AGENT:
      taskCur = current->mutable_agents()->mutable_entries(p);
      taskCur->mutable_task_info()->CopyFrom(info);
      break;

    case TaskType::PRIMARY_DBSERVER:
      taskCur = current->mutable_dbservers()->mutable_entries(p);
      taskCur->mutable_task_info()->CopyFrom(info);
      break;

    case TaskType::SECONDARY_DBSERVER:
      taskCur = current->mutable_secondaries()->mutable_entries(p);
      taskCur->mutable_task_info()->CopyFrom(info);
      break;

    case TaskType::COORDINATOR:
      taskCur = current->mutable_coordinators()->mutable_entries(p);
      taskCur->mutable_task_info()->CopyFrom(info);
      break;

    case TaskType::UNKNOWN:
      LOG(INFO)
      << "unknown task type " << (int) taskType
      << " for " << taskId.value();
      break;
  }

  lease.changed();   // make sure that the state is persisted later
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the task info
////////////////////////////////////////////////////////////////////////////////

void Caretaker::setTaskInfo (ArangoState::Lease& lease,
                             TaskType taskType, int p,
                             mesos::TaskInfo const& taskInfo) {
  Current* current = lease.state().mutable_current();

  switch (taskType) {
    case TaskType::AGENT:
      current->mutable_agents()
        ->mutable_entries(p)
        ->mutable_task_info()
        ->CopyFrom(taskInfo);
      break;

    case TaskType::PRIMARY_DBSERVER:
      current->mutable_dbservers()
        ->mutable_entries(p)
        ->mutable_task_info()
        ->CopyFrom(taskInfo);
      break;

    case TaskType::SECONDARY_DBSERVER:
      current->mutable_secondaries()
        ->mutable_entries(p)
        ->mutable_task_info()
        ->CopyFrom(taskInfo);
      break;

    case TaskType::COORDINATOR:
      current->mutable_coordinators()
        ->mutable_entries(p)
        ->mutable_task_info()
        ->CopyFrom(taskInfo);
      break;

    case TaskType::UNKNOWN:
      LOG(INFO)
      << "unknown task type " << (int) taskType
      << " for " << taskInfo.task_id().value();
      break;
  }

  lease.changed();  // make sure the state is persisted later
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the task plan state
////////////////////////////////////////////////////////////////////////////////

void Caretaker::setTaskPlanState (ArangoState::Lease& lease,
                                  TaskType taskType, int p,
                                  TaskPlanState const taskPlanState, bool& deleted) {
  Plan* plan = lease.state().mutable_plan();
  TaskPlan* tp = nullptr;

  switch (taskType) {
    case TaskType::AGENT:
      tp = plan->mutable_agents()->mutable_entries(p);
      break;

    case TaskType::PRIMARY_DBSERVER:
      tp = plan->mutable_dbservers()->mutable_entries(p);
      break;

    case TaskType::SECONDARY_DBSERVER:
      tp = plan->mutable_secondaries()->mutable_entries(p);
      break;

    case TaskType::COORDINATOR:
      tp = plan->mutable_coordinators()->mutable_entries(p);
      break;

    case TaskType::UNKNOWN:
      LOG(INFO)
      << "unknown task type " << (int) taskType;
      return;
  }

  if (tp->state() == TASK_STATE_SHUTTING_DOWN && taskPlanState == TASK_STATE_KILLED) {
    // mop: the task was just killed gracefully
    deleted = true;
  } else if (tp->state() != TASK_STATE_DEAD) {
    // Do not overwrite a TASK_STATE_DEAD, because we do not want zombies:
    tp->set_state(taskPlanState);
    double now = chrono::duration_cast<chrono::seconds>(
      chrono::steady_clock::now().time_since_epoch()).count();
    tp->set_timestamp(now);
    lease.changed();   // make sure state will be persisted later
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                          static protected methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief set a default minimum resource set for a target
////////////////////////////////////////////////////////////////////////////////

void Caretaker::setStandardMinimum (Target* te, int size) {
  mesos::Resource* m = te->add_minimal_resources();
  m->set_role("*");
  m->set_name("cpus");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(size == 0 ? 0.25 : 1);

  m = te->add_minimal_resources();
  m->set_role("*");
  m->set_name("mem");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(size == 0 ? 512 : 1024);
  
  m = te->add_minimal_resources();
  m->set_role("*");
  m->set_name("disk");
  m->set_type(mesos::Value::SCALAR);
  m->mutable_scalar()->set_value(size == 0 ? 512 : 1024);
}

void Caretaker::updateTarget() {
  // mop: do nothing by default :S
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
