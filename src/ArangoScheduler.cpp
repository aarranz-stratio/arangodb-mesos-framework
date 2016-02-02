////////////////////////////////////////////////////////////////////////////////
/// @brief scheduler for the ArangoDB framework
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

#include "ArangoScheduler.h"

#include "ArangoManager.h"
#include "ArangoState.h"
#include "Global.h"
#include "utils.h"

#include <atomic>
#include <iostream>
#include <string>

#include <boost/algorithm/string.hpp>
#include <curl/curl.h>

#include <mesos/resources.hpp>

using namespace std;
using namespace boost;
using namespace arangodb;

// -----------------------------------------------------------------------------
// --SECTION--                                                 private functions
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief checks the master version
////////////////////////////////////////////////////////////////////////////////

static void checkVersion (string hostname, int port) {
  std::string body;
  long httpCode = 0;
  int res = doHTTPGet("http://" + hostname + ":" + to_string(port) 
                      + "/state.json", body, httpCode);
  if (res == 0 && httpCode == 200) {
    picojson::value s;
    std::string err = picojson::parse(s, body);

    if (err.empty()) {
      if (s.is<picojson::object>()) {
        auto& o = s.get<picojson::object>();
        auto& v = o["version"];

        if (v.is<string>()) {
          string version = v.get<string>();

          if (! version.empty()) {
            vector<string> vv;
            boost::split(vv, version, boost::is_any_of("."));

            int major = 0;
            int minor = 0;

            if (vv.size() >= 2) {
              major = stoi(vv[0]);
              minor = stoi(vv[1]);

              if (major == 0 && minor < 22) {
                err = "version '" + version + "' is not suitable";
              }
              else {
                LOG(INFO)
                << "version '" << version << "' is suitable";
              }
            }
            else {
              err = "version '" + version + "' is corrupt";
            }
          }
          else {
            err = "version field is empty";
          }
        }
        else {
          err = "version field is not a string";
        }
      }
      else {
        err = "state is not a json object";
      }
    }

    if (! err.empty()) {
      LOG(WARNING)
      << "malformed state object from master: " << err;
    }
  }
  else {
    LOG(WARNING)
    << "could not get version from master, curl error: "
    << res << ", HTTP result code: " << httpCode;
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                             class ArangoScheduler
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

ArangoScheduler::ArangoScheduler ()
  : _driver(nullptr) {
}

////////////////////////////////////////////////////////////////////////////////
/// @brief destructor
////////////////////////////////////////////////////////////////////////////////

ArangoScheduler::~ArangoScheduler () {
}

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the driver
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::setDriver (mesos::SchedulerDriver* driver) {
  _driver = driver;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief makes a dynamic reservation
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::reserveDynamically (const mesos::Offer& offer,
                                          const mesos::Resources& resources) const {
  mesos::Offer::Operation reserve;
  reserve.set_type(mesos::Offer::Operation::RESERVE);
  reserve.mutable_reserve()->mutable_resources()->CopyFrom(resources);

  _driver->acceptOffers({offer.id()}, {reserve});
}

////////////////////////////////////////////////////////////////////////////////
/// @brief cancels a dynamic reservation
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::unreserveDynamically (const mesos::Offer& offer,
                                            const mesos::Resources& resources) const {
  mesos::Offer::Operation unreserve;
  unreserve.set_type(mesos::Offer::Operation::UNRESERVE);
  unreserve.mutable_unreserve()->mutable_resources()->CopyFrom(resources);

  _driver->acceptOffers({offer.id()}, {unreserve});
}

////////////////////////////////////////////////////////////////////////////////
/// @brief creates a persistent disk
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::makePersistent (const mesos::Offer& offer,
                                      const mesos::Resources& resources) const {
  mesos::Offer::Operation reserve;
  reserve.set_type(mesos::Offer::Operation::CREATE);
  reserve.mutable_create()->mutable_volumes()->CopyFrom(resources);

  _driver->acceptOffers({offer.id()}, {reserve});
}

////////////////////////////////////////////////////////////////////////////////
/// @brief destroys persistent disks
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::destroyPersistent (const mesos::Offer& offer,
                                         const mesos::Resources& resources) const {
  mesos::Offer::Operation destroy;
  destroy.set_type(mesos::Offer::Operation::DESTROY);
  destroy.mutable_destroy()->mutable_volumes()->CopyFrom(resources);

  _driver->acceptOffers({offer.id()}, {destroy});
}

////////////////////////////////////////////////////////////////////////////////
/// @brief declines an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::declineOffer (const mesos::OfferID& offerId) const {
  _driver->declineOffer(offerId);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief starts an instances with a given offer and resources
////////////////////////////////////////////////////////////////////////////////

mesos::TaskInfo ArangoScheduler::startInstance (
    string const& taskId,
    string const& name,
    TaskCurrent const& info,
    mesos::ContainerInfo const& container,
    mesos::CommandInfo const& command) const {

  mesos::SlaveID const& slaveId = info.slave_id();
  mesos::OfferID const& offerId = info.offer_id();
  mesos::Resources const& resources = info.resources();
  string const& offerStr = offerId.value();

  LOG(INFO)
  << "DEBUG startInstance: "
  << "launching task " << name 
  << " using offer " << offerStr
  << " and resources " << resources;

  mesos::TaskInfo task;

  task.set_name(name);
  task.mutable_task_id()->set_value(taskId);
  task.mutable_slave_id()->CopyFrom(slaveId);
  task.mutable_resources()->CopyFrom(resources);
  task.mutable_container()->CopyFrom(container);
  task.mutable_command()->CopyFrom(command);

  mesos::DiscoveryInfo di;
  di.set_visibility(mesos::DiscoveryInfo::CLUSTER);
  di.set_name(name);
  mesos::Ports po;
  auto p = po.add_ports();
  p->set_number(info.ports(0));
  p->set_name("ArangoDB");
  p->set_protocol("tcp");
  di.mutable_ports()->CopyFrom(po);
  task.mutable_discovery()->CopyFrom(di);

  LOG(INFO) << "Discovery visibility CLUSTER";

  // launch the tasks
  vector<mesos::TaskInfo> tasks;
  tasks.push_back(task);

  _driver->launchTasks(offerId, tasks);

  return task;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief kills an instances
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::killInstance (const string& taskId) const {
  LOG(INFO)
  << "INSTANCE kill instance " << taskId;

  mesos::TaskID ti;
  ti.set_value(taskId);

  _driver->killTask(ti);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief posts an request to the master
////////////////////////////////////////////////////////////////////////////////

string ArangoScheduler::postRequest (const string& command,
                                     const string& body) const {
  string url = Global::masterUrl() + command;

  string result;

  long httpCode = 0;
  int res = doHTTPPost(url, body, result, httpCode);
  if (res != 0) {
    LOG(WARNING)
    << "could not perform postRequest, error: " << res
    << ", HTTP result code: " << httpCode;
  }

  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief stops the driver
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::stop () {
  _driver->stop();
}

////////////////////////////////////////////////////////////////////////////////
/// @brief reconciles all tasks
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::reconcileTasks () {
  vector<mesos::TaskStatus> status;
  _driver->reconcileTasks(status);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief reconciles a single task
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::reconcileTask (std::string const& taskId,
                                     std::string const& slaveId) {
  vector<mesos::TaskStatus> status;
  mesos::TaskStatus ts;
  ts.mutable_task_id()->set_value(taskId);
  ts.mutable_slave_id()->set_value(slaveId);
  status.push_back(ts);

  _driver->reconcileTasks(status);
}

// -----------------------------------------------------------------------------
// --SECTION--                                                 Scheduler methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when scheduler has been register
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::registered (mesos::SchedulerDriver* driver,
                                  const mesos::FrameworkID& frameworkId,
                                  const mesos::MasterInfo& master) {
  LOG(INFO)
  << "registered with framework-id " << frameworkId.value()
  << " at master " << master.id();

  {  // mark in state
    auto l = Global::state().lease(true);
    l.state().mutable_framework_id()->CopyFrom(frameworkId);
  }

  checkVersion(master.hostname(), master.port());

  Global::setMasterUrl("http://" + master.hostname() + ":" + to_string(master.port()) + "/");
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when scheduler has been re-register
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::reregistered (mesos::SchedulerDriver* driver,
                                    const mesos::MasterInfo& master) {
  LOG(INFO)
  << "re-registered at new master: " << master.id();

  vector<mesos::TaskStatus> status;
  driver->reconcileTasks(status);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when scheduler has been disconnected
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::disconnected (mesos::SchedulerDriver* driver) {
  LOG(INFO) << "DEBUG Disconnected! Waiting for reconnect.";
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when new resources are available
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::resourceOffers (mesos::SchedulerDriver* driver,
                                      const vector<mesos::Offer>& offers) {
  for (auto& offer : offers) {
#if 0
    LOG(INFO)
    << "DEBUG offer received " << offer.id().value()
    << " with " << offer.resources();
#endif

    if (Global::ignoreOffers() & 1) {
      LOG(INFO) << "Ignoring/declining all offers since the ignoreOffers flag 1 is set.";
      declineOffer(offer.id());
    }
    else {
      Global::manager().addOffer(offer);
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when new resources becomes unavailable
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::offerRescinded (mesos::SchedulerDriver* driver,
                                      const mesos::OfferID& offerId) {
  LOG(INFO)
  << "DEBUG offer rescinded " << offerId.value();

  Global::manager().removeOffer(offerId);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback when task changes
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::statusUpdate (mesos::SchedulerDriver* driver,
                                    const mesos::TaskStatus& status) {
  const string& taskId = status.task_id().value();
  auto state = status.state();
  auto& manager = Global::manager();

  LOG(INFO)
  << "TASK '" << taskId
  << "' is in state " << state
  << " with reason " << status.reason()
  << " from source " << status.source()
  << " with message '" << status.message() << "'";

  manager.taskStatusUpdate(status);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback for messages from executor
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::frameworkMessage (mesos::SchedulerDriver* driver,
                                        const mesos::ExecutorID& executorId,
                                        const mesos::SlaveID& slaveId,
                                        const string& data) {
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback for slave is down
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::slaveLost (mesos::SchedulerDriver* driver,
                                 const mesos::SlaveID& sid) {
  // TODO(fc) what to do?
  LOG(INFO) << "Slave Lost!";
}

////////////////////////////////////////////////////////////////////////////////
/// @brief callback for executor goes down
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::executorLost (mesos::SchedulerDriver* driver,
                                    const mesos::ExecutorID& executorID,
                                    const mesos::SlaveID& slaveID,
                                    int status) {
  // TODO(fc) what to do?
  LOG(INFO) << "Executor Lost!";
}

////////////////////////////////////////////////////////////////////////////////
/// @brief error handling
////////////////////////////////////////////////////////////////////////////////

void ArangoScheduler::error (mesos::SchedulerDriver* driver,
                             const string& message) {
  LOG(ERROR) << "ERROR " << message;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
