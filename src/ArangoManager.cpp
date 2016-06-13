//////////////////////////////////////////////////////////////////////////////
/// @brief manager for the ArangoDB framework
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

#include "ArangoManager.h"

#include "ArangoScheduler.h"
#include "ArangoState.h"
#include "Global.h"
#include "utils.h"

#include "pbjson.hpp"

#include <stout/uuid.hpp>

#include <iostream>
#include <set>
#include <unordered_set>
#include <random>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <chrono>
#include <thread>

using namespace arangodb;
using namespace std;


// -----------------------------------------------------------------------------
// --SECTION--                                               class ArangoManager
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

ArangoManager::ArangoManager ()
  : _stopDispatcher(false),
    _dispatcher(nullptr),
    _nextImplicitReconciliation(chrono::steady_clock::now()),
    _implicitReconciliationIntervall(chrono::minutes(5)),
    _maxReconcileIntervall(chrono::minutes(5)),
    _task2position(),
    _lock(),
    _storedOffers(),
    _taskStatusUpdates() {

  _dispatcher = new thread(&ArangoManager::dispatch, this);

  auto lease = Global::state().lease(false);
  Current current = lease.state().current();

  fillKnownInstances(TaskType::AGENT, current.agents());
  fillKnownInstances(TaskType::COORDINATOR, current.coordinators());
  fillKnownInstances(TaskType::PRIMARY_DBSERVER, current.dbservers());
  fillKnownInstances(TaskType::SECONDARY_DBSERVER, current.secondaries());
}

////////////////////////////////////////////////////////////////////////////////
/// @brief destructor
////////////////////////////////////////////////////////////////////////////////

ArangoManager::~ArangoManager () {
  _stopDispatcher = true;
  _dispatcher->join();

  delete _dispatcher;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief adds an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::addOffer (const mesos::Offer& offer) {
  lock_guard<mutex> lock(_lock);

#if 0
  // This is already logged in the scheduler in more concise format.
  {
    LOG(INFO) << "OFFER received: " << arangodb::toJson(offer);
  }
#endif

  _storedOffers[offer.id().value()] = offer;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief removes an offer
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::removeOffer (const mesos::OfferID& offerId) {
  lock_guard<mutex> lock(_lock);

  string id = offerId.value();

  LOG(INFO) << "OFFER removed: " << id;
  
  _storedOffers.erase(id);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief status update
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::taskStatusUpdate (const mesos::TaskStatus& status) {
  lock_guard<mutex> lock(_lock);

  _taskStatusUpdates.push_back(status);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief destroys the cluster and shuts down the scheduler
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::destroy () {
  LOG(INFO) << "destroy() called, killing off everything...";

  {
    // First set the target state to 0 instances:
    auto l = Global::state().lease(true);
    Targets* target = l.state().mutable_targets();
    target->mutable_agents()->set_instances(0);
    target->mutable_coordinators()->set_instances(0);
    target->mutable_dbservers()->set_instances(0);
    target->mutable_secondaries()->set_instances(0);

    LOG(INFO) << "The old state with DEAD tasks:CURRENT:\n"
              << arangodb::toJson(l.state().current());

    // Now set the state of all instances to TASK_STATE_DEAD:
    std::vector<std::string> ids;
    Plan* plan = l.state().mutable_plan();
    Current* current = l.state().mutable_current();

    auto markAllDead = [&] (TasksPlan* entries, TasksCurrent const& currs) 
                       -> void {
      for (int i = 0; i < entries->entries_size(); i++) {
        TaskPlan* entry = entries->mutable_entries(i);
        if (entry->state() != TASK_STATE_DEAD) {
          LOG(INFO) << "Planning to kill instance with id '"
                    << currs.entries(i).task_info().task_id().value()
                    << "'";
          ids.push_back(currs.entries(i).task_info().task_id().value());
          entry->set_state(TASK_STATE_DEAD);
        }
      }
    };

    markAllDead(plan->mutable_agents(), current->agents());
    markAllDead(plan->mutable_dbservers(), current->dbservers());
    markAllDead(plan->mutable_secondaries(), current->secondaries());
    markAllDead(plan->mutable_coordinators(), current->coordinators());

    LOG(INFO) << "The new state with DEAD tasks:\nPLAN:"
              << arangodb::toJson(l.state().plan());

    killAllInstances(ids);
  }

  // During the following time we will get KILL messages, this will keep
  // the status and as a consequences we will destroy all persistent volumes,
  // unreserve all reserved resources and decline the offers:
  this_thread::sleep_for(chrono::seconds(30));

  string body;
  {
    auto l = Global::state().lease();
    body = "frameworkId=" + l.state().framework_id().value();
  }

  // Now everything should be down, so terminate for good:
  Global::state().destroy();

  Global::scheduler().stop();

  Global::scheduler().postRequest("master/shutdown", body);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief endpoints of the coordinators
////////////////////////////////////////////////////////////////////////////////

vector<string> ArangoManager::coordinatorEndpoints () {
  auto l = Global::state().lease();
  Current current = l.state().current();
  auto const& coordinators = current.coordinators();

  vector<string> endpoints;

  for (int i = 0;  i < coordinators.entries_size();  ++i) {
    auto const& coordinator = coordinators.entries(i);
    if (coordinator.has_hostname() && coordinator.ports_size() > 0) {
      string endpoint = "http://" + coordinator.hostname() + ":" 
                        + to_string(coordinator.ports(0));
      endpoints.push_back(endpoint);
    }
  }

  return endpoints;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief endpoints of the DBservers
////////////////////////////////////////////////////////////////////////////////

vector<string> ArangoManager::dbserverEndpoints () {
  auto l = Global::state().lease();
  Current current = l.state().current();
  auto const& dbservers = current.dbservers();

  vector<string> endpoints;

  for (int i = 0; i < dbservers.entries_size(); ++i) {
    auto const& dbserver = dbservers.entries(i);
    if (dbserver.has_hostname() && dbserver.ports_size() > 0) {
      string endpoint = "http://" + dbserver.hostname() + ":" 
                        + to_string(dbserver.ports(0));
      endpoints.push_back(endpoint);
    }
  }

  return endpoints;
}

std::vector<std::string> ArangoManager::updateTarget() {
  std::vector<std::string> cleanedServers = {};

  std::string coordinatorURL;
  {
    auto lease = Global::state().lease();

    coordinatorURL = Global::state().getCoordinatorURL(lease);
  }
  if (coordinatorURL.empty()) {
    return cleanedServers;
  }

  std::string body;
  long httpCode = 0;
  int res = doHTTPGet(coordinatorURL + "/_admin/cluster/numberOfServers", body, httpCode);

  if (res != 0 || httpCode != 200) {
    LOG(ERROR) << "Couldn't retrieve cluster targets. HTTP Code: " << httpCode;
    return cleanedServers;
  }
    
  picojson::value value;
  std::string err = picojson::parse(value, body);
  
  if (!err.empty()) {
    LOG(WARNING) << "Couldn't parse json(cluster targets): " << err << ". Body was: " << body;
    return cleanedServers;
  }
  
  if (!value.is<picojson::object>()) {
    LOG(WARNING) << "Root result is not an object for cluster targets. Body was: " << body;
    return cleanedServers;
  }

  auto propertyPairs = {
    std::make_pair("numberOfDBServers", &Global::setNrDBServers),
    std::make_pair("numberOfCoordinators", &Global::setNrCoordinators),
  };

  auto cleanedServersValue = value.get("cleanedServers");
  
  if (cleanedServersValue.is<picojson::array>()) {
    auto arr = cleanedServersValue.get<picojson::array>();
    
    for (picojson::array::iterator it = arr.begin(); it != arr.end(); ++it) {
      if (it->is<std::string>()) {
        if (it->is<std::string>()) {
          cleanedServers.push_back(it->get<std::string>());
        }
      }
    }
  }

  {
    auto lease = Global::state().lease();
    for (auto const& it: propertyPairs) {
      auto nrValue = value.get(it.first);

      if (!nrValue.is<double>()) {
        LOG(ERROR) << it.first << " in cluster target is not a number but " << nrValue.to_str();
        continue;
      }
      (*it.second)(std::round(nrValue.get<double>()));
    }
  }
  Global::caretaker().updateTarget();

  return cleanedServers;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                   private methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief main dispatcher
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::dispatch () {
  static const int SLEEP_SEC = 2;
  //static const int SLEEP_SEC = 1;

  prepareReconciliation();

  while (! _stopDispatcher) {
    bool found;
    {
      auto l = Global::state().lease();
      found = l.state().has_framework_id();
    }

    if (! found) {
      this_thread::sleep_for(chrono::seconds(SLEEP_SEC));
      continue;
    }



    std::vector<std::string> cleanedServers = updateTarget();

    // start reconciliation
    reconcileTasks();

    // apply received status updates
    applyStatusUpdates(cleanedServers);

    updateServerIds();
    
    updatePlan(cleanedServers);
    
    // check all outstanding offers
    checkOutstandOffers();

    // apply any timeouts
    bool sleep = checkTimeouts();

    // wait for a little while, if we are idle
    if (sleep) {
      this_thread::sleep_for(chrono::seconds(2));
      {
        lock_guard<mutex> lock(_lock);
        if (! _storedOffers.empty()) {
          sleep = false;
        }
      }
      if (sleep) {
        this_thread::sleep_for(chrono::seconds(SLEEP_SEC));
      }
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief fill in TaskStatus
////////////////////////////////////////////////////////////////////////////////

static void fillTaskStatus (vector<pair<string,string>>& result,
                            TasksPlan const& plans,
                            TasksCurrent const& currents) {

  // we have to check the TaskInfo (not TaskStatus!)
  for (int i = 0;  i < currents.entries_size();  ++i) {
    TaskPlan const& planEntry = plans.entries(i);
    TaskCurrent const& entry = currents.entries(i);

    switch (planEntry.state()) {
      case TASK_STATE_NEW:
      case TASK_STATE_TRYING_TO_RESERVE:
      case TASK_STATE_TRYING_TO_PERSIST:
      case TASK_STATE_TRYING_TO_START:
      case TASK_STATE_TRYING_TO_RESTART:
      case TASK_STATE_RUNNING:
      case TASK_STATE_KILLED:
      case TASK_STATE_FAILED_OVER:
        // At this stage we do not distinguish the state, is this sensible?
        if (entry.has_task_info()) {
          auto const& info = entry.task_info();
          string taskId = info.task_id().value();
          string slaveId = info.slave_id().value();
          result.push_back(make_pair(taskId, slaveId));
        }

        break;
      case TASK_STATE_DEAD:
        break;
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief prepares the reconciliation of tasks
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::prepareReconciliation () {
  auto l = Global::state().lease();
  vector<pair<string,string>> taskSlaveIds;

  fillTaskStatus(taskSlaveIds, l.state().plan().agents(),
                               l.state().current().agents());
  fillTaskStatus(taskSlaveIds, l.state().plan().coordinators(), 
                               l.state().current().coordinators());
  fillTaskStatus(taskSlaveIds, l.state().plan().dbservers(),
                               l.state().current().dbservers());
  fillTaskStatus(taskSlaveIds, l.state().plan().secondaries(),
                               l.state().current().secondaries());

  auto now = chrono::steady_clock::now();

  for (auto const& taskSlaveId : taskSlaveIds) {
    auto nextReconcile = now;
    auto backoff = chrono::seconds(1);

    ReconcileTasks reconcile = {
      taskSlaveId.first,   // TaskId
      taskSlaveId.second,  // SlaveId
      nextReconcile,
      backoff
    };

    _reconciliationTasks[taskSlaveId.first] = reconcile;
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief tries to recover tasks
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::reconcileTasks () {

  // see http://mesos.apache.org/documentation/latest/reconciliation/
  // for details about reconciliation

  auto now = chrono::steady_clock::now();

  // first, we ask for implicit reconciliation periodically
  if (_nextImplicitReconciliation >= now) {
    LOG(INFO) << "DEBUG implicit reconciliation";

    Global::scheduler().reconcileTasks();
    _nextImplicitReconciliation = now + _implicitReconciliationIntervall;
  }

  // check for unknown tasks
  for (auto& task : _reconciliationTasks) {
    if (task.second._nextReconcile > now) {
      LOG(INFO) << "DEBUG explicit reconciliation for " << task.first;

      Global::scheduler().reconcileTask(task.second._taskId,
                                        task.second._slaveId);

      task.second._backoff *= 2;

      if (task.second._backoff >= _maxReconcileIntervall) {
        task.second._backoff = _maxReconcileIntervall;
      }

      task.second._nextReconcile = now + task.second._backoff;
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks for timeout
////////////////////////////////////////////////////////////////////////////////

static double const TryingToReserveTimeout  = 30;  // seconds
static double const TryingToPersistTimeout  = 30;
static double const TryingToStartTimeout    = 600; // docker pull might take
static double const TryingToShutdownTimeout = 120; // forcefully kill task after wait

/* mop: time mesos will allow ourselves to restart properly. this must be set
 * to a high value. When this timeout is exceeded mesos will declare the whole
 * framework lost and it will kill all associated tasks!
 */
static double const FailoverTimeout        = 60;

// Note that the failover timeout can be configured by the user via a
// command line option.

// For debugging purposes:
#if 0
static double const TryingToReserveTimeout = 30;  // seconds
static double const TryingToPersistTimeout = 30;
static double const TryingToStartTimeout   = 30; // docker pull might take
static double const TryingToRestartTimeout = 30;
static double const FailoverTimeout        = 86400;
static double const TryingToResurrectTimeout = 30;  // Patience before we
                                                    // give up on a persistent
                                                    // task.
#endif

static bool getServerId(TaskCurrent* task, std::string& server_id) {
  std::string body;
  long httpCode = 0;
  int res = doHTTPGet("http://" + task->hostname() + ":" + to_string(task->ports(0)) 
                      + "/_admin/server/id", body, httpCode);

  if (res != 0 || httpCode != 200) {
    LOG(ERROR) << "Couldn't retrieve server id. HTTP Code: " << httpCode;
    return false;
  }
    
  picojson::value s;
  std::string err = picojson::parse(s, body);

  if (!err.empty()) {
    LOG(WARNING) << "Couldn't parse json: " << err << ". Body was: " << body;
    return false;
  }

  if (!s.is<picojson::object>()) {
    LOG(WARNING) << "Root result is not an object. Body was: " << body;
    return false;
  }
  
  auto& id = s.get("id");

  if (!id.is<string>()) {
    LOG(WARNING) << "Id is not a string. Body was: " << body;
  }

  server_id = id.get<string>();
  return true;
}

bool ArangoManager::registerNewSecondary(ArangoState::Lease& lease, std::string const& primaryId) {
  Plan* plan = lease.state().mutable_plan();

  for (int i=0;i<plan->mutable_dbservers()->entries_size();i++) {
    if (plan->mutable_dbservers()->mutable_entries(i)->server_id() == primaryId) {
      return registerNewSecondary(lease, plan->mutable_dbservers()->mutable_entries(i));
    }
  }
  LOG(ERROR) << "Couldn't find primary " << primaryId;
  return false;
}

bool ArangoManager::registerNewSecondary(ArangoState::Lease& lease, TaskPlan* primary) {
  if (primary->server_id().empty()) {
    LOG(WARNING) << "Server ID from " << primary->name() << " not yet known. Can't register secondary yet";
    return false;
  }

  Plan* plan = lease.state().mutable_plan();
  TasksPlan* tasksPlanSecondary = plan->mutable_secondaries();
  std::string secondaryName = "Secondary"
    + std::to_string(tasksPlanSecondary->entries_size() + 1);
  
  // to the new one:
  std::string previousSecondaryName = "none";
  if (!primary->sync_partner().empty()) {
    previousSecondaryName = primary->sync_partner();
  }

  // Still needed: Tell the agency about this change and
  // configure the new server there:
  std::string resultBody;
  std::string coordinatorURL = Global::state().getCoordinatorURL(lease);

  if (coordinatorURL.empty()) {
    LOG(WARNING) << "Couldn't register secondary. There is no coordinator to talk to right now";
    return false;
  }

  long httpCode = 0;
  int res = 0;
  auto logError = [&] (std::string msg) -> void {
    LOG(ERROR) << "Problems with reconfiguring agency (secondary "
      << "of primary " << primary->server_id() << " from "
      << previousSecondaryName << " to new " << secondaryName
      << ")\n" << msg
      << ", libcurl error code: " << res
      << ", HTTP result code: " << httpCode
      << ", retrying...";
  };

  std::string body 
    =   R"({"primary":")" + primary->server_id() + R"(",)"
    + R"("oldSecondary":")" + previousSecondaryName + R"(",)"
    + R"("newSecondary":")" + secondaryName + R"("})";

  res = arangodb::doHTTPPut(coordinatorURL +
      "/_admin/cluster/replaceSecondary",
      body, resultBody, httpCode);

  if (res != 0 || httpCode != 200) {
    logError(resultBody);
    return false;
  }
  LOG(INFO) << "Successfully reconfigured agency (secondary "
    << "of primary " << primary->server_id() << " from "
    << previousSecondaryName << " to new " << secondaryName << ")";
  
  // mop: we successfully told the agency about our future secondary server
  // now update our taskplan
  primary->set_sync_partner(secondaryName);
  Current* current = lease.state().mutable_current();

  TasksCurrent* tasksCurrentSecondary = current->mutable_secondaries();

  double now = chrono::duration_cast<chrono::seconds>(
      chrono::steady_clock::now().time_since_epoch()).count();


  // Now create a new secondary:
  TaskPlan* tpnew = tasksPlanSecondary->add_entries();
  tpnew->set_state(TASK_STATE_NEW);
  tpnew->set_name(secondaryName);
  tpnew->set_sync_partner(primary->server_id());
  tpnew->set_timestamp(now);

  // mop: by convention: keep size in sync 
  tasksCurrentSecondary->add_entries();

  return true;
}

void ArangoManager::updateServerIds() {
  auto l = Global::state().lease();

  std::vector<TaskType> types 
    = { TaskType::PRIMARY_DBSERVER, TaskType::SECONDARY_DBSERVER, TaskType::COORDINATOR };

  auto* plan = l.state().mutable_plan();
  auto* current = l.state().mutable_current();

  for (auto taskType : types) {
    TasksPlan* tasksPlan;
    TasksCurrent* tasksCurr;
    switch (taskType) {
      case TaskType::COORDINATOR:
        tasksPlan = plan->mutable_coordinators();
        tasksCurr = current->mutable_coordinators();
        break;
      case TaskType::PRIMARY_DBSERVER:
        tasksPlan = plan->mutable_dbservers();
        tasksCurr = current->mutable_dbservers();
        break;
      case TaskType::SECONDARY_DBSERVER:
        tasksPlan = plan->mutable_secondaries();
        tasksCurr = current->mutable_secondaries();
        break;
      case TaskType::AGENT:
        continue;
    }
    for (int i = 0; i < tasksPlan->entries_size(); i++) {
      auto tp = tasksPlan->mutable_entries(i);
      auto tc = tasksCurr->mutable_entries(i);
      if (tp->state() == TASK_STATE_RUNNING) {
        if (tp->server_id().empty()) {
          std::string id;

          if (getServerId(tc, id)) {
            tp->set_server_id(id);
            l.changed();
          }
        }
      }
    }
  }
}

bool ArangoManager::checkTimeouts () {
  auto l = Global::state().lease();

  std::vector<TaskType> types 
    = { TaskType::AGENT, TaskType::PRIMARY_DBSERVER,
        TaskType::SECONDARY_DBSERVER, TaskType::COORDINATOR };

  auto* plan = l.state().mutable_plan();
  auto* current = l.state().mutable_current();

  for (auto taskType : types) {
    TasksPlan* tasksPlan;
    TasksCurrent* tasksCurr;
    switch (taskType) {
      case TaskType::AGENT:
        tasksPlan = plan->mutable_agents();
        tasksCurr = current->mutable_agents();
        break;
      case TaskType::PRIMARY_DBSERVER:
        tasksPlan = plan->mutable_dbservers();
        tasksCurr = current->mutable_dbservers();
        break;
      case TaskType::SECONDARY_DBSERVER:
        tasksPlan = plan->mutable_secondaries();
        tasksCurr = current->mutable_secondaries();
        break;
      case TaskType::COORDINATOR:
        tasksPlan = plan->mutable_coordinators();
        tasksCurr = current->mutable_coordinators();
        break;
      default:  // never happens
        tasksPlan = nullptr;
        tasksCurr = nullptr;
        break;
    }
    double now = chrono::duration_cast<chrono::seconds>(
      chrono::steady_clock::now().time_since_epoch()).count();
    double timeStamp;
    for (int i = 0; i < tasksPlan->entries_size(); i++) {
      TaskPlan* tp = tasksPlan->mutable_entries(i);
      TaskCurrent* ic = tasksCurr->mutable_entries(i);
      switch (tp->state()) {
        case TASK_STATE_NEW:
          // Wait forever here, no timeout.
          break;
        case TASK_STATE_TRYING_TO_RESERVE:
          // After a timeout, go back to state TASK_STATE_NEW, because
          // there was no satisfactory answer to our reservation request.
          // Resources will be freed automatically.
          timeStamp = tp->timestamp();
          if (now - timeStamp > TryingToReserveTimeout) {
            LOG(INFO) << "Timeout " << TryingToReserveTimeout << "s reached "
                      << " for task " << ic->task_info().name()
                      << " in state TASK_STATE_TRYING_TO_RESERVE.";
            LOG(INFO) << "Going back to state TASK_STATE_NEW.";
            tp->set_state(TASK_STATE_NEW);
            tp->clear_persistence_id();
            tp->set_timestamp(now);
            l.changed();
          }
          break;
        case TASK_STATE_TRYING_TO_PERSIST:
          // After a timeout, go back to state TASK_STATE_NEW, because
          // there was no satisfactory answer to our persistence request.
          // Resources will be freed automatically.
          timeStamp = tp->timestamp();
          if (now - timeStamp > TryingToPersistTimeout) {
            LOG(INFO) << "Timeout " << TryingToPersistTimeout << "s reached "
                      << " for task " << ic->task_info().name()
                      << " in state TASK_STATE_TRYING_TO_PERSIST.";
            LOG(INFO) << "Going back to state TASK_STATE_NEW.";
            tp->set_state(TASK_STATE_NEW);
            tp->clear_persistence_id();
            tp->set_timestamp(now);
            l.changed();
          }
          break;
        case TASK_STATE_TRYING_TO_START:
          // After a timeout, go back to state TASK_STATE_NEW, because
          // there was no satisfactory answer to our start request.
          // Resources will be freed automatically.
          timeStamp = tp->timestamp();
          if (now - timeStamp > TryingToStartTimeout) {
            LOG(INFO) << "Timeout " << TryingToPersistTimeout << "s reached "
                      << " for task " << ic->task_info().name()
                      << " in state TASK_STATE_TRYING_TO_START.";
            LOG(INFO) << "Going back to state TASK_STATE_NEW.";
            tp->set_state(TASK_STATE_NEW);
            tp->clear_persistence_id();
            tp->set_timestamp(now);
            l.changed();
          }
          break;
        case TASK_STATE_RUNNING:
          // Run forever here, no timeout.
          break;
        case TASK_STATE_FAILED_OVER:
          // This task has been replaced by its failover partner, now we
          // finally lose patience to wait for a restart and give up on the
          // task. This can only happen to a primary DBserver that has been
          // interchanged with its secondary and is now a secondary.
          // We want to get rid of this task and replace it with a new one.
          // This is exactly what we would have done if this server was a
          // secondary that has failed in the first place. Therefore we
          // fall through here and let the case for TASK_STATE_KILLED
          // take care of the rest.
        case TASK_STATE_KILLED:
          // After some time being killed, we have to take action and
          // engage in some automatic failover procedure:
          timeStamp = tp->timestamp();
          if (now - timeStamp > FailoverTimeout) {
            LOG(INFO) << "Timeout " << FailoverTimeout << "s reached "
                      << " for task " << ic->task_info().name()
                      << " in state TASK_STATE_KILLED.";
            if (taskType == TaskType::AGENT) {
              // ignore timeout, keep trying, otherwise we are lost
              LOG(INFO) << "Task is an agent, simply reset the timestamp and "
                        << "wait forever.";
              tp->set_timestamp(now);
              l.changed();
            }
            else if (taskType == TaskType::COORDINATOR) {
              // simply go back to TASK_STATE_NEW to start another one
              // There were no reservations and persistent volumes,
              // so Mesos will clean up behind ourselves.
              LOG(INFO) << "Going back to state TASK_STATE_NEW.";
              tp->set_state(TASK_STATE_NEW);
              tp->clear_persistence_id();
              tp->set_timestamp(now);
              l.changed();
            }
            else if (taskType == TaskType::SECONDARY_DBSERVER) {
              // find corresponding primary (partner)
              // make new secondary, change primary's secondary entry in
              // our state and in the registry, declare old secondary dead
              std::string primaryName = tp->sync_partner();
              // Give up on this one:
              tp->set_state(TASK_STATE_DEAD);
              tp->set_timestamp(now);
              tp->clear_persistence_id();
              tp->clear_sync_partner();
              ic->clear_slave_id();
              ic->clear_offer_id();
              ic->clear_resources();
              ic->clear_ports();
              ic->clear_hostname();
              ic->clear_container_path();
              ic->clear_task_info();

              registerNewSecondary(l, primaryName);
              l.changed();
            }
            else if (taskType == TaskType::PRIMARY_DBSERVER) {
              // interchange plan and current infos, update task2position
              // map, promote secondary to primary in state and agency,
              // make old primary the secondary of the old secondary,
              // set state of old primary to TASK_STATE_FAILED_OVER
              if (! tp->has_sync_partner()) {
                // We cannot do much here, so let's keep waiting...
                LOG(INFO) << "Task is a dbserver without a replica, simply "
                          << "reset the timestamp and wait forever.";
                tp->set_timestamp(now);
                l.changed();
              }
              else {
                std::string secondaryName = tp->sync_partner();

                // Find the corresponding secondary:
                TasksPlan* tasksPlanSecondary = plan->mutable_secondaries();
                TaskPlan* tpsecond;
                bool found = false;
                int j;
                for (j = 0; j < tasksPlanSecondary->entries_size(); j++) {
                  tpsecond = tasksPlanSecondary->mutable_entries(j);
                  if (tpsecond->name() == secondaryName) {
                    found = true;
                    break;
                  }
                }
                if (! found) {
                  LOG(INFO) << "Did not find replica of dbserver task, simply "
                            << "reset the timestamp and wait forever.";
                  tp->set_timestamp(now);
                  l.changed();
                }
                else {
                  // Now interchange the information on primary[i] and
                  // secondary[j]:
                  TaskPlan dummy;
                  dummy.CopyFrom(*tpsecond);
                  tpsecond->CopyFrom(*tp);
                  tp->CopyFrom(dummy);
                  TaskCurrent dummy2;
                  TaskCurrent* tpsecondcur
                      = current->mutable_secondaries()->mutable_entries(j);
                  dummy2.CopyFrom(*tpsecondcur);
                  tpsecondcur->CopyFrom(*ic);
                  ic->CopyFrom(dummy2);
                  
                  tp->set_timestamp(now);
                  tpsecond->set_timestamp(now);

                  // Set the new state of the failed task to 
                  // TASK_STATE_FAILED_OVER:
                  tpsecond->set_state(TASK_STATE_FAILED_OVER);

                  // Now update _task2position (note that ic and tpsecondcur
                  // have been interchanged by now!):
                  _task2position[ic->task_info().task_id().value()] 
                      = std::make_pair(TaskType::PRIMARY_DBSERVER, i);
                  _task2position[tpsecondcur->task_info().task_id().value()]
                      = std::make_pair(TaskType::SECONDARY_DBSERVER, j);

                  std::string resultBody;

                  // We try to reconfigure until the agency has answered...
                  while (true) {
                  // Still needed: Tell the agency about this change.
                    std::string coordinatorURL 
                        = Global::state().getCoordinatorURL(l);

                    if (coordinatorURL.empty()) {
                      LOG(WARNING) << "No active coordinator found";
                      this_thread::sleep_for(chrono::seconds(1));
                      continue;
                    }

                    long httpCode = 0;
                    int res = 0;
                    auto logError = [&] (std::string msg) -> void {
                      LOG(ERROR) << "Problems with reconfiguring agency "
                                 << "(switching primary " 
                                 << tpsecond->name()
                                 << " and secondary " << tp->name()
                                 << ")\n" << msg
                                 << ", libcurl error code: " << res
                                 << ", HTTP result code: " << httpCode
                                 << ", retrying...";
                      this_thread::sleep_for(chrono::seconds(2));
                    };

                    std::string body 
                      =   R"({"primary":")" + tpsecond->name() + R"(",)"
                        + R"("secondary":")" + tp->name() + R"("})";
                  
                    res = arangodb::doHTTPPut(std::string(coordinatorURL) +
                          "/_admin/cluster/swapPrimaryAndSecondary",
                          body, resultBody, httpCode);

                    if (res != 0 || httpCode != 200) {
                      logError(resultBody);
                      continue;
                    }
                    // All OK, let's get out of here:
                    break;
                  }

                  l.changed();

                  LOG(INFO) << "Successfully reconfigured agency "
                            << "(switching primary " << tpsecond->name()
                            << " and secondary " << tp->name() << ")";
                }
              }
            }
          }
          break;
        case TASK_STATE_TRYING_TO_RESTART:
          // We got the offer for a restart, but the restart is not happening.
          // We need to go back to state TASK_STATE_KILLED to wait for another
          // offer.
          timeStamp = tp->timestamp();
          if (now - timeStamp > TryingToStartTimeout) {
            LOG(INFO) << "Timeout " << TryingToPersistTimeout << "s reached "
                      << " for task " << ic->task_info().name()
                      << " in state TASK_STATE_TRYING_TO_RESTART.";
            LOG(INFO) << "Going back to state TASK_STATE_KILL.";
            tp->set_state(TASK_STATE_KILLED);
            l.changed();
            // Do not change the time stamp here, because we want to
            // notice alternating between KILLED and TRYING_TO_RESTART!
          }
          break;
        case TASK_STATE_DEAD:
          // This task is no longer used. Do nothing.
          break;
        case TASK_STATE_SHUTTING_DOWN:
          timeStamp = tp->timestamp();
          if (now - timeStamp > TryingToShutdownTimeout) {
            LOG(INFO) << "Timeout " << TryingToShutdownTimeout << " shutting down task " << ic->task_info().name() << " reached";
            Global::scheduler().killInstance(ic->task_info().task_id().value());
          }
          break;
      }
    }
  }
  return true;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief applies status updates
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::applyStatusUpdates (std::vector<std::string>& cleanedServers) {
  Caretaker& caretaker = Global::caretaker();
  
  lock_guard<mutex> lock(_lock);
  typedef std::unordered_set<int> DeleteMap;

  DeleteMap agentPosToDelete;
  DeleteMap coordinatorPosToDelete;
  DeleteMap dbServerPosToDelete;
  DeleteMap secondaryPosToDelete;

  auto lease = Global::state().lease(true);
  bool deleted = false;
  for (auto&& status : _taskStatusUpdates) {
    mesos::TaskID taskId = status.task_id();
    string taskIdStr = taskId.value();

    _reconciliationTasks.erase(taskIdStr);

    std::pair<TaskType, int>& pos = _task2position[taskIdStr];

    switch (status.state()) {
      case mesos::TASK_STAGING:
        break;

      case mesos::TASK_RUNNING: {
        caretaker.setTaskPlanState(lease, pos.first, pos.second,
                                   TASK_STATE_RUNNING, deleted);
        break;
      }
      case mesos::TASK_STARTING:
        // do nothing
        break;

      case mesos::TASK_FINISHED: // TERMINAL. The task finished successfully.
      case mesos::TASK_FAILED:   // TERMINAL. The task failed to finish successfully.
      case mesos::TASK_KILLED:   // TERMINAL. The task was killed by the executor.
      case mesos::TASK_LOST:     // TERMINAL. The task failed but can be rescheduled.
      case mesos::TASK_ERROR: {  // TERMINAL. The task failed but can be rescheduled.
        caretaker.setTaskPlanState(lease, pos.first, pos.second,
                                   TASK_STATE_KILLED, deleted);
        if (deleted) {
          switch(pos.first) {
            case TaskType::AGENT: {
                agentPosToDelete.insert(pos.second);
                break;
              }
            case TaskType::PRIMARY_DBSERVER: {
                dbServerPosToDelete.insert(pos.second);
                break;
              }
            case TaskType::SECONDARY_DBSERVER: {
                secondaryPosToDelete.insert(pos.second);
                break;
              }
            case TaskType::COORDINATOR: {
                coordinatorPosToDelete.insert(pos.second);
                break;
              }
          }
        }
        break;
      }
    }
  }
  
  Plan* plan = lease.state().mutable_plan();
  Current* current = lease.state().mutable_current();
  auto toDo = {
    std::make_tuple(agentPosToDelete, plan->mutable_agents(), current->mutable_agents()),
    std::make_tuple(dbServerPosToDelete, plan->mutable_dbservers(), current->mutable_dbservers()),
    std::make_tuple(secondaryPosToDelete, plan->mutable_secondaries(), current->mutable_secondaries()),
    std::make_tuple(coordinatorPosToDelete, plan->mutable_coordinators(), current->mutable_coordinators()),
  };
  
  bool changed = false;
  for (auto const& tup: toDo) {
    auto const& toDelete = std::get<0>(tup);
    TasksCurrent* current = std::get<2>(tup);
    if (toDelete.size() > 0) {
      changed = true;

      TasksPlan* plan = std::get<1>(tup);

      TasksPlan originalPlan;
      originalPlan.CopyFrom(*plan);
      TasksCurrent originalCurrent;
      originalCurrent.CopyFrom(*current);
      
      plan->clear_entries();
      current->clear_entries();
      for (int i=0;i<originalPlan.entries_size();i++) {
        auto got = toDelete.find(i);
        TaskPlan planEntry = originalPlan.entries(i);
        // mop: re-add if not marked for deletion
        if (got == toDelete.end()) {
          plan->add_entries()->CopyFrom(planEntry);
          
          TaskCurrent currentEntry = originalCurrent.entries(i);
          current->add_entries()->CopyFrom(currentEntry);
        } else {
          if (planEntry.has_server_id()) {
            for (auto it=cleanedServers.begin();it!=cleanedServers.end();++it) {
              if (*it == planEntry.server_id()) {
                cleanedServers.erase(it);
                break;
              }
            }
          }
          LOG(INFO) << "Deleting " << originalCurrent.entries(i).task_info().name();
        }
      }
    }
  }

  if (changed) {
    // mop: this is suboptimal but the framework will be rewritten soon :S
    _task2position.clear();
    fillKnownInstances(TaskType::AGENT, current->agents());
    fillKnownInstances(TaskType::COORDINATOR, current->coordinators());
    fillKnownInstances(TaskType::PRIMARY_DBSERVER, current->dbservers());
    fillKnownInstances(TaskType::SECONDARY_DBSERVER, current->secondaries());
    
    std::vector<picojson::value> cleanedServersJsonValues;
    std::transform(cleanedServers.begin(), cleanedServers.end(), std::back_inserter(cleanedServersJsonValues), [](std::string const& cleanedServer) {
      return picojson::value(cleanedServer);
    });
    picojson::value cleanedServersJson = picojson::value(cleanedServersJsonValues);

    std::string body = "{\"cleanedServers\": " + cleanedServersJson.serialize() + "}";
    std::string resultBody; 
    long httpCode = 0;
    std::string coordinatorURL = Global::state().getCoordinatorURL(lease);
    int res = arangodb::doHTTPPut(coordinatorURL +
      "/_admin/cluster/numberOfServers",
      body, resultBody, httpCode);

    if (httpCode>=200 && httpCode<300) {
      LOG(INFO) << "Successfully reset cleaned servers";
    } else {
      LOG(WARNING) << "Failed resetting cleaned servers. Statuscode " << httpCode << ", Body: " << resultBody;
    }
    
    lease.changed();
  }

  _taskStatusUpdates.clear();
}

void ArangoManager::updatePlan(std::vector<std::string> const& cleanedServers) {
  Caretaker& caretaker = Global::caretaker();
  
  // ...........................................................................
  // first of all, update our plan
  // ...........................................................................

  caretaker.updatePlan(cleanedServers);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief checks available offers
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::checkOutstandOffers () {
  Caretaker& caretaker = Global::caretaker();

  // ...........................................................................
  // check all stored offers
  // ...........................................................................

  {
    lock_guard<mutex> lock(_lock);

    for (auto&& id_offer : _storedOffers) {
      caretaker.checkOffer(id_offer.second);
    }

    _storedOffers.clear();
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief recover task mapping
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::fillKnownInstances (TaskType type,
                                        TasksCurrent const& currents) {
  LOG(INFO)
  << "recovering instance type " << (int) type;

  for (int i = 0;  i < currents.entries_size();  ++i) {
    TaskCurrent const& entry = currents.entries(i);

    if (entry.has_task_info()) {
      string id = entry.task_info().task_id().value();

      LOG(INFO)
      << "for task id " << id << ": " << i;

      _task2position[id] = std::make_pair(type, i);
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief kills all running tasks
////////////////////////////////////////////////////////////////////////////////

void ArangoManager::killAllInstances (std::vector<std::string>& ids) {
  for (auto const& id : ids) {
    Global::scheduler().killInstance(id);
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
