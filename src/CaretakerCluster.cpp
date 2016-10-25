///////////////////////////////////////////////////////////////////////////////
/// @brief cluster caretaker
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

#include "CaretakerCluster.h"

#include "ArangoState.h"
#include "ArangoManager.h"
#include "Global.h"
#include "ArangoScheduler.h"

#include "mesos/resources.hpp"
#include "arangodb.pb.h"
#include "pbjson.hpp"
#include "utils.h"

using namespace arangodb;

// -----------------------------------------------------------------------------
// --SECTION--                                                   class Caretaker
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

CaretakerCluster::CaretakerCluster () {
  {
    auto lease = Global::state().lease();
    Targets* targets = lease.state().mutable_targets();

    // AGENCY
    Target* agency = targets->mutable_agents();

    agency->clear_minimal_resources();
    agency->set_number_ports(1);

    if (Global::minResourcesAgent().empty()) {
      setStandardMinimum(agency, 0);
    }
    else {
      Try<mesos::Resources> x
        = mesos::Resources::parse(Global::minResourcesAgent());
      if (x.isError()) {
        LOG(ERROR) << "cannot parse minimum resources for agent:\n  '"
          << Global::minResourcesAgent() << "'";
        setStandardMinimum(agency, 0);
      }
      else {
        mesos::Resources res = x.get().flatten();   // always flatten to role "*"
        auto m = agency->mutable_minimal_resources();
        m->CopyFrom(res);
      }
    }

    // COORDINATOR
    Target* coordinator = targets->mutable_coordinators();
    coordinator->clear_minimal_resources();
    coordinator->set_number_ports(1);
    if (Global::minResourcesCoordinator().empty()) {
      setStandardMinimum(coordinator, 1);
    }
    else {
      Try<mesos::Resources> x
        = mesos::Resources::parse(Global::minResourcesCoordinator());
      if (x.isError()) {
        LOG(ERROR) << "cannot parse minimum resources for coordinator:\n  '"
          << Global::minResourcesCoordinator() << "'";
        setStandardMinimum(coordinator, 1);
      }
      else {
        mesos::Resources res = x.get().flatten();   // always flatten to role "*"
        auto m = coordinator->mutable_minimal_resources();
        m->CopyFrom(res);
      }
    }

    // DBSERVER
    Target* dbserver = targets->mutable_dbservers();
    dbserver->clear_minimal_resources();
    dbserver->set_number_ports(1);
    if (Global::minResourcesDBServer().empty()) {
      setStandardMinimum(dbserver, 1);
    }
    else {
      Try<mesos::Resources> x
        = mesos::Resources::parse(Global::minResourcesDBServer());
      if (x.isError()) {
        LOG(ERROR) << "cannot parse minimum resources for DBServer:\n  '"
          << Global::minResourcesDBServer() << "'";
        setStandardMinimum(dbserver, 1);
      }
      else {
        mesos::Resources res = x.get().flatten();   // always flatten to role "*"
        auto m = dbserver->mutable_minimal_resources();
        m->CopyFrom(res);
      }
    }

    // SECONDARIES
    Target* secondary = targets->mutable_secondaries();
    secondary->clear_minimal_resources();
    secondary->set_number_ports(1);
    if (Global::minResourcesSecondary().empty()) {
      setStandardMinimum(secondary, 1);
    }
    else {
      Try<mesos::Resources> x
        = mesos::Resources::parse(Global::minResourcesSecondary());
      if (x.isError()) {
        LOG(ERROR) << "cannot parse minimum resources for Secondary:\n  '"
          << Global::minResourcesSecondary() << "'";
        setStandardMinimum(secondary, 1);
      }
      else {
        mesos::Resources res = x.get().flatten();   // always flatten to role "*"
        auto m = secondary->mutable_minimal_resources();
        m->CopyFrom(res);
      }
    }
  }
  updateTarget();
}

// -----------------------------------------------------------------------------
// --SECTION--                                               some static helpers
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief count the number of planned instances of a certain kind
////////////////////////////////////////////////////////////////////////////////

static int countPlannedInstances (TasksPlan const& plans) {
  int plannedInstances = 0;

  for (int i = 0; i < plans.entries_size(); i++) {
    TaskPlan const& entry = plans.entries(i);

    if (entry.state() != TASK_STATE_DEAD && entry.state() != TASK_STATE_SHUTTING_DOWN) {
      plannedInstances += 1;
    }
  }

  return plannedInstances;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief count the number of running instances of a certain kind
////////////////////////////////////////////////////////////////////////////////

static int countRunningInstances (TasksPlan const& plans) {
  int runningInstances = 0;

  for (int i = 0; i < plans.entries_size(); i++) {
    TaskPlan const& entry = plans.entries(i);

    if (entry.state() == TASK_STATE_RUNNING ||
        entry.state() == TASK_STATE_TRYING_TO_START ||
        entry.state() == TASK_STATE_TRYING_TO_RESTART) {
      runningInstances += 1;
    }
  }

  return runningInstances;
}

// -----------------------------------------------------------------------------
// --SECTION--                                            virtual public methods
// -----------------------------------------------------------------------------

int CaretakerCluster::removeNewTasks(TasksPlan* tasksPlan, TasksCurrent* tasksCurrent, int toRemove) {
  std::vector<int> toDeleteIndices = {};
  // mop: first find new tasks which have not yet been started...they can simply be deleted
  for (int i=tasksPlan->entries_size() - 1;i>=0 && toDeleteIndices.size() < toRemove;i--) {
    auto task = tasksPlan->mutable_entries(i);

    if (task->state() == TASK_STATE_NEW) {
      toDeleteIndices.push_back(i);
    }
  }

  if (toDeleteIndices.size() > 0) {
    TasksPlan oldPlan;
    TasksCurrent oldCurrent;
    oldPlan.CopyFrom(*tasksPlan);
    oldCurrent.CopyFrom(*tasksCurrent);

    tasksPlan->clear_entries();
    tasksCurrent->clear_entries();
    for (size_t i=0;i<oldPlan.entries_size();i++) {
      if (std::find(std::begin(toDeleteIndices), std::end(toDeleteIndices), i) == std::end(toDeleteIndices)) {
        auto taskPlan = oldPlan.entries(i);
        tasksPlan->add_entries()->CopyFrom(taskPlan);
        auto taskCurrent = oldCurrent.entries(i);
        tasksCurrent->add_entries()->CopyFrom(taskCurrent);
      }
    }
  }

  return toDeleteIndices.size();
}

////////////////////////////////////////////////////////////////////////////////
/// {@inheritDoc}
////////////////////////////////////////////////////////////////////////////////

void CaretakerCluster::updatePlan (std::vector<std::string> const& cleanedServers) {
  // This updates the Plan according to what is in the Target, this is
  // used to scale up coordinators or DBServers

  auto lease = Global::state().lease(true);

  Targets* targets = lease.state().mutable_targets();
  Plan* plan = lease.state().mutable_plan();
  Current* current = lease.state().mutable_current();
  int t, p;

  t = (int) targets->agents().instances();
  TasksPlan* tasks = plan->mutable_agents();
  p = countPlannedInstances(plan->agents());
  if (t < p) {
    LOG(INFO)
    << "INFO reducing number of agents is not supported";
    targets->mutable_agents()->set_instances(p);
  }
  if (p < t) {
    LOG(INFO)
    << "DEBUG creating " << (t - p) << " more agents in plan";

    for (int i = p; i < t; ++i) {
      TaskPlan* task = tasks->add_entries();
      task->set_state(TASK_STATE_NEW);

      std::string name = "Agent" 
                         + std::to_string(tasks->entries_size());
      task->set_name(name);

      TasksCurrent* agents = current->mutable_agents();
      agents->add_entries();
    }
  }

  // need at least one DB server
  tasks = plan->mutable_dbservers();

  for (auto const& serverId: cleanedServers) {
    for (int i=0;i<tasks->entries_size();i++) {
      auto planDbServer = tasks->mutable_entries(i);
      auto const& currentDbServer = current->dbservers().entries(i);
      if (planDbServer->state() != TASK_STATE_SHUTTING_DOWN && planDbServer->server_id() == serverId) {
        shutdownSecondary(lease, planDbServer);
        shutdownServer(planDbServer, currentDbServer);
      }
    }
  }
  t = (int) targets->dbservers().instances();
  p = countPlannedInstances(plan->dbservers());

  if (t < p) {
    LOG(INFO)
    << "DEBUG reducing dbservers by " << (p - t) << " in plan";
    auto tasksCurrent = current->mutable_dbservers();
    // mop: try to kill dbservers which have not yet been started
    removeNewTasks(tasks, tasksCurrent, p - t);
    // mop: if there are still more dbservers than planned the supervision is
    // supposed to clean out some existing dbservers....we remain helpless
    // here :)
  }
  if (p < t) {
    LOG(INFO)
    << "DEBUG creating " << (t - p) << " more db-servers in plan";

    for (int i = p;  i < t;  ++i) {
      TaskPlan* task = tasks->add_entries();
      task->set_state(TASK_STATE_NEW);
      std::string name = "DBServer"
                         + std::to_string(tasks->entries_size());
      task->set_name(name);

      // mop: by convention Current and Plan arrays have to have equal size
      TasksCurrent* dbservers = current->mutable_dbservers();
      dbservers->add_entries();
    }
  }

  // need at least one coordinator
  t = (int) targets->coordinators().instances();
  tasks = plan->mutable_coordinators();
  p = countPlannedInstances(plan->coordinators());

  if (t < p) {
    LOG(INFO)
    << "INFO reducing the number of coordinators from " << p << " to " << t;

    int toShutdown = p - t;

    double now = chrono::duration_cast<chrono::seconds>(
      chrono::steady_clock::now().time_since_epoch()).count();

    auto tasksCurrent = current->mutable_coordinators();
    // mop: first remove "hanging" tasks which are trying to start right now
    toShutdown -= removeNewTasks(tasks, tasksCurrent, toShutdown);
    int shuttingDown = 0;
    // mop: finally if still necessary kill the newest tasks
    for (int i=tasks->entries_size() - 1;i>=0 && shuttingDown < toShutdown;i--) {
      auto planCoordinator = tasks->mutable_entries(i);
      auto const& currentCoordinator = tasksCurrent->entries(i);

      if (planCoordinator->has_server_id()) {
        if (shutdownServer(planCoordinator, currentCoordinator)) {
          shuttingDown++;
        }
      }
    }
  }

  if (p < t) {
    LOG(INFO)
    << "DEBUG creating " << (t - p) << " more coordinators in plan";

    for (int i = p; i < t; ++i) {
      TaskPlan* task = tasks->add_entries();
      task->set_state(TASK_STATE_NEW);
      std::string name = "Coordinator" 
                         + std::to_string(tasks->entries_size());
      task->set_name(name);

      TasksCurrent* coordinators = current->mutable_coordinators();
      coordinators->add_entries();
    }
  }
}

void CaretakerCluster::shutdownSecondary(ArangoState::Lease& lease, TaskPlan* dbserver) {
  if (!dbserver->has_sync_partner()) {
    return;
  }

  Plan* plan = lease.state().mutable_plan();
  
  TasksPlan* secondaries = plan->mutable_secondaries();
  TaskPlan* foundPlan = nullptr;
  TaskCurrent foundCurrent;
  for (int i=0;i<secondaries->entries_size();i++) {
    TaskPlan* secondary = secondaries->mutable_entries(i);
    if (secondary->state() != TASK_STATE_SHUTTING_DOWN && secondary->server_id() == dbserver->sync_partner()) {
      foundPlan = secondary;
      foundCurrent = lease.state().current().secondaries().entries(i);
      break;
    }
  }

  if (foundPlan != nullptr) {
    std::string coordinatorURL = Global::state().getCoordinatorURL(lease);
    std::string body 
      =   R"({"primary":")" + dbserver->server_id() + R"(",)"
      + R"("oldSecondary":")" + foundPlan->server_id() + R"(",)"
      + R"("newSecondary":"none"})";

    std::string resultBody;
    long httpCode = 0;
    int res = 0;
    res = arangodb::doClusterHTTPPut(coordinatorURL +
        "/_admin/cluster/replaceSecondary",
        body, resultBody, httpCode);

    if (res != 0 || httpCode != 200) {
      LOG(INFO) << "Couldn't unregister secondary " << httpCode << ", " << resultBody;
    } else {
      LOG(INFO) << "Successfully reconfigured agency (secondary "
        << "of primary " << dbserver->server_id() << " from "
        << foundPlan->server_id() << " to new \"none\"";
    }
    shutdownServer(foundPlan, foundCurrent);
  }
}

bool CaretakerCluster::shutdownServer(TaskPlan* taskPlan, TaskCurrent const& taskCurrent) {
  if (taskCurrent.has_hostname() && taskCurrent.ports_size() > 0) {
    string endpoint = "http://" + taskCurrent.hostname() + ":" 
      + to_string(taskCurrent.ports(0));

    std::string body;
    long httpCode = 0;
    LOG(INFO) << "Shutting down " << taskPlan->server_id();

    double now = chrono::duration_cast<chrono::seconds>(
        chrono::steady_clock::now().time_since_epoch()).count();
    doClusterHTTPDelete(endpoint + "/_admin/shutdown?remove_from_cluster=1", body, httpCode);

    if (httpCode >= 200 && httpCode < 300) {
      taskPlan->set_state(TASK_STATE_SHUTTING_DOWN);
      taskPlan->set_timestamp(now);
      return true;
    }
  }

  return false;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief check an incoming offer against a certain kind of server
////////////////////////////////////////////////////////////////////////////////

void CaretakerCluster::checkOffer (const mesos::Offer& offer) {
  // We proceed as follows:
  //   If not all agencies are up and running, then we check whether
  //   this offer is good for an agency.
  //   If all agencies are running, we ask the first one if it is initialized
  //   properly. If not, we wait and decline the offer.
  //   Otherwise, if not all DBservers are up and running, we check first
  //   whether this offer is good for one of them.
  //   If all DBservers are up, and we use asynchronous replication,
  //   we check whether all secondaries are up, lastly, we check with 
  //   the coordinators. If all is well, we decline politely.

  auto lease = Global::state().lease();

  Targets* targets = lease.state().mutable_targets();
  Plan* plan = lease.state().mutable_plan();
  Current* current = lease.state().mutable_current();

  bool offerUsed = false;

#if 0
  // Further debugging output:
  LOG(INFO) 
  << "checkOffer, here is the state:\n"
  << "TARGETS:" << Global::state().jsonTargets() << "\n"
  << "PLAN:"   << Global::state().jsonPlan() << "\n"
  << "CURRENT:"<< Global::state().jsonCurrent() << "\n";
#endif

  std::string offerString;
  pbjson::pb2json(&offer, offerString);

  LOG(INFO)
  << "And here the offer:\n" << offerString << "\n";

  int plannedInstances = countPlannedInstances(plan->agents());
  int runningInstances = countRunningInstances(plan->agents());
  if (runningInstances < plannedInstances) {
    LOG(INFO)
    << "planned agent instances: " << plannedInstances << ", "
    << "running agent instances: " << runningInstances;
    // Try to use the offer for a new agent:
    offerUsed = checkOfferOneType(lease, "agency", true,
                                  targets->agents(),
                                  plan->mutable_agents(),
                                  current->mutable_agents(),
                                  offer, ! current->cluster_complete(),
                                  TaskType::AGENT);

    if (offerUsed) {
      lease.changed();   // save new state
      return;
    }

    // Otherwise, fall through to give other task types a chance to look
    // at the offer. This only happens when the cluster is already
    // initialized.
  }

  // Now look after the DBservers:
  plannedInstances = countPlannedInstances(plan->dbservers());
  runningInstances = countRunningInstances(plan->dbservers());
  if (runningInstances < plannedInstances) {
    LOG(INFO)
    << "planned DBServer instances: " << plannedInstances << ", "
    << "running DBServer instances: " << runningInstances;
    // Try to use the offer for a new DBserver:
    offerUsed = checkOfferOneType(lease, "primary", true,
                                  targets->dbservers(),
                                  plan->mutable_dbservers(),
                                  current->mutable_dbservers(),
                                  offer, ! current->cluster_complete(),
                                  TaskType::PRIMARY_DBSERVER);

    if (offerUsed) {
      lease.changed();  // make sure new state is saved
      return;
    }
    // Otherwise, fall through to give other task types a chance to look
    // at the offer. This only happens when the cluster is already
    // initialized.
  }
  
  // Create coordinators:
  plannedInstances = countPlannedInstances(plan->coordinators());
  runningInstances = countRunningInstances(plan->coordinators());
  if (runningInstances < plannedInstances) {
    LOG(INFO)
    << "planned coordinator instances: " << plannedInstances << ", "
    << "running coordinator instances: " << runningInstances;
    // Try to use the offer for a new coordinator:
    if (checkOfferOneType(lease, "coordinator", false,
                          targets->coordinators(),
                          plan->mutable_coordinators(),
                          current->mutable_coordinators(),
                          offer, ! current->cluster_complete(),
                          TaskType::COORDINATOR)) {
      lease.changed();  // make sure that the new state is saved
      return;   // if we have used or declined the offer, we will not 
                // want to run cluster init and will no longer have to
                // return reservations or persistent volumes
    }
  }

  // Now the secondaries, if needed. Must be done after coordinators have been started
  // as they register themselves via coordinators
  if (Global::asyncReplication()) {
    // mop: the number of planned secondaries equals the number of currently running
    // dbservers
    plannedInstances = countRunningInstances(plan->dbservers());
    runningInstances = countRunningInstances(plan->secondaries());
    if (runningInstances < plannedInstances) {
      LOG(INFO)
      << "planned secondary DBServer instances: " << plannedInstances << ", "
      << "running secondary DBServer instances: " << runningInstances;
      
      for (int i=0;i<plan->mutable_dbservers()->entries_size();i++) {
        if (!plan->mutable_dbservers()->mutable_entries(i)->has_sync_partner()) {
          
          if (!Global::manager().registerNewSecondary(lease, plan->mutable_dbservers()->mutable_entries(i))) {
            // mop: reconfiguration failed...decline and retry on next offer
            Global::scheduler().declineOffer(offer.id());
            return;
          }
        }
      }

      // Try to use the offer for a new DBserver:
      offerUsed = checkOfferOneType(lease, "secondary", true,
                                    targets->secondaries(),
                                    plan->mutable_secondaries(),
                                    current->mutable_secondaries(),
                                    offer, ! current->cluster_complete(),
                                    TaskType::SECONDARY_DBSERVER);

      if (offerUsed) {
        lease.changed();  // make sure new state is saved
        return;
      }
      // Otherwise, fall through to give other task types a chance to look
      // at the offer. This only happens when the cluster is already
      // initialized.
    }
  }


  // plannedInstances is 0 if and only if we have shut down the cluster,
  // if this happened before the cluster was complete, there would be 
  // chaos.
  if (plannedInstances > 0 && 
      runningInstances == plannedInstances &&
      ! current->cluster_complete()) {
    std::stringstream sin;
    sin << "{\"numberOfCoordinators\":" << targets->coordinators().instances() << ",\"numberOfDBServers\":" << targets->dbservers().instances() << "}";

    std::string body = sin.str();
    std::string resultBody; 
    long httpCode = 0;
    std::string coordinatorURL = Global::state().getCoordinatorURL(lease);
    int res = arangodb::doClusterHTTPPut(coordinatorURL +
      "/_admin/cluster/numberOfServers",
      body, resultBody, httpCode);

    if (httpCode>=200 && httpCode<300) {
      LOG(INFO) << "Successfully set the current target in the agency";
      LOG(INFO) << "Cluster is complete.";
      current->set_cluster_complete(true);
      lease.changed();
    } else {
      LOG(WARNING) << "Failed setting current target in the agency. Statuscode " << httpCode << ", Body: " << resultBody;
    }
  }

  // Nobody wanted this offer, see whether there is a persistent disk
  // in there and destroy it:
  mesos::Resources offered = offer.resources();
  mesos::Resources offeredDisk = filterIsDisk(offered);
  mesos::Resources toDestroy;
  for (auto& res : offeredDisk) {
    if (res.role() == Global::role() &&
        res.has_disk() &&
        res.disk().has_persistence() &&
        res.has_reservation() &&
        res.reservation().principal() == Global::principal()) {
      toDestroy += res;
    }
  }
  if (! toDestroy.empty()) {
    std::string offerString;
    pbjson::pb2json(&offer, offerString);
    LOG(INFO) << "Found a persistent disk(s) that nobody wants, "
      << "will destroy:" << toDestroy
      << " Original offer:" << offerString;
    Global::scheduler().destroyPersistent(offer, toDestroy);
    return;
  }

  // If there was no persistent disk, maybe there is a dynamic reservation,
  // if so, unreserve it:
  mesos::Resources toUnreserve;
  for (auto& res : offered) {
    if (res.role() == Global::role() &&
        res.has_reservation() &&
        res.reservation().principal() == Global::principal()) {
      toUnreserve += res;
    }
  }
  if (! toUnreserve.empty()) {
    std::string offerString;
    pbjson::pb2json(&offer, offerString);
    LOG(INFO) << "Found dynamically reserved resources that nobody wants, "
              << "will unreserve:" << toUnreserve
              << " Original offer:" << offerString;
    Global::scheduler().unreserveDynamically(offer, toUnreserve);
    return;
  }

  // All is good, simply decline offer:
  if (! current->cluster_complete()) {
    LOG(INFO) << "Declining offer " << offer.id().value();
  }
  Global::scheduler().declineOffer(offer.id());
}

void CaretakerCluster::updateTarget() {
  bool changed = false;

  auto lease = Global::state().lease();
  Targets* targets = lease.state().mutable_targets();

  // AGENCY
  Target* agency = targets->mutable_agents();

  if (agency->instances() != Global::nrAgents()) {
    agency->set_instances(Global::nrAgents());
    changed = true;
  }

  // COORDINATOR
  Target* coordinator = targets->mutable_coordinators();
  if (coordinator->instances() != Global::nrCoordinators()) {
    coordinator->set_instances(Global::nrCoordinators());
    changed = true;
  }
    
  // DBSERVER
  Target* dbserver = targets->mutable_dbservers();
  if (dbserver->instances() != Global::nrDBServers()) {
    dbserver->set_instances(Global::nrDBServers());
    changed = true;
  }
  
  // SECONDARIES
  Target* secondary = targets->mutable_secondaries();
  if (secondary->instances() != Global::nrDBServers()) { 
    secondary->set_instances(Global::nrDBServers());
    changed = true;
  }

  if (changed) {
    lease.changed();
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
