///////////////////////////////////////////////////////////////////////////////
/// @brief global defines and configuration objects
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

#include "Global.h"

#include "CaretakerStandalone.h"

using namespace arangodb;
using namespace std;

// -----------------------------------------------------------------------------
// --SECTION--                                                 private variables
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief caretaker
////////////////////////////////////////////////////////////////////////////////

static Caretaker* CARETAKER = nullptr;

////////////////////////////////////////////////////////////////////////////////
/// @brief ArangoDB manager
////////////////////////////////////////////////////////////////////////////////

static ArangoManager* MANAGER = nullptr;

////////////////////////////////////////////////////////////////////////////////
/// @brief state
////////////////////////////////////////////////////////////////////////////////

static ArangoState* STATE = nullptr;

////////////////////////////////////////////////////////////////////////////////
/// @brief scheduler
////////////////////////////////////////////////////////////////////////////////

static ArangoScheduler* SCHEDULER = nullptr;

////////////////////////////////////////////////////////////////////////////////
/// @brief mode
////////////////////////////////////////////////////////////////////////////////

static OperationMode MODE = OperationMode::STANDALONE;

////////////////////////////////////////////////////////////////////////////////
/// @brief asynchronous replication flag
////////////////////////////////////////////////////////////////////////////////

static bool ASYNC_REPL = false;

////////////////////////////////////////////////////////////////////////////////
/// @brief debugging flag to ignore all offers
////////////////////////////////////////////////////////////////////////////////

static int  IGNORE_OFFERS = 0;

////////////////////////////////////////////////////////////////////////////////
/// @brief minimal resources for an agent, mesos string specification
////////////////////////////////////////////////////////////////////////////////

static std::string MINRESOURCESAGENT = "mem(*):512; cpus(*):0.25; disk(*):512";

////////////////////////////////////////////////////////////////////////////////
/// @brief minimal resources for a DBServer, mesos string specification
////////////////////////////////////////////////////////////////////////////////

static std::string MINRESOURCESDBSERVER 
  = "mem(*):1024; cpus(*):1; disk(*):1024";

////////////////////////////////////////////////////////////////////////////////
/// @brief minimal resources for a secondary, mesos string specification
////////////////////////////////////////////////////////////////////////////////

static std::string MINRESOURCESSECONDARY
  = "mem(*):1024; cpus(*):1; disk(*):1024";

////////////////////////////////////////////////////////////////////////////////
/// @brief minimal resources for an coordinator, mesos string specification
////////////////////////////////////////////////////////////////////////////////

static std::string MINRESOURCESCOORDINATOR 
  = "mem(*):1024; cpus(*):1; disk(*):1024";

////////////////////////////////////////////////////////////////////////////////
/// @brief initial number of agents
////////////////////////////////////////////////////////////////////////////////

static int NRAGENTS = 1;

////////////////////////////////////////////////////////////////////////////////
/// @brief initial number of DBservers
////////////////////////////////////////////////////////////////////////////////

static int NRDBSERVERS = 2;

////////////////////////////////////////////////////////////////////////////////
/// @brief initial number of coordinators
////////////////////////////////////////////////////////////////////////////////

static int NRCOORDINATORS = 1;

////////////////////////////////////////////////////////////////////////////////
/// @brief role
////////////////////////////////////////////////////////////////////////////////

static string ROLE = "arangodb";

////////////////////////////////////////////////////////////////////////////////
/// @brief frameworkName
////////////////////////////////////////////////////////////////////////////////

static string FRAMEWORK_NAME = "cluster";

////////////////////////////////////////////////////////////////////////////////
/// @brief principal
////////////////////////////////////////////////////////////////////////////////

static string PRINCIPAL = "arangodb";

////////////////////////////////////////////////////////////////////////////////
/// @brief master url
////////////////////////////////////////////////////////////////////////////////

static string MASTER_URL = "http://master.mesos:5050/";

////////////////////////////////////////////////////////////////////////////////
/// @brief volume path
////////////////////////////////////////////////////////////////////////////////

static string VOLUME_PATH = "/tmp";

////////////////////////////////////////////////////////////////////////////////
/// @brief secondaries with dbservers flag
////////////////////////////////////////////////////////////////////////////////

static bool SECONDARIES_WITH_DBSERVERS = false;

////////////////////////////////////////////////////////////////////////////////
/// @brief Coordinators with dbservers flag
////////////////////////////////////////////////////////////////////////////////

static bool COORDINATORS_WITH_DBSERVERS = false;

////////////////////////////////////////////////////////////////////////////////
/// @brief secondary on same agent
////////////////////////////////////////////////////////////////////////////////

static bool SECONDARY_SAME_SERVER = false;

////////////////////////////////////////////////////////////////////////////////
/// @brief docker image name
////////////////////////////////////////////////////////////////////////////////

static string ARANGODB_IMAGE = "arangodb/arangodb-mesos:latest";

static bool ARANGODB_FORCE_PULL_IMAGE = true;

////////////////////////////////////////////////////////////////////////////////
/// @brief run docker image privileged
////////////////////////////////////////////////////////////////////////////////

static bool ARANGODB_PRIVILEGED_IMAGE = false;

////////////////////////////////////////////////////////////////////////////////
/// @brief Framework port
////////////////////////////////////////////////////////////////////////////////

static int ARANGODB_WEBUI_PORT = 8180;

////////////////////////////////////////////////////////////////////////////////
/// @brief Framework port
////////////////////////////////////////////////////////////////////////////////

static int ARANGODB_FRAMEWORK_PORT = 8181;

static std::string ARANGODB_ENTERPRISE_KEY = "";

static std::string ARANGODB_JWT_SECRET = "";

static std::string ARANGODB_SSL_KEYFILE = "";

static std::string ARANGODB_ADDITIONAL_AGENT_ARGS = "";
static std::string ARANGODB_ADDITIONAL_DBSERVER_ARGS = "";
static std::string ARANGODB_ADDITIONAL_SECONDARY_ARGS = "";
static std::string ARANGODB_ADDITIONAL_COORDINATOR_ARGS = "";

// -----------------------------------------------------------------------------
// --SECTION--                                             static public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief caretaker
////////////////////////////////////////////////////////////////////////////////

Caretaker& Global::caretaker () {
  return *CARETAKER;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the caretaker
////////////////////////////////////////////////////////////////////////////////

void Global::setCaretaker (Caretaker* caretaker) {
  CARETAKER = caretaker;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief manager
////////////////////////////////////////////////////////////////////////////////

ArangoManager& Global::manager () {
  return *MANAGER;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the manager
////////////////////////////////////////////////////////////////////////////////

void Global::setManager (ArangoManager* manager) {
  MANAGER = manager;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief state
////////////////////////////////////////////////////////////////////////////////

ArangoState& Global::state () {
  return *STATE;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the state
////////////////////////////////////////////////////////////////////////////////

void Global::setState (ArangoState* state) {
  STATE = state;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief scheduler
////////////////////////////////////////////////////////////////////////////////

ArangoScheduler& Global::scheduler () {
  return *SCHEDULER;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the scheduler
////////////////////////////////////////////////////////////////////////////////

void Global::setScheduler (ArangoScheduler* scheduler) {
  SCHEDULER = scheduler;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief minimal resources for an agent, mesos string specification
////////////////////////////////////////////////////////////////////////////////

std::string Global::minResourcesAgent () {
  return MINRESOURCESAGENT;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the minimal resources for an agent
////////////////////////////////////////////////////////////////////////////////

void Global::setMinResourcesAgent (std::string val) {
  MINRESOURCESAGENT = val;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief minimal resources for a DBserver, mesos string specification
////////////////////////////////////////////////////////////////////////////////

std::string Global::minResourcesDBServer () {
  return MINRESOURCESDBSERVER;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the minimal resources for a DBserver, mesos string spec
////////////////////////////////////////////////////////////////////////////////

void Global::setMinResourcesDBServer (std::string val) {
  MINRESOURCESDBSERVER = val;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief minimal resources for a secondary, mesos string specification
////////////////////////////////////////////////////////////////////////////////

std::string Global::minResourcesSecondary () {
  return MINRESOURCESSECONDARY;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the minimal resources for a DBserver, mesos string spec
////////////////////////////////////////////////////////////////////////////////

void Global::setMinResourcesSecondary (std::string val) {
  MINRESOURCESSECONDARY = val;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief minimal resources for a coordinator, mesos string specification
////////////////////////////////////////////////////////////////////////////////

std::string Global::minResourcesCoordinator () {
  return MINRESOURCESCOORDINATOR;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the minimal resources for a coordinator
////////////////////////////////////////////////////////////////////////////////

void Global::setMinResourcesCoordinator (std::string val) {
  MINRESOURCESCOORDINATOR = val;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief initial number of agents
////////////////////////////////////////////////////////////////////////////////

int Global::nrAgents () {
  return NRAGENTS;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the initial number of agents
////////////////////////////////////////////////////////////////////////////////

void Global::setNrAgents (int val) {
  NRAGENTS = val;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief initial number of DBservers
////////////////////////////////////////////////////////////////////////////////

int Global::nrDBServers () {
  return NRDBSERVERS;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the initial number of DBservers
////////////////////////////////////////////////////////////////////////////////

void Global::setNrDBServers (int val) {
  NRDBSERVERS = val;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief initial number of coordinators
////////////////////////////////////////////////////////////////////////////////

int Global::nrCoordinators () {
  return NRCOORDINATORS;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the initial number of coordinators
////////////////////////////////////////////////////////////////////////////////

void Global::setNrCoordinators (int val) {
  NRCOORDINATORS = val;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief mode
////////////////////////////////////////////////////////////////////////////////

OperationMode Global::mode () {
  return MODE;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief mode in lower case
////////////////////////////////////////////////////////////////////////////////

string Global::modeLC () {
  switch (Global::mode()) {
    case OperationMode::STANDALONE:
      return "standalone";
      break;

    case OperationMode::CLUSTER:
      return "cluster";
      break;
  }

  return "unknown";
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the mode
////////////////////////////////////////////////////////////////////////////////

void Global::setMode (OperationMode mode) {
  MODE = mode;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief asynchronous replication flag
////////////////////////////////////////////////////////////////////////////////

bool Global::asyncReplication () {
  return ASYNC_REPL;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the asynchronous replication flag
////////////////////////////////////////////////////////////////////////////////

void Global::setAsyncReplication (bool flag) {
  ASYNC_REPL = flag;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief debugging flags to ignore some offers
////////////////////////////////////////////////////////////////////////////////

int Global::ignoreOffers () {
  return IGNORE_OFFERS;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief debugging flag to ignore some offers
////////////////////////////////////////////////////////////////////////////////

void Global::setIgnoreOffers (int flags) {
  IGNORE_OFFERS = flags;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief role
////////////////////////////////////////////////////////////////////////////////

string Global::role () {
  return ROLE;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the role
////////////////////////////////////////////////////////////////////////////////

void Global::setRole (const std::string& role) {
  ROLE = role;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief frameworkName
////////////////////////////////////////////////////////////////////////////////

string Global::frameworkName () {
  return FRAMEWORK_NAME;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the frameworkName
////////////////////////////////////////////////////////////////////////////////

void Global::setFrameworkName (const std::string& frameworkName) {
  FRAMEWORK_NAME = frameworkName;
}

std::string Global::principal() {
  return PRINCIPAL;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief create a reservation
////////////////////////////////////////////////////////////////////////////////

mesos::Resource::ReservationInfo Global::createReservation () {
  mesos::Resource::ReservationInfo reservation;
  reservation.set_principal(PRINCIPAL);

  return reservation;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the principal
////////////////////////////////////////////////////////////////////////////////

void Global::setPrincipal (const std::string& principal) {
  PRINCIPAL = principal;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief master url
////////////////////////////////////////////////////////////////////////////////

const string& Global::masterUrl () {
  return MASTER_URL;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the master url
////////////////////////////////////////////////////////////////////////////////

void Global::setMasterUrl (const std::string& url) {
  MASTER_URL = url;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief volume path
////////////////////////////////////////////////////////////////////////////////

const std::string& Global::volumePath () {
  return VOLUME_PATH;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief sets the volume path
////////////////////////////////////////////////////////////////////////////////

void Global::setVolumePath (const std::string& path) {
  VOLUME_PATH = path;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief secondaries with dbservers flag
////////////////////////////////////////////////////////////////////////////////

bool Global::secondariesWithDBservers () {
  return SECONDARIES_WITH_DBSERVERS;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief secondaries with dbservers flag
////////////////////////////////////////////////////////////////////////////////

void Global::setSecondariesWithDBservers (bool f) {
  SECONDARIES_WITH_DBSERVERS = f;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief coordinators with dbservers flag
////////////////////////////////////////////////////////////////////////////////

bool Global::coordinatorsWithDBservers () {
  return COORDINATORS_WITH_DBSERVERS;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief coordinators with dbservers flag
////////////////////////////////////////////////////////////////////////////////

void Global::setCoordinatorsWithDBservers (bool f) {
  COORDINATORS_WITH_DBSERVERS = f;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief secondary on same agent
////////////////////////////////////////////////////////////////////////////////

bool Global::secondarySameServer () {
  return SECONDARY_SAME_SERVER;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief secondary on same agent
////////////////////////////////////////////////////////////////////////////////

void Global::setSecondarySameServer (bool f) {
  SECONDARY_SAME_SERVER = f;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief docker image to use
////////////////////////////////////////////////////////////////////////////////

const string& Global::arangoDBImage () {
  return ARANGODB_IMAGE;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief docker image to use
////////////////////////////////////////////////////////////////////////////////

void Global::setArangoDBImage (const std::string& val) {
  ARANGODB_IMAGE = val;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief force pull the docker image
////////////////////////////////////////////////////////////////////////////////

bool Global::arangoDBForcePullImage() {
  return ARANGODB_FORCE_PULL_IMAGE;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief run docker image privileged
////////////////////////////////////////////////////////////////////////////////

void Global::setArangoDBForcePullImage(bool f) {
  ARANGODB_FORCE_PULL_IMAGE = f;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief run docker image privileged
////////////////////////////////////////////////////////////////////////////////

bool Global::arangoDBPrivilegedImage() {
  return ARANGODB_PRIVILEGED_IMAGE;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief run docker image privileged
////////////////////////////////////////////////////////////////////////////////

void Global::setArangoDBPrivilegedImage(bool f) {
  ARANGODB_PRIVILEGED_IMAGE = f;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief get framework port
////////////////////////////////////////////////////////////////////////////////

int Global::frameworkPort() {
  return ARANGODB_FRAMEWORK_PORT;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief set framework port
////////////////////////////////////////////////////////////////////////////////

void Global::setFrameworkPort(int port) {
  ARANGODB_FRAMEWORK_PORT = port;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief get webui port
////////////////////////////////////////////////////////////////////////////////

int Global::webuiPort() {
  return ARANGODB_WEBUI_PORT;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief set webui port
////////////////////////////////////////////////////////////////////////////////

void Global::setWebuiPort(int port) {
  ARANGODB_WEBUI_PORT = port;
}

bool Global::startReverseProxy() {
  char const*  confFile = Global::state().getProxyConfFilename();
  int currentPid = Global::state().getProxyPid();
  Global::state().setRestartProxy(RESTART_KEEP_RUNNING);
  pid_t pid = fork();
  if (pid == -1) {
    return false;
  } else if (pid == 0) {
    int ret;
    if (currentPid > 0) {
      std::string pidString = std::to_string(currentPid);
      ret = execl("/usr/sbin/haproxy", "haproxy", "-f", confFile, "-sf", pidString.c_str(), (char*) 0);
    } else {
      ret = execl("/usr/sbin/haproxy", "haproxy", "-f", confFile, (char*) 0);
    }
    exit(0);
  } else {
    Global::state().setProxyPid(pid);
    return true;
  }
}

void Global::setArangoDBEnterpriseKey(std::string const& arangoDBEnterpriseKey) {
  ARANGODB_ENTERPRISE_KEY = arangoDBEnterpriseKey;
}

std::string Global::arangoDBEnterpriseKey() {
  return ARANGODB_ENTERPRISE_KEY;
}

void Global::setArangoDBJwtSecret(std::string const& arangoDBJwtSecret) {
  ARANGODB_JWT_SECRET = arangoDBJwtSecret;
}

std::string Global::arangoDBJwtSecret() {
  return ARANGODB_JWT_SECRET;
}

void Global::setArangoDBSslKeyfile(std::string const& sslKeyfile) {
  ARANGODB_SSL_KEYFILE = sslKeyfile;
}

std::string Global::arangoDBSslKeyfile() {
  return ARANGODB_SSL_KEYFILE;
}

void Global::setArangoDBAdditionalAgentArgs(std::string const& additionalAgentArgs) {
  ARANGODB_ADDITIONAL_AGENT_ARGS = additionalAgentArgs;
}

std::string Global::arangoDBAdditionalAgentArgs() {
  return ARANGODB_ADDITIONAL_AGENT_ARGS;
}

void Global::setArangoDBAdditionalDBServerArgs(std::string const& additionalDBServerArgs) {
  ARANGODB_ADDITIONAL_DBSERVER_ARGS = additionalDBServerArgs;
}

std::string Global::arangoDBAdditionalDBServerArgs() {
  return ARANGODB_ADDITIONAL_DBSERVER_ARGS;
}

void Global::setArangoDBAdditionalSecondaryArgs(std::string const& additionalSecondaryArgs) {
  ARANGODB_ADDITIONAL_SECONDARY_ARGS = additionalSecondaryArgs;
}

std::string Global::arangoDBAdditionalSecondaryArgs() {
  return ARANGODB_ADDITIONAL_SECONDARY_ARGS;
}

void Global::setArangoDBAdditionalCoordinatorArgs(std::string const& additionalCoordinatorArgs) {
  ARANGODB_ADDITIONAL_COORDINATOR_ARGS = additionalCoordinatorArgs;
}

std::string Global::arangoDBAdditionalCoordinatorArgs() {
  return ARANGODB_ADDITIONAL_COORDINATOR_ARGS;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
