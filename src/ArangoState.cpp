///////////////////////////////////////////////////////////////////////////////
/// @brief state of the ArangoDB framework
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

#include "ArangoState.h"

#include "Global.h"
#include "utils.h"

#include "pbjson.hpp"

#include <state/leveldb.hpp>
#include <state/zookeeper.hpp>

#include "logging/logging.hpp"
#include "logging/flags.hpp"

#include <cstdlib>
#include <fstream>
#include <random>
#include <regex>
#include <string>

using namespace arangodb;
using namespace mesos::internal::state;
using namespace std;

// -----------------------------------------------------------------------------
// --SECTION--                                                 class ArangoState
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

ArangoState::ArangoState (const string& name, const string& zk)
  : _name(name),
    _zk(zk),
    _storage(nullptr),
    _stateStore(nullptr),
    _isLeased(false),
    _coordinatorHAProxyList("")
{
  const char* tmpDir = std::getenv("TMPDIR");
  if (tmpDir == nullptr || strlen(tmpDir) == 0) {
    _proxyConfFilename = "/tmp";
  } else {
    _proxyConfFilename = tmpDir;
  }
  
  _proxyConfFilename += "/arango-haproxy.conf";
}

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief initialize storage and state
////////////////////////////////////////////////////////////////////////////////

void ArangoState::init () {
  if (_zk.empty()) {
    string path = "./STATE_" + _name;

    LOG(INFO) << "using leveldb at " << path;

    _storage = new LevelDBStorage(path);
  }
  else {
    string userAndPass  = "(([^/@:]+):([^/@]*)@)";
    string hostAndPort  = "[A-z0-9\\.-]+(:[0-9]+)?";
    string hostAndPorts = "(" + hostAndPort + "(," + hostAndPort + ")*)";
    string zkNode       = "[^/]+(/[^/]+)*";
    string REGEX        = "zk://(" + userAndPass +"?" + hostAndPorts + ")(/" + zkNode + ")";

    regex re(REGEX);
    smatch m;
    bool ok = regex_match(_zk, m, re);

    if (! ok) {
      LOG(ERROR) << "FATAL cannot parse zookeeper '" << _zk << "'";
      exit(EXIT_FAILURE);
    }

    _storage = new ZooKeeperStorage(m[1], Seconds(120), m[9]);
  }

  _stateStore = new mesos::internal::state::State(_storage);

  _state.mutable_targets();
  _state.mutable_targets()->set_mode(Global::modeLC());
  _state.mutable_targets()->set_asynchronous_replication(Global::asyncReplication());
  Target* te;
  te = _state.mutable_targets()->mutable_agents();
  te->set_instances(Global::nrAgents());
  te->set_number_ports(1);
  te = _state.mutable_targets()->mutable_coordinators();
  te->set_instances(Global::nrCoordinators());
  te->set_number_ports(1);
  te = _state.mutable_targets()->mutable_dbservers();
  te->set_instances(Global::nrDBServers());
  te->set_number_ports(1);
  te = _state.mutable_targets()->mutable_secondaries();
  te->set_instances(Global::nrDBServers());
  te->set_number_ports(1);

  _state.mutable_plan();
  _state.mutable_plan()->mutable_agents();
  _state.mutable_plan()->mutable_coordinators();
  _state.mutable_plan()->mutable_dbservers();
  _state.mutable_plan()->mutable_secondaries();

  _state.mutable_current();
  _state.mutable_current()->mutable_agents();
  _state.mutable_current()->mutable_coordinators();
  _state.mutable_current()->mutable_dbservers();
  _state.mutable_current()->mutable_secondaries();

  _state.mutable_current()->set_cluster_complete(false);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief loads the state
////////////////////////////////////////////////////////////////////////////////

void ArangoState::load () {
  lock_guard<mutex> lock(_lock);
  assert(! _isLeased);

  Variable variable = _stateStore->fetch("state_"+_name).get();
  string value = variable.value();

  if (! value.empty()) {
    _state.ParseFromString(value);

    if (_state.targets().mode() != Global::modeLC()) {
      LOG(ERROR)
      << "FATAL stored state is for mode '"
      << _state.targets().mode() << "', "
      << "requested mode is '" << Global::modeLC() << "'";

      exit(EXIT_FAILURE);
    }

    bool stateAsyncRepl 
        = _state.targets().has_asynchronous_replication() &&
          _state.targets().asynchronous_replication();
    if (stateAsyncRepl != Global::asyncReplication()) {
      LOG(ERROR)
      << "FATAL stored state is for asyncReplication flag '"
      << stateAsyncRepl << "', "
      << "requested value is '" << Global::asyncReplication() << "'";

      exit(EXIT_FAILURE);
    }
  }

  LOG(INFO) << "current state: " << arangodb::toJson(_state);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief removes the state from store
////////////////////////////////////////////////////////////////////////////////

void ArangoState::destroy () {
  lock_guard<mutex> lock(_lock);

  Variable variable = _stateStore->fetch("state_"+_name).get();
  auto r = _stateStore->expunge(variable);
  r.await();  // Wait until state is actually expunged
}

////////////////////////////////////////////////////////////////////////////////
/// @brief create a reverse proxy config from our current state
////////////////////////////////////////////////////////////////////////////////

bool ArangoState::createReverseProxyConfig() {
  std::ofstream outfile(_proxyConfFilename);
  if (!outfile.is_open()) {
    LOG(ERROR) << "Couldn't open file " << _proxyConfFilename;
    return false;
  }

  outfile << R"(global
    user root
    group root

    # Stats required for this module to work
    # https://github.com/observing/haproxy#haproxycfg
    stats socket /tmp/haproxy.sock level admin

defaults
    log     global
    mode    http
    option httplog
    timeout connect 5000
    timeout client  50000
    timeout server  50000

    frontend arangodb
        bind *:)" << Global::webuiPort() << R"(
        acl url_framework path_beg /framework/
        use_backend     framework  if  url_framework
        default_backend arangodb
    
    backend framework
        reqrep ^([^\ :]*)\ /framework/(.*)     \1\ /\2
        server framework 127.0.0.1:)" << Global::frameworkPort() << R"(

    backend arangodb
        # i have no idea how to really detect the adminrouter :(
        acl is_adminrouter hdr_reg(x-forwarded-for) .*        
        option forwardfor
        reqadd X-Script-Name:\ /service/)" << Global::frameworkName() << " if is_adminrouter\n" << _coordinatorHAProxyList;

  if (outfile.fail()) {
    LOG(ERROR) << "Couldn't write to " << _proxyConfFilename;
    return false;
  }

  return true;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief find the URL of our own agency
////////////////////////////////////////////////////////////////////////////////

std::string ArangoState::getAgencyURL (ArangoState::Lease& lease) {
  auto const& agents = lease.state().current().agents();
  std::string hostname = agents.entries(0).hostname();
  uint32_t port = agents.entries(0).ports(0);
  return "http://" + hostname + ":" + std::to_string(port) + "/v2/keys/arango";
}

////////////////////////////////////////////////////////////////////////////////
/// @brief find the URL of some coordinator
////////////////////////////////////////////////////////////////////////////////

std::string ArangoState::getCoordinatorURL (ArangoState::Lease& lease) {
  auto const& coordinators = lease.state().current().coordinators();
  auto nr = coordinators.entries_size();
  
  if (nr == 0) {
    return std::string("");
  }
  long now = chrono::duration_cast<chrono::seconds>(
      chrono::steady_clock::now().time_since_epoch()).count();
  std::default_random_engine generator(now);
  std::uniform_int_distribution<int> distribution(0, nr-1);
  int which = distribution(generator);  // generates number of a coordinator
  
  auto coordinator = coordinators.entries(which);
  std::string hostname = coordinator.hostname();
  
  if (coordinator.ports_size() == 0) {
    return std::string("");
  }

  uint32_t port = coordinator.ports(0);
  std::string url("http://" + hostname + ":" + std::to_string(port));

  return url;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief is the cluster healthy?
////////////////////////////////////////////////////////////////////////////////

bool ArangoState::clusterHealthy(Lease& lease) {
  return lease.state().current().cluster_complete();
}


// -----------------------------------------------------------------------------
// --SECTION--                                                   private methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief saves the state
////////////////////////////////////////////////////////////////////////////////

bool ArangoState::save () {
  lock_guard<mutex> lock(_lock);
  assert(_isLeased);

  string value;
  _state.SerializeToString(&value);

#if 0
  string json;
  pbjson::pb2json(&_state, json);
  LOG(INFO) << "State saved: " << json << "\n";
#endif

  Variable variable = _stateStore->fetch("state_"+_name).get();
  variable = variable.mutate(value);
  _stateStore->store(variable);

  std::string backends = "";
  auto const coordinators = _state.current().coordinators().entries();

  int i=0;
  for (auto const& coordinator: coordinators) {
    // mop: not yet ready :S
    if (coordinator.ports().size() == 0) {
      continue;
    }
    std::string serverName = std::to_string(i);
    backends += "        server coordinator" + std::to_string(i) + " " + coordinator.hostname() + ":" + std::to_string(coordinator.ports(0)) + '\n';
    i++;
  }

  if (backends == _coordinatorHAProxyList) {
    return false;
  }

  _coordinatorHAProxyList = backends;
  return true;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
