////////////////////////////////////////////////////////////////////////////////
/// @brief ArangoDB Mesos Framework
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

#ifdef unix
#undef unix
#endif
#include <libgen.h>

#include <csignal>
#include <iostream>
#include <stdlib.h>
#include <string>
#include <unistd.h>

#include "ArangoManager.h"
#include "ArangoScheduler.h"
#include "ArangoState.h"
#include "CaretakerStandalone.h"
#include "CaretakerCluster.h"
#include "Global.h"
#include "HttpServer.h"

#include <stout/check.hpp>
#include <stout/exit.hpp>
#include <stout/flags.hpp>
#include <stout/numify.hpp>
#include <stout/os.hpp>
#include <stout/stringify.hpp>
#include <stout/net.hpp>

#include "logging/flags.hpp"
#include "logging/logging.hpp"

using namespace std;
using namespace mesos::internal;
using namespace arangodb;

// -----------------------------------------------------------------------------
// --SECTION--                                                 private functions
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief update from env
////////////////////////////////////////////////////////////////////////////////
static void updateFromEnv (const string& name, string& var) {
  Option<string> env = os::getenv(name);

  if (env.isSome()) {
    var = env.get();
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief update from env
////////////////////////////////////////////////////////////////////////////////

static void updateFromEnv (const string& name, int& var) {
  Option<string> env = os::getenv(name);

  if (env.isSome()) {
    var = atoi(env.get().c_str());
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief update from env
////////////////////////////////////////////////////////////////////////////////

static void updateFromEnv (const string& name, double& var) {
  Option<string> env = os::getenv(name);

  if (env.isSome()) {
    var = atof(env.get().c_str());
  }
}

////////////////////////////////////////////////////////////////////////////////
/// @brief prints help
////////////////////////////////////////////////////////////////////////////////

static void usage (const string& argv0, const flags::FlagsBase& flags) {
  cerr << "Usage: " << argv0 << " [...]" << "\n"
       << "\n"
       << "Supported options:" << "\n"
       << flags.usage() << "\n"
       << "Supported environment:" << "\n"
       << "  ARANGODB_MODE        overrides '--mode'\n"
       << "  ARANGODB_ASYNC_REPLICATION\n"
          "                       overrides '--async_replication'\n"
       << "  ARANGODB_ROLE        overrides '--role'\n"
       << "  ARANGODB_MINIMAL_RESOURCES_AGENT\n"
       << "                       overrides '--minimal_resources_agent'\n"
       << "  ARANGODB_MINIMAL_RESOURCES_DBSERVER\n"
          "                       overrides '--minimal_resources_dbserver'\n"
       << "  ARANGODB_MINIMAL_RESOURCES_SECONDARY\n"
          "                       overrides '--minimal_resources_secondary'\n"
       << "  ARANGODB_MINIMAL_RESOURCES_COORDINATOR\n"
          "                       overrides '--minimal_resources_coordinator'\n"
       << "  ARANGODB_NR_AGENTS   overrides '--nr_agents'\n"
       << "  ARANGODB_NR_DBSERVERS\n"
       << "                       overrides '--nr_dbservers'\n"
       << "  ARANGODB_NR_COORDINATORS\n"
       << "                       overrides '--nr_coordinators'\n"
       << "  ARANGODB_PRINCIPAL   overrides '--principal'\n"
       << "  ARANGODB_USER        overrides '--user'\n"
       << "  ARANGODB_FRAMEWORK_NAME\n"
       << "                       overrides '--framework_name'\n"
       << "  ARANGODB_FRAMEWORK_PORT\n"
       << "                       overrides '--framework_port'\n"
       << "  ARANGODB_WEBUI       overrides '--webui'\n"
       << "  ARANGODB_WEBUI_PORT  overrides '--webui_port'\n"
       << "  ARANGODB_FAILOVER_TIMEOUT\n"
       << "                       overrides '--failover_timeout'\n"
       << "  ARANGODB_SECONDARIES_WITH_DBSERVERS\n"
       << "                       overrides '--secondaries_with_dbservers'\n"
       << "  ARANGODB_COORDINATORS_WITH_DBSERVERS\n"
       << "                       overrides '--coordinators_with_dbservers'\n"
       << "  ARANGODB_IMAGE       overrides '--arangodb_image'\n"
       << "  ARANGODB_PRIVILEGED_IMAGE\n"
       << "                       overrides '--arangodb_privileged_image'\n"
       << "  ARANGODB_JWT_SECRET\n"
       << "                       overrides '--arangodb_jwt_secret\n"
       << "  ARANGODB_SSL_KEYFILE\n"
       << "                       overrides '--arangodb_ssl_keyfile\n"
       << "  ARANGODB_ENTERPRISE_KEY\n"
       << "                       overrides '--arangodb_enterprise_key'\n"
       << "  ARANGODB_STORAGE_ENGINE\n"
       << "                       overrides '--arangodb_storage_engine\n"
       << "  ARANGODB_ADDITIONAL_AGENT_ARGS\n"
       << "                       overrides '--arangodb_additional_agent_args'\n"
       << "  ARANGODB_ADDITIONAL_DBSERVER_ARGS\n"
       << "                       overrides '--arangodb_additional_dbserver_args'\n"
       << "  ARANGODB_ADDITIONAL_SECONDARY_ARGS\n"
       << "                       overrides '--arangodb_additional_secondary_args'\n"
       << "  ARANGODB_ADDITIONAL_COORDINATOR_ARGS\n"
       << "                       overrides '--arangodb_additional_coordinator_args'\n"
       << "  ARANGODB_ZK          overrides '--zk'\n"
       << "\n"
       << "  MESOS_MASTER         overrides '--master'\n"
       << "  MESOS_SECRET         secret for mesos authentication\n"
       << "  MESOS_AUTHENTICATE   enable authentication\n"
       << "\n";
}

////////////////////////////////////////////////////////////////////////////////
/// @brief string command line argument to bool
////////////////////////////////////////////////////////////////////////////////
bool str2bool(const string in) {
  if (in == "yes" || in == "true" || in == "y") {
    return true;
  } else {
    return false;
  }
}

/* first, here is the code for the signal handler */
void catch_child(int sig_num)
{
  pid_t pid;
  /* when we get here, we know there's a zombie child waiting */
  int child_status;

  pid = waitpid(-1, &child_status, 0);
  LOG(INFO) << "old haproxy(" << pid << ") exited with status " << child_status;
  if (child_status != 0) {
    LOG(INFO) << "Scheduling restart";
    Global::state().setRestartProxy(RESTART_FRESH_START);
  }
}

// -----------------------------------------------------------------------------
// --SECTION--                                                  public functions
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief ArangoDB framework
////////////////////////////////////////////////////////////////////////////////

class Flags : public virtual mesos::internal::logging::Flags
{
public:
  Flags()
  { 
    add(&Flags::containerizer,
        "containerizer",
        "Which containerizer we should run arangodb in (docker, mesos)",
        "docker");
  add(&Flags::mode,
            "mode",
            "Mode of operation (standalone, cluster)",
            "cluster");

  add(&Flags::async_repl,
            "async_replication",
            "Flag, whether we run secondaries for asynchronous replication",
            "false");

  add(&Flags::role,
            "role",
            "Role to use when registering",
            "*");

  add(&Flags::minimal_resources_agent,
            "minimal_resources_agent",
            "Minimal resources to accept for an agent",
            "");

  add(&Flags::minimal_resources_dbserver,
            "minimal_resources_dbserver",
            "Minimal resources to accept for a DBServer",
            "");

  add(&Flags::minimal_resources_secondary,
            "minimal_resources_secondary",
            "Minimal resources to accept for a secondary DBServer",
            "");

  add(&Flags::minimal_resources_coordinator,
            "minimal_resources_coordinator",
            "Minimal resources to accept for a coordinator",
            "");

  add(&Flags::nragents,
            "nr_agents",
            "Number of agents in agency",
            1);

  add(&Flags::nrdbservers,
            "nr_dbservers",
            "Initial number of DBservers in cluster",
            2);

  add(&Flags::nrcoordinators,
            "nr_coordinators",
            "Initial number of coordinators in cluster",
            1);

  add(&Flags::principal,
            "principal",
            "Principal for persistent volumes",
            "arangodb");

  add(&Flags::frameworkUser,
            "user",
            "User for the framework",
            "");

  add(&Flags::frameworkName,
            "framework_name",
            "custom framework name",
            "arangodb");

  add(&Flags::webui,
            "webui",
            "URL to advertise for external access to the UI",
            "");

  add(&Flags::frameworkPort,
            "framework_port",
            "framework http port",
            Global::frameworkPort());
  
  add(&Flags::webuiPort,
            "webui_port",
            "webui http port",
            Global::webuiPort());

  add(&Flags::failoverTimeout,
            "failover_timeout",
            "failover timeout in seconds",
            60 * 60 * 24 * 10);
  
  add(&Flags::declineOfferRefuseSeconds,
            "refuse_seconds",
            "number of seconds to refuse an offer if declined",
            20);

  add(&Flags::offerLimit,
            "offer_limit",
            "number of offers we are accepting",
            10);

  add(&Flags::resetState,
            "reset_state",
            "ignore any old state",
            "false");

  add(&Flags::secondariesWithDBservers,
            "secondaries_with_dbservers",
            "run secondaries only on agents with DBservers",
            "false");

  add(&Flags::coordinatorsWithDBservers,
            "coordinators_with_dbservers",
            "run coordinators only on agents with DBservers",
            "false");

  add(&Flags::secondarySameServer,
            "secondary_same_server",
            "allow to run a secondary on same agent as its primary",
            "false");
  
  add(&Flags::arangoDBImage,
            "arangodb_image",
            "ArangoDB docker image to use",
            "");
  
  add(&Flags::arangoDBForcePullImage,
            "arangodb_force_pull_image",
            "force pulling the ArangoDB image",
            "true");

  add(&Flags::arangoDBPrivilegedImage,
            "arangodb_privileged_image",
            "start the arangodb image privileged",
            "false");
  add(&Flags::master,
            "master",
            "ip:port of master to connect",
            "");
  
  add(&Flags::arangoDBEnterpriseKey,
            "arangodb_enterprise_key",
            "enterprise key for arangodb",
            "");
  
  add(&Flags::arangoDBStorageEngine,
            "arangodb_storage_engine",
            "storage engine to choose",
            "auto");

  add(&Flags::arangoDBJwtSecret,
            "arangodb_jwt_secret",
            "secret for internal cluster communication",
            "");

  add(&Flags::arangoDBSslKeyfile,
            "arangodb_ssl_keyfile",
            "SSL keyfile to use (optional - specify .pem file base64 encoded)",
            "");
  
  add(&Flags::arangoDBEncryptionKeyfile,
            "arangodb_encryption_keyfile",
            "data encryption keyfile to use (optional - specify 32 byte aes keyfile base64 encoded)",
            "");

  add(&Flags::arangoDBAdditionalAgentArgs,
            "arangodb_additional_agent_args",
            "additional command line arguments to be passed when starting an agent",
            "");

  add(&Flags::arangoDBAdditionalDBServerArgs,
            "arangodb_additional_dbserver_args",
            "additional command line arguments to be passed when starting a dbserver",
            "");

  add(&Flags::arangoDBAdditionalSecondaryArgs,
            "arangodb_additional_secondary_args",
            "additional command line arguments to be passed when starting a secondary",
            "");

  add(&Flags::arangoDBAdditionalCoordinatorArgs,
            "arangodb_additional_coordinator_args",
            "additional command line arguments to be passed when starting an coordinator",
            "");

  add(&Flags::zk,
            "zk",
            "zookeeper for state",
            "");

  }
public:
  std::string containerizer;
  string mode;
  string async_repl;
  string role;
  string minimal_resources_agent;
  string minimal_resources_dbserver;
  string minimal_resources_secondary;
  string minimal_resources_coordinator;
  int nragents;
  int nrdbservers;
  int nrcoordinators;
  string principal;
  string frameworkUser;
  string frameworkName;
  string webui;
  int frameworkPort;
  int webuiPort;
  double failoverTimeout;
  double declineOfferRefuseSeconds;
  int offerLimit;
  string resetState;
  string secondariesWithDBservers;
  string coordinatorsWithDBservers;
  string secondarySameServer;
  string arangoDBImage;
  string arangoDBForcePullImage;
  string arangoDBPrivilegedImage;
  // address of master and zookeeper
  string master;
  string arangoDBEnterpriseKey;
  string arangoDBStorageEngine;
  string arangoDBJwtSecret;
  string arangoDBSslKeyfile;
  string arangoDBEncryptionKeyfile;
  string arangoDBAdditionalAgentArgs;
  string arangoDBAdditionalDBServerArgs;
  string arangoDBAdditionalSecondaryArgs;
  string arangoDBAdditionalCoordinatorArgs;
  string zk;
};

#ifdef unix
#undef unix
#endif
#include "pbjson.hpp"

int main (int argc, char** argv) {

  // ...........................................................................
  // command line options
  // ...........................................................................
#if 0
  std::string myOffer = R"EOF( 
{
  "offer": {
    "id": {
      "value": "0b905d78-05a2-4a21-a99d-1d562c316c6e-O5568"
    },
    "framework_id": {
      "value": "0b905d78-05a2-4a21-a99d-1d562c316c6e-0031"
    },
    "slave_id": {
      "value": "0b905d78-05a2-4a21-a99d-1d562c316c6e-S3"
    },
    "hostname": "10.0.3.18",
    "url": {
      "scheme": "http",
      "address": {
        "hostname": "10.0.3.18",
        "ip": "10.0.3.18",
        "port": 5051
      },
      "path": "/slave(1)",
      "query": []
    },
    "resources": [
      {
        "name": "ports",
        "type": 1,
        "ranges": {
          "range": [
            {
              "begin": 1025,
              "end": 2180
            },
            {
              "begin": 2182,
              "end": 3887
            },
            {
              "begin": 3889,
              "end": 5049
            },
            {
              "begin": 5052,
              "end": 8079
            },
            {
              "begin": 8082,
              "end": 8180
            },
            {
              "begin": 8182,
              "end": 29407
            },
            {
              "begin": 29411,
              "end": 32000
            }
          ]
        },
        "allocation_info": {
          "role": "arangodb3"
        },
        "reservations": []
      },
      {
        "name": "disk",
        "type": 0,
        "scalar": {
          "value": 35566
        },
        "allocation_info": {
          "role": "arangodb3"
        },
        "reservations": []
      },
      {
        "name": "cpus",
        "type": 0,
        "scalar": {
          "value": 3.75
        },
        "allocation_info": {
          "role": "arangodb3"
        },
        "reservations": []
      },
      {
        "name": "mem",
        "type": 0,
        "scalar": {
          "value": 13509
        },
        "allocation_info": {
          "role": "arangodb3"
        },
        "reservations": []
      }
    ],
    "attributes": [],
    "executor_ids": [],
    "allocation_info": {
      "role": "arangodb3"
    }
  }
}
)EOF";
#if 0
  std::string myOfferResource = R"EOF( 
[
      {
        "name": "ports",
        "type": "RANGES",
        "ranges": {
          "range": [
            {
              "begin": 1025,
              "end": 2180
            },
            {
              "begin": 2182,
              "end": 3887
            },
            {
              "begin": 3889,
              "end": 5049
            },
            {
              "begin": 5052,
              "end": 8079
            },
            {
              "begin": 8082,
              "end": 8180
            },
            {
              "begin": 8182,
              "end": 29407
            },
            {
              "begin": 29411,
              "end": 32000
            }
          ]
        },
        "allocation_info": {
          "role": "arangodb3"
        },
        "reservations": []
      },
      {
        "name": "disk",
        "type": "SCALAR",
        "scalar": {
          "value": 35566
        },
        "allocation_info": {
          "role": "arangodb3"
        },
        "reservations": []
      },
      {
        "name": "cpus",
        "type": "SCALAR",
        "scalar": {
          "value": 3.75
        },
        "allocation_info": {
          "role": "arangodb3"
        },
        "reservations": []
      },
      {
        "name": "mem",
        "type": "SCALAR",
        "scalar": {
          "value": 13509
        },
        "allocation_info": {
          "role": "arangodb3"
        },
        "reservations": []
      }
    ]
)EOF";

  std::string myMinimumResource = R"EOF( 
[
      {
        "name": "disk",
        "type": "SCALAR",
        "scalar": {
          "value": 2048
        }
      },
      {
        "name": "cpus",
        "type": "SCALAR",
        "scalar": {
          "value": 0.25
        }
      },
      {
        "name": "mem",
        "type": "SCALAR",
        "scalar": {
          "value": 2048
        }
      }
    ]
)EOF";

#endif
    std::string myOfferResource = R"EOF( 
[
      {
        "name": "mem",
        "type": "SCALAR",
        "scalar": {
          "value": 13509
        },
        "allocation_info": {
          "role": "arangodb3"
        },
        "reservations": []
      }
    ]
)EOF";

  std::string myMinimumResource = R"EOF( 
[
      {
        "name": "mem",
        "type": "SCALAR",
        "scalar": {
          "value": 2048
        },
        "allocation_info": {
          "role": "arangodb3"
        }
      }
    ]
)EOF";

  
  // std::cout << myOffer;


  mesos::Offer offer;

  mesos::Resources myresource;
  auto foo = myresource.parse(myOfferResource, "arangodb3").get();
  std::cout << foo <<std::endl;

  auto bar = myresource.parse(myMinimumResource, "arangodb3").get();
  std::cout << bar <<std::endl;

  foo.contains(bar);

  //  auto res = offer.add_resources();


  
  return 1;



#endif




  
  // parse the command line flags
  Flags flags;

  Try<flags::Warnings> load = flags.load(None(), argc, argv);

  if (load.isError()) {
    cerr << load.error() << endl;
    usage(argv[0], flags);
    exit(EXIT_FAILURE);
  }

  if (flags.help) {
    usage(argv[0], flags);
    exit(EXIT_SUCCESS);
  }

  updateFromEnv("ARANGODB_MODE", flags.mode);
  updateFromEnv("ARANGODB_ASYNC_REPLICATION", flags.async_repl);
  updateFromEnv("ARANGODB_ROLE", flags.role);
  updateFromEnv("ARANGODB_MINIMAL_RESOURCES_AGENT", flags.minimal_resources_agent);
  updateFromEnv("ARANGODB_MINIMAL_RESOURCES_DBSERVER", flags.minimal_resources_dbserver);
  updateFromEnv("ARANGODB_MINIMAL_RESOURCES_SECONDARY",  flags.minimal_resources_secondary);
  updateFromEnv("ARANGODB_MINIMAL_RESOURCES_COORDINATOR", flags.minimal_resources_coordinator);
  updateFromEnv("ARANGODB_NR_AGENTS", flags.nragents);

  if (flags.nragents < 1) {
    flags.nragents = 1;
  }

  updateFromEnv("ARANGODB_NR_DBSERVERS", flags.nrdbservers);

  if (flags.nrdbservers < 1) {
    flags.nrdbservers = 1;
  }

  updateFromEnv("ARANGODB_NR_COORDINATORS", flags.nrcoordinators);

  if (flags.nrcoordinators < 1) {
    flags.nrcoordinators = 1;
  }

  updateFromEnv("ARANGODB_PRINCIPAL", flags.principal);
  updateFromEnv("ARANGODB_USER", flags.frameworkUser);
  updateFromEnv("ARANGODB_FRAMEWORK_NAME", flags.frameworkName);
  updateFromEnv("ARANGODB_WEBUI", flags.webui);
  updateFromEnv("ARANGODB_WEBUI_PORT", flags.webuiPort);
  updateFromEnv("ARANGODB_FRAMEWORK_PORT", flags.frameworkPort);
  updateFromEnv("ARANGODB_FAILOVER_TIMEOUT", flags.failoverTimeout);
  updateFromEnv("ARANGODB_DECLINE_OFFER_REFUSE_SECONDS", flags.declineOfferRefuseSeconds);
  updateFromEnv("ARANGODB_OFFER_LIMIT", flags.offerLimit);
  updateFromEnv("ARANGODB_RESET_STATE", flags.resetState);
  updateFromEnv("ARANGODB_SECONDARIES_WITH_DBSERVERS", flags.secondariesWithDBservers);
  updateFromEnv("ARANGODB_COORDINATORS_WITH_DBSERVERS", flags.coordinatorsWithDBservers);
  updateFromEnv("ARANGODB_IMAGE", flags.arangoDBImage);
  updateFromEnv("ARANGODB_FORCE_PULL_IMAGE", flags.arangoDBForcePullImage);
  updateFromEnv("ARANGODB_PRIVILEGED_IMAGE", flags.arangoDBPrivilegedImage);
  updateFromEnv("ARANGODB_ENTERPRISE_KEY", flags.arangoDBEnterpriseKey);
  updateFromEnv("ARANGODB_JWT_SECRET", flags.arangoDBJwtSecret);
  updateFromEnv("ARANGODB_SSL_KEYFILE", flags.arangoDBSslKeyfile);
  updateFromEnv("ARANGODB_STORAGE_ENGINE", flags.arangoDBStorageEngine);
  updateFromEnv("ARANGODB_ENCRYPTION_KEYFILE", flags.arangoDBEncryptionKeyfile);
  updateFromEnv("ARANGODB_ADDITIONAL_AGENT_ARGS", flags.arangoDBAdditionalAgentArgs);
  updateFromEnv("ARANGODB_ADDITIONAL_DBSERVER_ARGS", flags.arangoDBAdditionalDBServerArgs);
  updateFromEnv("ARANGODB_ADDITIONAL_SECONDARY_ARGS", flags.arangoDBAdditionalSecondaryArgs);
  updateFromEnv("ARANGODB_ADDITIONAL_COORDINATOR_ARGS", flags.arangoDBAdditionalCoordinatorArgs);

  updateFromEnv("MESOS_MASTER", flags.master);
  updateFromEnv("ARANGODB_ZK", flags.zk);

  if (flags.master.empty()) {
    cerr << "Missing master, either use flag '--master' or set 'MESOS_MASTER'" << endl;
    usage(argv[0], flags);
    exit(EXIT_FAILURE);
  }

  if (flags.arangoDBImage.empty()) {
    cerr << "Missing image, please provide an arangodb image to run on the agents via '--arangodb_image' or set 'ARANGODB_IMAGE'" << endl;
    usage(argv[0], flags);
    exit(EXIT_FAILURE);
  }

  logging::initialize(argv[0], flags, true); // Catch signals.

  Global::setArangoDBImage(flags.arangoDBImage);
  LOG(INFO) << "ArangoDB Image: " << Global::arangoDBImage();

  if (flags.mode == "standalone") {
    Global::setMode(OperationMode::STANDALONE);
  }
  else if (flags.mode == "cluster") {
    Global::setMode(OperationMode::CLUSTER);
  }
  else {
    cerr << argv[0] << ": expecting mode '" << flags.mode << "' to be "
         << "standalone, cluster" << "\n";
  }
  LOG(INFO) << "Mode: " << flags.mode;

  Global::setAsyncReplication(str2bool(flags.async_repl));
  LOG(INFO) << "Asynchronous replication flag: " << Global::asyncReplication();

  Global::setFrameworkName(flags.frameworkName);
  
  Global::setContainerizer(flags.containerizer);

  Global::setSecondariesWithDBservers(str2bool(flags.secondariesWithDBservers));
  LOG(INFO) << "SecondariesWithDBservers: " << Global::secondariesWithDBservers();

  Global::setCoordinatorsWithDBservers(str2bool(flags.coordinatorsWithDBservers));
  LOG(INFO) << "CoordinatorsWithDBservers: " << Global::coordinatorsWithDBservers();
  
  Global::setSecondarySameServer(str2bool(flags.secondarySameServer));
  LOG(INFO) << "SecondarySameServer: " << Global::secondarySameServer();
  
  Global::setArangoDBForcePullImage(str2bool(flags.arangoDBForcePullImage));
  LOG(INFO) << "ArangoDBForcePullImage: " << Global::arangoDBForcePullImage();

  Global::setArangoDBPrivilegedImage(str2bool(flags.arangoDBPrivilegedImage));
  LOG(INFO) << "ArangoDBPrivilegedImage: " << Global::arangoDBPrivilegedImage();

  LOG(INFO) << "Minimal resources agent: " << flags.minimal_resources_agent;
  Global::setMinResourcesAgent(flags.minimal_resources_agent);
  LOG(INFO) << "Minimal resources DBserver: " << flags.minimal_resources_dbserver;
  Global::setMinResourcesDBServer(flags.minimal_resources_dbserver);
  LOG(INFO) << "Minimal resources secondary DBserver: " 
            << flags.minimal_resources_secondary;
  Global::setMinResourcesSecondary(flags.minimal_resources_secondary);
  LOG(INFO) << "Minimal resources coordinator: " 
            << flags.minimal_resources_coordinator;
  Global::setMinResourcesCoordinator(flags.minimal_resources_coordinator);
  LOG(INFO) << "Number of agents in agency: " << flags.nragents;
  Global::setNrAgents(flags.nragents);
  LOG(INFO) << "Number of DBservers: " << flags.nrdbservers;
  Global::setNrDBServers(flags.nrdbservers);
  LOG(INFO) << "Number of coordinators: " << flags.nrcoordinators;
  Global::setNrCoordinators(flags.nrcoordinators);
  LOG(INFO) << "Framework port: " << flags.frameworkPort;
  Global::setFrameworkPort(flags.frameworkPort);
  LOG(INFO) << "WebUI port: " << flags.webuiPort;
  Global::setWebuiPort(flags.webuiPort);
  LOG(INFO) << "ArangoDB Enterprise Key: " << flags.arangoDBEnterpriseKey;
  Global::setArangoDBEnterpriseKey(flags.arangoDBEnterpriseKey);
  LOG(INFO) << "ArangoDB JWT Secret: " << flags.arangoDBJwtSecret;
  Global::setArangoDBJwtSecret(flags.arangoDBJwtSecret);
  LOG(INFO) << "ArangoDB SSL Keyfile: " << flags.arangoDBSslKeyfile;
  Global::setArangoDBSslKeyfile(flags.arangoDBSslKeyfile);
  LOG(INFO) << "ArangoDB Storage Engine: " << flags.arangoDBStorageEngine;
  Global::setArangoDBStorageEngine(flags.arangoDBStorageEngine);
  LOG(INFO) << "ArangoDB Encryption Keyfile: " << flags.arangoDBEncryptionKeyfile;
  Global::setArangoDBEncryptionKeyfile(flags.arangoDBEncryptionKeyfile);
  LOG(INFO) << "ArangoDB additional agent args: " << flags.arangoDBAdditionalAgentArgs;
  Global::setArangoDBAdditionalAgentArgs(flags.arangoDBAdditionalAgentArgs);
  LOG(INFO) << "ArangoDB additional dbserver args: " << flags.arangoDBAdditionalAgentArgs;
  Global::setArangoDBAdditionalDBServerArgs(flags.arangoDBAdditionalDBServerArgs);
  LOG(INFO) << "ArangoDB additional secondary args: " << flags.arangoDBAdditionalSecondaryArgs;
  Global::setArangoDBAdditionalSecondaryArgs(flags.arangoDBAdditionalSecondaryArgs);
  LOG(INFO) << "ArangoDB additional coordinator args: " << flags.arangoDBAdditionalCoordinatorArgs;
  Global::setArangoDBAdditionalCoordinatorArgs(flags.arangoDBAdditionalCoordinatorArgs);

  // ...........................................................................
  // state
  // ...........................................................................

  LOG(INFO) << "zookeeper: " << flags.zk;

  ArangoState state(flags.frameworkName, flags.zk);
  state.init();

  if (flags.resetState == "true" || flags.resetState == "y" || flags.resetState == "yes") {
    state.destroy();
  }
  else {
    state.load();
  }
  
  
  if (!state.createReverseProxyConfig()) {
    LOG(ERROR) << "Couldn't create reverse proxy config";
    exit(EXIT_FAILURE);
  }

  struct sigaction new_action;
  new_action.sa_handler = catch_child;
  sigemptyset (&new_action.sa_mask);
  new_action.sa_flags = 0;
  sigaction(SIGCHLD, &new_action, nullptr);
  Global::setState(&state);
  state.setRestartProxy(RESTART_FRESH_START);

  // ...........................................................................
  // framework
  // ...........................................................................

  // create the framework
  mesos::FrameworkInfo framework;
  framework.set_user(flags.frameworkUser);
  LOG(INFO) << "framework user: " << flags.frameworkUser;
  framework.set_checkpoint(true);

  framework.set_name(flags.frameworkName);
  LOG(INFO) << "framework name: " << flags.frameworkName;

  framework.set_role(flags.role);
  LOG(INFO) << "role: " << flags.role;

  if (0.0 < flags.failoverTimeout) {
    framework.set_failover_timeout(flags.failoverTimeout);
  }
  else {
    flags.failoverTimeout = 0.0;
  }

  auto cap = framework.add_capabilities();
  cap->set_type(mesos::FrameworkInfo::Capability::RESERVATION_REFINEMENT);
  LOG(INFO) << "failover timeout: " << flags.failoverTimeout;
  
  {
    auto lease = Global::state().lease();
    if (lease.state().has_framework_id()) {
      framework.mutable_id()->CopyFrom(lease.state().framework_id());
    }
  }

  // ...........................................................................
  // http server
  // ...........................................................................

  if (flags.webui.empty()) {
    Try<string> hostnameTry = net::hostname();
    string hostname = hostnameTry.get();

    flags.webui = "http://" + hostname + ":" + std::to_string(flags.webuiPort);
  }

  LOG(INFO) << "webui url: " << flags.webui << " (local port is " << flags.webuiPort << ")";
  LOG(INFO) << "framework listening on port: " << flags.frameworkPort;

  framework.set_webui_url(flags.webui);

  Option<string> mesosCheckpoint =  os::getenv("MESOS_CHECKPOINT");

  if (mesosCheckpoint.isSome()) {
    framework.set_checkpoint(numify<bool>(mesosCheckpoint).get());
  }

  // ...........................................................................
  // global options
  // ...........................................................................

  Global::setRole(flags.role);
  Global::setPrincipal(flags.principal);
  Global::setDeclineOfferRefuseSeconds(flags.declineOfferRefuseSeconds);
  LOG(INFO) << "refuse seconds: " << Global::declineOfferRefuseSeconds();
  Global::setOfferLimit(flags.offerLimit);
  LOG(INFO) << "offer limit: " << Global::offerLimit();


  // ...........................................................................
  // Caretaker
  // ...........................................................................

  unique_ptr<Caretaker> caretaker;

  switch (Global::mode()) {
    case OperationMode::STANDALONE:
      caretaker.reset(new CaretakerStandalone);
      break;

    case OperationMode::CLUSTER:
      caretaker.reset(new CaretakerCluster);
      break;
  }

  Global::setCaretaker(caretaker.get());

  // ...........................................................................
  // manager
  // ...........................................................................

  ArangoManager* manager = new ArangoManager();
  Global::setManager(manager);

  // ...........................................................................
  // scheduler
  // ...........................................................................

  // create the scheduler
  ArangoScheduler scheduler;

  mesos::MesosSchedulerDriver* driver;

  Option<string> mesosAuthenticate = os::getenv("MESOS_AUTHENTICATE");

  if (mesosAuthenticate.isSome() && mesosAuthenticate.get() == "true") {
    cout << "Enabling authentication for the framework" << endl;

    if (flags.principal.empty()) {
      EXIT(EXIT_FAILURE) << "Expecting authentication principal in the environment";
    }

    Option<string> mesosSecret = os::getenv("MESOS_SECRET");

    if (mesosSecret.isNone()) {
      EXIT(EXIT_FAILURE) << "Expecting authentication secret in the environment";
    }

    mesos::Credential credential;
    credential.set_principal(flags.principal);
    credential.set_secret(mesosSecret.get());

    framework.set_principal(flags.principal);
    driver = new mesos::MesosSchedulerDriver(&scheduler, framework, flags.master, credential);
  }
  else {
    framework.set_principal(flags.principal);
    driver = new mesos::MesosSchedulerDriver(&scheduler, framework, flags.master);
  }

  scheduler.setDriver(driver);

  Global::setScheduler(&scheduler);

  // ...........................................................................
  // run
  // ...........................................................................

  // and the http server
  HttpServer http;

  // start and wait
  LOG(INFO) << "http port: " << Global::frameworkPort();
  http.start(Global::frameworkPort());

  int status = driver->run() == mesos::DRIVER_STOPPED ? 0 : 1;

  // ensure that the driver process terminates
  driver->stop();

  delete driver;
  delete manager;

  sleep(30);   // Wait some more time before terminating the process to
               // allow the user to use 
               //   dcos package uninstall arangodb
               // to remove the Marathon job
  http.stop();

  return status;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
