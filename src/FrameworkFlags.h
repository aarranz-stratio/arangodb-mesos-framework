////////////////////////////////////////////////////////////////////////////////
/// @brief utilities
///
/// @file
///
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// @author Andreas Streichardt
/// @author Copyright 2017, ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#ifndef FRAMEWORK_FLAGS_H
#define FRAMEWORK_FLAGS_H 1

#include "Global.h"

#include <string>
#include "logging/flags.hpp"

namespace arangodb {

class FrameworkFlags : public virtual mesos::internal::logging::Flags {
 public:
  FrameworkFlags() {
    add(&FrameworkFlags::mode, "mode", "Mode of operation (standalone, cluster)",
              "cluster");

    add(&FrameworkFlags::async_repl, "async_replication",
              "Flag, whether we run secondaries for asynchronous replication",
              "false");

    add(&FrameworkFlags::role, "role", "Role to use when registering", "*");
    add(&FrameworkFlags::minimal_resources_agent, "minimal_resources_agent",
              "Minimal resources to accept for an agent", "");
    add(&FrameworkFlags::minimal_resources_dbserver, "minimal_resources_dbserver",
              "Minimal resources to accept for a DBServer", "");

    add(&FrameworkFlags::minimal_resources_secondary, "minimal_resources_secondary",
              "Minimal resources to accept for a secondary DBServer", "");
    add(&FrameworkFlags::minimal_resources_coordinator, "minimal_resources_coordinator",
              "Minimal resources to accept for a coordinator", "");
    add(&FrameworkFlags::nragents, "nr_agents", "Number of agents in agency", 1);
    add(&FrameworkFlags::nrdbservers, "nr_dbservers",
              "Initial number of DBservers in cluster", 2);
    add(&FrameworkFlags::nrcoordinators, "nr_coordinators",
              "Initial number of coordinators in cluster", 1);
    add(&FrameworkFlags::principal, "principal", "Principal for persistent volumes",
              "arangodb");
    add(&FrameworkFlags::frameworkUser, "user", "User for the framework", "");
    add(&FrameworkFlags::frameworkName, "framework_name", "custom framework name",
              "arangodb");
    add(&FrameworkFlags::webui, "webui", "URL to advertise for external access to the UI",
              "");
    add(&FrameworkFlags::frameworkPort, "framework_port", "framework http port",
              Global::frameworkPort());
    add(&FrameworkFlags::webuiPort, "webui_port", "webui http port", Global::webuiPort());
    add(&FrameworkFlags::failoverTimeout, "failover_timeout",
              "failover timeout in seconds", 60 * 60 * 24 * 10);
    add(&FrameworkFlags::resetState, "reset_state", "ignore any old state", "false");
    add(&FrameworkFlags::secondariesWithDBservers, "secondaries_with_dbservers",
              "run secondaries only on agents with DBservers", "false");
    add(&FrameworkFlags::coordinatorsWithDBservers, "coordinators_with_dbservers",
              "run coordinators only on agents with DBservers", "false");
    add(&FrameworkFlags::secondarySameServer, "secondary_same_server",
              "allow to run a secondary on same agent as its primary", "false");
    add(&FrameworkFlags::arangoDBImage, "arangodb_image", "ArangoDB docker image to use",
              "");
    add(&FrameworkFlags::arangoDBForcePullImage, "arangodb_force_pull_image",
              "force pulling the ArangoDB image", "true");
    add(&FrameworkFlags::arangoDBPrivilegedImage, "arangodb_privileged_image",
              "start the arangodb image privileged", "false");
    add(&FrameworkFlags::master, "master", "ip:port of master to connect", "");
    add(&FrameworkFlags::arangoDBEnterpriseKey, "arangodb_enterprise_key",
              "enterprise key for arangodb", "");
    add(&FrameworkFlags::arangoDBStorageEngine, "arangodb_storage_engine",
              "storage engine to choose", "auto");
    add(&FrameworkFlags::arangoDBJwtSecret, "arangodb_jwt_secret",
              "secret for internal cluster communication", "");
    add(
        &FrameworkFlags::arangoDBSslKeyfile, "arangodb_ssl_keyfile",
        "SSL keyfile to use (optional - specify .pem file base64 encoded)", "");
    add(&FrameworkFlags::arangoDBEncryptionKeyfile, "arangodb_encryption_keyfile",
              "data encryption keyfile to use (optional - specify 32 byte aes "
              "keyfile base64 encoded)",
              "");
    add(
        &FrameworkFlags::arangoDBAdditionalAgentArgs, "arangodb_additional_agent_args",
        "additional command line arguments to be passed when starting an agent",
        "");
    add(&FrameworkFlags::arangoDBAdditionalDBServerArgs,
              "arangodb_additional_dbserver_args",
              "additional command line arguments to be passed when starting a "
              "dbserver",
              "");
    add(&FrameworkFlags::arangoDBAdditionalSecondaryArgs,
              "arangodb_additional_secondary_args",
              "additional command line arguments to be passed when starting a "
              "secondary",
              "");

    add(&FrameworkFlags::arangoDBAdditionalCoordinatorArgs,
              "arangodb_additional_coordinator_args",
              "additional command line arguments to be passed when starting an "
              "coordinator",
              "");

    add(&FrameworkFlags::zk, "zk", "zookeeper for state", "");
  }

  std::string mode;
  std::string async_repl;
  std::string role;
  std::string minimal_resources_agent;
  std::string minimal_resources_dbserver;
  std::string minimal_resources_secondary;
  std::string minimal_resources_coordinator;
  int nragents;
  int nrdbservers;
  int nrcoordinators;
  std::string principal;
  std::string frameworkUser;
  std::string frameworkName;
  std::string webui;
  int frameworkPort;
  int webuiPort;
  double failoverTimeout;
  std::string resetState;
  std::string secondariesWithDBservers;
  std::string coordinatorsWithDBservers;
  std::string secondarySameServer;
  std::string arangoDBImage;
  std::string arangoDBForcePullImage;
  std::string arangoDBPrivilegedImage;
  // address of master and zookeeper
  std::string master;
  std::string arangoDBEnterpriseKey;
  std::string arangoDBStorageEngine;
  std::string arangoDBJwtSecret;
  std::string arangoDBSslKeyfile;
  std::string arangoDBEncryptionKeyfile;
  std::string arangoDBAdditionalAgentArgs;
  std::string arangoDBAdditionalDBServerArgs;
  std::string arangoDBAdditionalSecondaryArgs;
  std::string arangoDBAdditionalCoordinatorArgs;
  std::string zk;
};
}
#endif