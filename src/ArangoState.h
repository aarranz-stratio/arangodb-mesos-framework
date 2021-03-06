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

#ifndef ARANGO_STATE_H
#define ARANGO_STATE_H 1

#include "arangodb.pb.h"
#include "Global.h"

#include <atomic>
#include <mutex>
#include <csignal>
#include <thread>
#include <chrono>

#include <state/protobuf.hpp>

namespace arangodb {
  const int RESTART_KEEP_RUNNING = 0;
  const int RESTART_FRESH_START = 1;
  const int RESTART_RESTART = 2;

// -----------------------------------------------------------------------------
// --SECTION--                                                       ArangoState
// -----------------------------------------------------------------------------

  class ArangoState {

// -----------------------------------------------------------------------------
// --SECTION--                                      constructors and destructors
// -----------------------------------------------------------------------------

    public:

////////////////////////////////////////////////////////////////////////////////
/// @brief constructor
////////////////////////////////////////////////////////////////////////////////

      ArangoState (const std::string& name, const std::string& zk);

// -----------------------------------------------------------------------------
// --SECTION--                                                    public methods
// -----------------------------------------------------------------------------

    public:

////////////////////////////////////////////////////////////////////////////////
/// @brief initializes the state
////////////////////////////////////////////////////////////////////////////////

      void init ();

////////////////////////////////////////////////////////////////////////////////
/// @brief loads the state
////////////////////////////////////////////////////////////////////////////////

      void load ();

////////////////////////////////////////////////////////////////////////////////
/// @brief removes the state from store
////////////////////////////////////////////////////////////////////////////////

      void destroy ();

////////////////////////////////////////////////////////////////////////////////
/// @brief gets filename of the haproxy config
////////////////////////////////////////////////////////////////////////////////

      char const* getProxyConfFilename() const { return _proxyConfFilename.c_str(); }

// -----------------------------------------------------------------------------
// --SECTION--                                     state leasing infrastructure
// -----------------------------------------------------------------------------

      class Lease {
          ArangoState* _parent;
          bool _changed;
          bool _moved;
        public:

          State& state () {
            return _parent->_state;
          }

          void changed () {
            _changed = true;
          }

          Lease (ArangoState* p, bool write) 
              : _parent(p), _changed(write), _moved(false) {
          }

          ~Lease () {
            if (_moved) {
              return;
            }
            if (_changed) {
              // mop: recreate config
              if (_parent->save()) {
                _parent->createReverseProxyConfig();
                _parent->setRestartProxy(RESTART_RESTART);
              }
            }
            std::lock_guard<std::mutex> lock(_parent->_lock);
            _parent->_isLeased = false;
          }

          // Moving is allowed, used in the lease() function below
          Lease (Lease&& that) 
              : _parent(that._parent), _changed(that._changed), _moved(false) {
            that._moved = true;
          }

          // All other copy or move constructors or assignments are deleted:
          Lease () = delete;
          Lease (Lease const& that) = delete;
          Lease& operator= (Lease const& that) = delete;
          Lease& operator= (Lease&& that) = delete;
      };

      Lease lease (bool write = false) {
        while (true) {
          bool ok = false;
          {
            std::lock_guard<std::mutex> lock(_lock);
            if (! _isLeased) {
              ok = true;
              _isLeased = true;
            }
          }
          if (ok) {
            Lease result (this, write);
            return result;
          }
          LOG(INFO) << "Did not get lease of state, waiting 1 sec...";
          std::this_thread::sleep_for(std::chrono::seconds(1));
        }
      }

////////////////////////////////////////////////////////////////////////////////
/// @brief find the URL of our own agency
////////////////////////////////////////////////////////////////////////////////

      std::string getAgencyURL (Lease& lease);

////////////////////////////////////////////////////////////////////////////////
/// @brief find the URL of some coordinator
////////////////////////////////////////////////////////////////////////////////

      std::string getCoordinatorURL (Lease& lease);

////////////////////////////////////////////////////////////////////////////////
/// @brief is the cluster healthy?
////////////////////////////////////////////////////////////////////////////////

      bool clusterHealthy(Lease& lease);

////////////////////////////////////////////////////////////////////////////////
/// @brief create a reverse proxy config from our current state
////////////////////////////////////////////////////////////////////////////////

      bool createReverseProxyConfig();

////////////////////////////////////////////////////////////////////////////////
/// @brief set proxy pid
////////////////////////////////////////////////////////////////////////////////

      void setProxyPid(pid_t pid);

////////////////////////////////////////////////////////////////////////////////
/// @brief set proxy pid
////////////////////////////////////////////////////////////////////////////////

      pid_t getProxyPid();

      void setRestartProxy(int restartProxy);
      
      int getRestartProxy();

// -----------------------------------------------------------------------------
// --SECTION--                                                   private methods
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief saves the state
////////////////////////////////////////////////////////////////////////////////

    private:

      bool save ();

// -----------------------------------------------------------------------------
// --SECTION--                                                 private variables
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief name of the state for none-zk
////////////////////////////////////////////////////////////////////////////////

      const std::string _name;

////////////////////////////////////////////////////////////////////////////////
/// @brief zk uri
////////////////////////////////////////////////////////////////////////////////

      const std::string _zk;

////////////////////////////////////////////////////////////////////////////////
/// @brief storage for state
////////////////////////////////////////////////////////////////////////////////

      mesos::state::Storage* _storage;

////////////////////////////////////////////////////////////////////////////////
/// @brief state storage 
////////////////////////////////////////////////////////////////////////////////

      mesos::state::State* _stateStore;

////////////////////////////////////////////////////////////////////////////////
/// @brief the state itself
////////////////////////////////////////////////////////////////////////////////

      State _state;

////////////////////////////////////////////////////////////////////////////////
/// @brief flag, whether or not the state is leased out
////////////////////////////////////////////////////////////////////////////////

      bool _isLeased;

////////////////////////////////////////////////////////////////////////////////
/// @brief mutex for state
////////////////////////////////////////////////////////////////////////////////

      std::mutex _lock;

////////////////////////////////////////////////////////////////////////////////
/// @brief the proxy config filename
////////////////////////////////////////////////////////////////////////////////

      std::string _proxyConfFilename;

////////////////////////////////////////////////////////////////////////////////
/// @brief list of all coordinators for haproxy
////////////////////////////////////////////////////////////////////////////////

      std::string _coordinatorHAProxyList;

////////////////////////////////////////////////////////////////////////////////
/// @brief proxy pid
////////////////////////////////////////////////////////////////////////////////

      pid_t _proxyPid;

      std::atomic<int> _restartProxy;
  };
}

#endif

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
