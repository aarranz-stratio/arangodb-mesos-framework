////////////////////////////////////////////////////////////////////////////////
/// @brief utilities
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

/*
  Base64 encode/decode adapted from here: https://gist.github.com/barrysteyn/7308212

  OpenSSL Base64 Encoding: Binary Safe and Portable
=================================================

Herewith is an example of encoding to and from base64 using OpenSSL's C library. Code presented here is both binary safe, and portable (i.e. it should work on any Posix compliant system e.g. FreeBSD and Linux).

License
=======
The MIT License (MIT)

Copyright (c) 2013 Barry Steyn

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#include "utils.h"

#include <time.h>
#include <string>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <picojson.h>
#include <sstream>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>

#include "Global.h"

using namespace arangodb;
using namespace std;

typedef std::unordered_map<std::string, std::string> Headers;

// -----------------------------------------------------------------------------
// --SECTION--                                                 private functions
// -----------------------------------------------------------------------------

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts diskspace from a resource
///////////////////////////////////////////////////////////////////////////////

double arangodb::diskspaceResource (const mesos::Resource& resource) {
  if (resource.name() == "disk" && resource.type() == mesos::Value::SCALAR) {
    return resource.scalar().value();
  }

  return 0.0;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts cpus from a resource
///////////////////////////////////////////////////////////////////////////////

static double cpusResource (const mesos::Resource& resource) {
  if (resource.name() == "cpus" && resource.type() == mesos::Value::SCALAR) {
    return resource.scalar().value();
  }

  return 0.0;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts memory from a resource
///////////////////////////////////////////////////////////////////////////////

static double memoryResource (const mesos::Resource& resource) {
  if (resource.name() == "mem" && resource.type() == mesos::Value::SCALAR) {
    return resource.scalar().value();
  }

  return 0;
}

// -----------------------------------------------------------------------------
// --SECTION--                                                  public functions
// -----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////////////
/// @brief computes a FNV hash for strings
////////////////////////////////////////////////////////////////////////////////

uint64_t arangodb::FnvHashString (const vector<string>& texts) {
  uint64_t nMagicPrime = 0x00000100000001b3ULL;
  uint64_t nHashVal = 0xcbf29ce484222325ULL;

  for (auto text : texts) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(text.c_str());
    const uint8_t* e = p + text.size();

    for (; p < e;  ++p) {
      nHashVal ^= *p;
      nHashVal *= nMagicPrime;
    }
  }

  return nHashVal;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief splits a string
////////////////////////////////////////////////////////////////////////////////

vector<string> arangodb::split (const string& value, char separator) {
  vector<string> result;
  string::size_type p = 0;
  string::size_type q;

  while ((q = value.find(separator, p)) != string::npos) {
    result.emplace_back(value, p, q - p);
    p = q + 1;
  }

  result.emplace_back(value, p);
  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief joins a vector of string
////////////////////////////////////////////////////////////////////////////////

string arangodb::join (const vector<string>& value, string separator) {
  string result = "";
  string sep = "";

  for (const auto& v : value) {
    result += sep + v;
    sep = separator;
  }

  return result;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief jsonify a protobuf message
////////////////////////////////////////////////////////////////////////////////

string arangodb::toJson (::google::protobuf::Message const& msg) {
  string result;
  pbjson::pb2json(&msg, result);
  return result;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts diskspace from resources
///////////////////////////////////////////////////////////////////////////////

double arangodb::diskspace (const mesos::Resources& resources) {
  double value = 0;

  for (const auto& resource : resources) {
    value += diskspaceResource(resource);
  }

  return value;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts cpus from resources
///////////////////////////////////////////////////////////////////////////////

double arangodb::cpus (const mesos::Resources& resources) {
  double value = 0;

  for (const auto& resource : resources) {
    value += cpusResource(resource);
  }

  return value;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts memory from resources
///////////////////////////////////////////////////////////////////////////////

double arangodb::memory (const mesos::Resources& resources) {
  double value = 0;

  for (const auto& resource : resources) {
    value += memoryResource(resource);
  }

  return value;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief converts system time
///////////////////////////////////////////////////////////////////////////////

string arangodb::toStringSystemTime (const chrono::system_clock::time_point& tp) {
  time_t tt = chrono::system_clock::to_time_t(tp);

  char buf[1024];
  strftime(buf, sizeof(buf) - 1, "%F %T", localtime(&tt));

  return buf;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief not-a-port filter
///////////////////////////////////////////////////////////////////////////////

bool arangodb::notIsPorts (const mesos::Resource& resource) {
  return resource.name() != "ports";
}

///////////////////////////////////////////////////////////////////////////////
/// @brief a-port filter
///////////////////////////////////////////////////////////////////////////////

bool arangodb::isPorts (const mesos::Resource& resource) {
  return resource.name() == "ports";
}

///////////////////////////////////////////////////////////////////////////////
/// @brief throw away rounding errors
///////////////////////////////////////////////////////////////////////////////

bool arangodb::isNoRoundingError (const mesos::Resource& resource) {
  return resource.type() != mesos::Value::SCALAR ||
         fabs(resource.scalar().value()) > 1e-6;
}

///////////////////////////////////////////////////////////////////////////////
/// @brief is-a-disk filter
///////////////////////////////////////////////////////////////////////////////

bool arangodb::isDisk (const mesos::Resource& resource) {
  return resource.name() == "disk";
}

///////////////////////////////////////////////////////////////////////////////
/// @brief is-not-a-disk filter
///////////////////////////////////////////////////////////////////////////////

bool arangodb::notIsDisk (const mesos::Resource& resource) {
  return resource.name() != "disk";
}

///////////////////////////////////////////////////////////////////////////////
/// @brief is-default-role filter
///////////////////////////////////////////////////////////////////////////////

bool arangodb::isDefaultRole (mesos::Resource const& resource) {
  return ! resource.has_role() || resource.role() == "*";
}

///////////////////////////////////////////////////////////////////////////////
/// @brief extracts number of avaiable ports from an offer, if the given
/// role string is empty, port ranges with any role are counted, otherwise
/// we only count port ranges with that role.
///////////////////////////////////////////////////////////////////////////////

size_t arangodb::numberPorts (mesos::Offer const& offer,
                              std::string const& role) {
  size_t value = 0;

  for (int i = 0; i < offer.resources_size(); ++i) {
    auto const& resource = offer.resources(i);

    if (resource.name() == "ports" &&
        resource.type() == mesos::Value::RANGES) {
      if (role.empty() ||
          (resource.has_role() && resource.role() == role)) {
           // note that role is optional but has a default, therefore
           // has_role() should always be true. Should, for whatever reason,
           // no role be set in the offer, we do not count this range!
        const auto& ranges = resource.ranges();

        for (int j = 0; j < ranges.range_size(); ++j) {
          const auto& range = ranges.range(j);

          value += range.end() - range.begin() + 1;
        }
      }
    }
  }

  return value;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the disk resources
////////////////////////////////////////////////////////////////////////////////

mesos::Resources arangodb::filterIsDisk (const mesos::Resources& resources) {
  return resources.filter(isDisk);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief filters out negligible resources due to rounding errors
////////////////////////////////////////////////////////////////////////////////

mesos::Resources arangodb::filterNoRoundingError (const mesos::Resources& resources) {
  return resources.filter(isNoRoundingError);
}


////////////////////////////////////////////////////////////////////////////////
/// @brief returns the non-disk resources
////////////////////////////////////////////////////////////////////////////////

mesos::Resources arangodb::filterNotIsDisk (const mesos::Resources& resources) {
  return resources.filter(notIsDisk);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the persistent-volume resources
////////////////////////////////////////////////////////////////////////////////

mesos::Resources arangodb::filterIsPersistentVolume (const mesos::Resources& resources) {
  return resources.filter(mesos::Resources::isPersistentVolume);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the non-port resources
////////////////////////////////////////////////////////////////////////////////

mesos::Resources arangodb::filterNotIsPorts (const mesos::Resources& resources) {
  return resources.filter(notIsPorts);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief intersect two sets of resources
////////////////////////////////////////////////////////////////////////////////

mesos::Resources arangodb::intersectResources (mesos::Resources const& a,
                                               mesos::Resources const& b) {
  return a-(a-b);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief returns the default role resources
////////////////////////////////////////////////////////////////////////////////

mesos::Resources arangodb::filterIsDefaultRole (const mesos::Resources& resources) {
  return resources.filter(isDefaultRole);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief do a GET request using libcurl
////////////////////////////////////////////////////////////////////////////////

static size_t WriteMemoryCallback(void* contents, size_t size, size_t nmemb,
                                  void *userp) {
  size_t realsize = size * nmemb;
  std::string* mem = static_cast<std::string*>(userp);

  mem->append((char*) contents, realsize);

  return realsize;
}

struct ReadInput {
  std::string const* input;
  size_t pos;

  ReadInput (std::string const* i) : input(i), pos(0) {
  }
};

static size_t ReadMemoryCallback(void* contents, size_t size, size_t nmemb,
                                 void* userp) {
  size_t realsize = size * nmemb;
  auto input = static_cast<ReadInput*>(userp);
  size_t available = input->input->size() - input->pos;
  if (available == 0) {
    return 0;
  }
  if (realsize >= available) {
    memcpy(contents, input->input->c_str() + input->pos, available);
    input->pos += available;
    return available;
  }
  else {
    memcpy(contents, input->input->c_str() + input->pos, realsize);
    input->pos += realsize;
    return realsize;
  }
}

static std::string base64UrlEncode(std::string const& str) {
  // mop: EEK! openssl :S
  BIO *bio, *b64;
  BUF_MEM *bufferPtr;

  b64 = BIO_new(BIO_f_base64());
  bio = BIO_new(BIO_s_mem());
  bio = BIO_push(b64, bio);

  BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL); //Ignore newlines - write everything in one line
  BIO_write(bio, str.c_str(), str.length());
  BIO_flush(bio);
  BIO_get_mem_ptr(bio, &bufferPtr);
  BIO_set_close(bio, BIO_NOCLOSE);
  BIO_free_all(bio);
  
  std::string base64((*bufferPtr).data, (*bufferPtr).length);

  bool isFiller = false;
  std::transform(base64.begin(), base64.end(), base64.begin(), [](unsigned char c) {
    switch(c) {
      case '+':
        return (unsigned char) '-';
        break;
      case '/':
        return (unsigned char) '_';
        break;
      default:
        return (unsigned char) c;
    }
  });
  if (base64.at(base64.length() - 1) == '=') {
    base64.resize(base64.length() - 1);
  }
  return base64;
}

static std::string createJwt(picojson::value const& body, std::string const& key) {
  EVP_MD* evp_md = const_cast<EVP_MD*>(EVP_sha256());
  
  picojson::object headerObject;
  headerObject["alg"] = picojson::value("HS256");
  headerObject["typ"] = picojson::value("JWT");

  picojson::value header(headerObject);

  std::string const& message = base64UrlEncode(header.serialize())
    + "." + base64UrlEncode(body.serialize());
  
  std::string signature(EVP_MAX_MD_SIZE + 1, '\0');
  unsigned int md_len;

  HMAC(evp_md, key.c_str(), (int)key.size(), (const unsigned char*) message.c_str(), message.size(),
       (unsigned char*) signature.c_str(), &md_len);
  
  signature.resize(md_len);
  return message + "." + base64UrlEncode(signature);
}

static Headers createClusterHeaders() {
  Headers headers = {};
  std::string const& jwtSecret = Global::arangoDBJwtSecret();
  if (!jwtSecret.empty()) {
    picojson::object payload;
    payload["iss"] = picojson::value("arangodb");
    payload["server_id"] = picojson::value("mesos_framework");
    headers["Authorization"] = "bearer " + createJwt(picojson::value(payload), jwtSecret);
  }
  return headers;
}

static int executeHTTPGet (std::string url, Headers const& headers, std::string& resultBody, long& httpCode) {
  CURL *curl;
  CURLcode res;

  curl = curl_easy_init();

  resultBody.clear();

  if (curl) {
    struct curl_slist* requestHeaders = nullptr;
    if (headers.size() > 0) {
      for (auto const& it: headers) {
        std::string const header(it.first + ": " + it.second);
        requestHeaders = curl_slist_append(requestHeaders, header.c_str());
      }
      curl_easy_setopt(curl, CURLOPT_HTTPHEADER, requestHeaders);
    }
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*) &resultBody);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "libcurl-agent/1.0");
    // mop: XXX :S CURLE 51 and 60...
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

    res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
      LOG(WARNING)
        << "cannot connect to " << url << ", curl error: " << res;
    }
    else {
      httpCode = 0;
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
    }
    curl_easy_cleanup(curl);
    if (requestHeaders != nullptr) {
      curl_slist_free_all(requestHeaders);
    }
    return res;
  }
  else {
    return -1;  // indicate that curl did not properly initialize
  }
}

int arangodb::doHTTPGet (std::string url, std::string& resultBody,
                         long& httpCode) {
  return executeHTTPGet(url, {}, resultBody, httpCode);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief do a POST request using libcurl, a return value of 0 means
/// OK, the input body is in body, in the end, the body of the result is
/// in resultBody. If libcurl did not initialise properly, -1 is returned.
/// Otherwise, a positive libcurl error code (see man 3 libcurl-errors)
/// is returned.
/// If the result is 0, then httpCode is set to the resulting HTTP code.
////////////////////////////////////////////////////////////////////////////////

static int executeHTTPPost (std::string url, Headers const& headers, std::string const& body,
                                           std::string& resultBody,
                                           long& httpCode) {
  CURL *curl;
  CURLcode res;

  curl = curl_easy_init();

  if (curl) {
    struct curl_slist* requestHeaders = nullptr;
    if (headers.size() > 0) {
      for (auto const& it: headers) {
        std::string const header(it.first + ": " + it.second);
        requestHeaders = curl_slist_append(requestHeaders, header.c_str());
      }
      curl_easy_setopt(curl, CURLOPT_HTTPHEADER, requestHeaders);
    }
    resultBody.clear();

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*) &resultBody);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "libcurl-agent/1.0");
    curl_easy_setopt(curl, CURLOPT_POST, 1);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    // mop: XXX :S CURLE 51 and 60...
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

    res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
      LOG(WARNING)
      << "cannot connect to " << url << ", curl error: " << res;
    }
    else {
      httpCode = 0;
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
    }
    curl_easy_cleanup(curl);
    if (requestHeaders != nullptr) {
      curl_slist_free_all(requestHeaders);
    }
    return res;
  }
  else {
    return -1;  // indicate that curl did not properly initialize
  }
}

int arangodb::doHTTPPost (std::string url, std::string const& body,
                                           std::string& resultBody,
                                           long& httpCode) {
  return executeHTTPPost(url, {}, body, resultBody, httpCode);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief do a PUT request using libcurl, a return value of 0 means
/// OK, the input body is in body, in the end, the body of the result is
/// in resultBody. If libcurl did not initialise properly, -1 is returned.
/// Otherwise, a positive libcurl error code (see man 3 libcurl-errors)
/// is returned.
/// If the result is 0, then httpCode is set to the resulting HTTP code.
////////////////////////////////////////////////////////////////////////////////

static int executeHTTPPut (std::string url, Headers const& headers, std::string const& body,
                                          std::string& resultBody,
                                          long& httpCode) {
  CURL *curl;
  CURLcode res;
  
  curl = curl_easy_init();

  if (curl) {
    resultBody.clear();
    struct curl_slist* requestHeaders = NULL;
    requestHeaders = curl_slist_append(requestHeaders, "Content-Type: application/x-www-form-urlencoded");
    if (headers.size() > 0) {
      for (auto const& it: headers) {
        std::string const header(it.first + ": " + it.second);
        requestHeaders = curl_slist_append(requestHeaders, header.c_str());
      }
    }

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, requestHeaders);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_READFUNCTION, ReadMemoryCallback);
    ReadInput input(&body);
    curl_easy_setopt(curl, CURLOPT_READDATA, &input);
    curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, body.size());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*) &resultBody);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "libcurl-agent/1.0");
    curl_easy_setopt(curl, CURLOPT_UPLOAD, 1);
    // mop: XXX :S CURLE 51 and 60...
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    
    res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
      LOG(WARNING)
      << "cannot connect to " << url << ", curl error: " << res;
    }
    else {
      httpCode = 0;
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
    }
    curl_easy_cleanup(curl);
    if (requestHeaders != nullptr) {
      curl_slist_free_all(requestHeaders);
    }

    return res;
  }
  else {
    return -1;  // indicate that curl did not properly initialize
  }
}

int arangodb::doHTTPPut (std::string url, std::string const& body,
                                          std::string& resultBody,
                                          long& httpCode) {
  return executeHTTPPut(url, {}, body, resultBody, httpCode);
}

////////////////////////////////////////////////////////////////////////////////
/// @brief do a DELETE request using libcurl, a return value of 0 means OK, the
/// body of the result is in resultBody. If libcurl did not initialise 
/// properly, -1 is returned and resultBody is empty, otherwise, a positive
/// libcurl error code (see man 3 libcurl-errors) is returned. 
/// If the result is 0, then httpCode is set to the resulting HTTP code.
////////////////////////////////////////////////////////////////////////////////

static int executeHTTPDelete (std::string url, Headers const& headers, std::string& resultBody,
                            long& httpCode) {
  CURL *curl;
  CURLcode res;

  curl = curl_easy_init();

  resultBody.clear();

  if (curl) {
    struct curl_slist* requestHeaders = nullptr;
    if (headers.size() > 0) {
      for (auto const& it: headers) {
        std::string const header(it.first + ": " + it.second);
        requestHeaders = curl_slist_append(requestHeaders, header.c_str());
      }
      curl_easy_setopt(curl, CURLOPT_HTTPHEADER, requestHeaders);
    }
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*) &resultBody);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "libcurl-agent/1.0");
    // mop: XXX :S CURLE 51 and 60...
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

    res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
      LOG(WARNING)
      << "cannot connect to " << url << ", curl error: " << res;
    }
    else {
      httpCode = 0;
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
    }
    curl_easy_cleanup(curl);
    if (requestHeaders != nullptr) {
      curl_slist_free_all(requestHeaders);
    }
    return res;
  }
  else {
    return -1;  // indicate that curl did not properly initialize
  }
}

int arangodb::doHTTPDelete (std::string url, std::string& resultBody,
                            long& httpCode) {
  return executeHTTPDelete(url, {}, resultBody, httpCode);
}

int arangodb::doClusterHTTPGet (std::string url, std::string& resultBody,
                         long& httpCode) {
  Headers headers = createClusterHeaders();
  return executeHTTPGet(url, headers, resultBody, httpCode);
}

int arangodb::doClusterHTTPPost (std::string url, std::string const& body,
                                           std::string& resultBody,
                                           long& httpCode) {
  Headers headers = createClusterHeaders();
  return executeHTTPPost(url, headers, body, resultBody, httpCode);
}

int arangodb::doClusterHTTPPut (std::string url, std::string const& body,
                                          std::string& resultBody,
                                          long& httpCode) {
  Headers headers = createClusterHeaders();
  return executeHTTPPut(url, headers, body, resultBody, httpCode);
}

int arangodb::doClusterHTTPDelete (std::string url, std::string& resultBody,
                            long& httpCode) {
  Headers headers = createClusterHeaders();
  return executeHTTPDelete(url, headers, resultBody, httpCode);
}


// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------
