#include <signal.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include "../CustomNonBlockingServer.h"
#include "../CustomNonBlockingServerSocket.h"
#include "../CustomThreadFactory.h"
#include "../CustomThreadManager.h"

#include "../utils.h"
#include "../utils_memcached.h"
#include "../utils_mongodb.h"
#include "../utils_thrift.h"
#include "UserMentionHandler.h"
#include "nlohmann/json.hpp"
#include "httplib.h"
using namespace httplib;

using apache::thrift::protocol::TBinaryProtocolFactory;
using namespace social_network;

static memcached_pool_st* memcached_client_pool;
static mongoc_client_pool_t* mongodb_client_pool;

void sigintHandler(int sig) {
  if (memcached_client_pool != nullptr) {
    memcached_pool_destroy(memcached_client_pool);
  }
  if (mongodb_client_pool != nullptr) {
    mongoc_client_pool_destroy(mongodb_client_pool);
  }
  exit(EXIT_SUCCESS);
}

int main(int argc, char* argv[]) {
  signal(SIGINT, sigintHandler);
  init_logger();
  SetUpTracer("config/jaeger-config.yml", "user-mention-service");

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  int port = config_json["user-mention-service"]["port"];

  int mongodb_conns = config_json["user-mongodb"]["connections"];
  int mongodb_timeout = config_json["user-mongodb"]["timeout_ms"];

  int memcached_conns = config_json["user-memcached"]["connections"];
  int memcached_timeout = config_json["user-memcached"]["timeout_ms"];

  memcached_client_pool =
      init_memcached_client_pool(config_json, "user", 32, memcached_conns);
  mongodb_client_pool =
      init_mongodb_client_pool(config_json, "user", mongodb_conns);
  if (memcached_client_pool == nullptr || mongodb_client_pool == nullptr) {
    return EXIT_FAILURE;
  }

  std::shared_ptr<CustomNonblockingServerSocket> server_socket =
    std::make_shared<CustomNonblockingServerSocket>("0.0.0.0", port);

  // Add CPU set configuration for workers
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  for (const auto& elem : config_json["user-mention-service"]["cpuset_worker"]) {
    CPU_SET(elem.get<int>(), &cpuset);
  }

  auto workerThreadFactory = std::make_shared<CustomThreadFactory>(false, &cpuset);
  auto workerThreadManager = CustomThreadManager::newSimpleCustomThreadManager(
    config_json["user-mention-service"]["num_worker_threads"]);
  workerThreadManager->threadFactory(workerThreadFactory);
  workerThreadManager->start();

  // Add CPU set configuration for IO threads
  cpu_set_t cpusetio;
  CPU_ZERO(&cpusetio);
  for (const auto& elem : config_json["user-mention-service"]["cpuset_io"]) {
    CPU_SET(elem.get<int>(), &cpusetio);
  }

  std::thread serverThread;
  CustomNonblockingServer server(
      std::make_shared<UserMentionServiceProcessor>(
          std::make_shared<UserMentionHandler>(
              memcached_client_pool, mongodb_client_pool)),
      std::make_shared<TBinaryProtocolFactory>(),
      server_socket,
      std::make_shared<CustomThreadFactory>(false, &cpusetio),
      workerThreadManager
  );
  server.setThreadManager(workerThreadManager);
  server.setNumIOThreads(config_json["user-mention-service"]["num_io_threads"]);

  LOG(info) << "Starting the user-mention-service server...";
  serverThread = std::thread([&]() {
    server.serve();
  });

  int updateCpusetPort = config_json["user-mention-service"]["update_cpuset_port"];

  std::thread updateCpusetThread([&]() {
    Server http_server;

    http_server.Post("/update_cpuset", [&](const Request& req, Response& res) {
      try {
        auto j = json::parse(req.body);
        
        if (!j.contains("cpu_ids") || !j.contains("is_worker")) {
          throw std::runtime_error("Request body must contain 'cpu_ids' array and 'is_worker' boolean");
        }

        if (!j["cpu_ids"].is_array()) {
          throw std::runtime_error("cpu_ids must be an array of CPU IDs");
        }

        if (!j["is_worker"].is_boolean()) {
          throw std::runtime_error("is_worker must be a boolean value");
        }

        std::vector<int> cpu_ids;
        for (const auto& cpu_id : j["cpu_ids"]) {
          if (!cpu_id.is_number()) {
            throw std::runtime_error("Each element must be a CPU ID number");
          }
          cpu_ids.push_back(cpu_id.get<int>());
        }

        bool is_worker = j["is_worker"].get<bool>();

        bool success;
        if (is_worker) {
          success = server.changeWorkerCpuset(cpu_ids);
        } else {
          success = server.changeIOCpuset(cpu_ids);
        }

        if (success) {
          res.set_content("CPU set updated successfully", "text/plain");
        } else {
          res.status = 500;
          res.set_content("Failed to update CPU set - thread manager not initialized", "text/plain");
        }
        
      } catch (const std::exception& e) {
        res.status = 400;
        res.set_content(std::string("Failed to update CPU set: ") + e.what(), "text/plain");
      }
    });

    http_server.listen("0.0.0.0", updateCpusetPort);
    std::cout << "Update CPU set server is listening at port " << updateCpusetPort << std::endl;
    while (true);
  });

  serverThread.join();
  updateCpusetThread.join();
}