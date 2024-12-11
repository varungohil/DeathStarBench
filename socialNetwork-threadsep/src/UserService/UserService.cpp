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
#include "UserHandler.h"
#include "httplib.h"
using namespace httplib;

using apache::thrift::protocol::TBinaryProtocolFactory;
using namespace social_network;

void sigintHandler(int sig) { exit(EXIT_SUCCESS); }

int main(int argc, char *argv[]) {
  signal(SIGINT, sigintHandler);
  init_logger();

  SetUpTracer("config/jaeger-config.yml", "user-service");

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  std::string secret = config_json["secret"];

  int port = config_json["user-service"]["port"];

  std::string social_graph_addr = config_json["social-graph-service"]["addr"];
  int social_graph_port = config_json["social-graph-service"]["port"];
  int social_graph_conns = config_json["social-graph-service"]["connections"];
  int social_graph_timeout = config_json["social-graph-service"]["timeout_ms"];
  int social_graph_keepalive =
      config_json["social-graph-service"]["keepalive_ms"];

  int mongodb_conns = config_json["user-mongodb"]["connections"];
  int mongodb_timeout = config_json["user-mongodb"]["timeout_ms"];

  int memcached_conns = config_json["user-memcached"]["connections"];
  int memcached_timeout = config_json["user-memcached"]["timeout_ms"];

  memcached_pool_st *memcached_client_pool =
      init_memcached_client_pool(config_json, "user", 32, memcached_conns);
  mongoc_client_pool_t *mongodb_client_pool =
      init_mongodb_client_pool(config_json, "user", mongodb_conns);

  if (memcached_client_pool == nullptr || mongodb_client_pool == nullptr) {
    return EXIT_FAILURE;
  }

  std::string netif = config_json["user-service"]["netif"];
  std::string machine_id = GetMachineId(netif);
  if (machine_id == "") {
    exit(EXIT_FAILURE);
  }
  LOG(info) << "machine_id = " << machine_id;

  std::mutex thread_lock;

  ClientPool<ThriftClient<SocialGraphServiceClient>> social_graph_client_pool(
      "social-graph", social_graph_addr, social_graph_port, 0,
      social_graph_conns, social_graph_timeout, social_graph_keepalive, config_json);

  mongoc_client_t *mongodb_client = mongoc_client_pool_pop(mongodb_client_pool);
  if (!mongodb_client) {
    LOG(fatal) << "Failed to pop mongoc client";
    return EXIT_FAILURE;
  }
  bool r = false;
  while (!r) {
    r = CreateIndex(mongodb_client, "user", "user_id", true);
    if (!r) {
      LOG(error) << "Failed to create mongodb index, try again";
      sleep(1);
    }
  }
  mongoc_client_pool_push(mongodb_client_pool, mongodb_client);
  std::shared_ptr<CustomNonblockingServerSocket> server_socket =
    std::make_shared<CustomNonblockingServerSocket>("0.0.0.0", port);

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  for (const auto& elem : config_json["user-service"]["cpuset_worker"]) {
    CPU_SET(elem.get<int>(), &cpuset);
  }

  auto workerThreadFactory = std::make_shared<CustomThreadFactory>(false, &cpuset);
  auto workerThreadManager = CustomThreadManager::newSimpleCustomThreadManager(
    config_json["user-service"]["num_worker_threads"]);
  workerThreadManager->threadFactory(workerThreadFactory);
  workerThreadManager->start();

  cpu_set_t cpusetio;
  CPU_ZERO(&cpusetio);
  for (const auto& elem : config_json["user-service"]["cpuset_io"]) {
    CPU_SET(elem.get<int>(), &cpusetio);
  }

  std::thread serverThread;
  CustomNonblockingServer server(
      std::make_shared<UserServiceProcessor>(std::make_shared<UserHandler>(
          &thread_lock, machine_id, secret, memcached_client_pool,
          mongodb_client_pool, &social_graph_client_pool)),
      std::make_shared<TBinaryProtocolFactory>(),
      server_socket,
      std::make_shared<CustomThreadFactory>(false, &cpusetio),
      workerThreadManager
  );
  server.setThreadManager(workerThreadManager);
  server.setNumIOThreads(config_json["user-service"]["num_io_threads"]);

  LOG(info) << "Starting the user-service server ...";
  serverThread = std::thread([&]() {
    server.serve();
  });

  int updateCpusetPort = config_json["user-service"]["update_cpuset_port"];

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