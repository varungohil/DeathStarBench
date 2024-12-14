#include <signal.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <boost/program_options.hpp>

#include "../utils.h"
#include "../utils_mongodb.h"
#include "../utils_redis.h"
#include "../utils_thrift.h"
#include "SocialGraphHandler.h"
#include "../CustomNonBlockingServer.h"
#include "../CustomNonBlockingServerSocket.h"
#include "../CustomThreadFactory.h"
#include "../CustomThreadManager.h"
#include "httplib.h"
using namespace httplib;

using json = nlohmann::json;
using apache::thrift::protocol::TBinaryProtocolFactory;
using apache::thrift::transport::TFramedTransportFactory;
using namespace social_network;

void sigintHandler(int sig) { exit(EXIT_SUCCESS); }

int main(int argc, char *argv[]) {
  signal(SIGINT, sigintHandler);
  init_logger();

  // Command line options
  namespace po = boost::program_options;
  po::options_description desc("Options");
  desc.add_options()("help", "produce help message")(
      "redis-cluster",
      po::value<bool>()->default_value(false)->implicit_value(true),
      "Enable redis cluster mode");

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << "\n";
    return 0;
  }

  bool redis_cluster_flag = false;
  if (vm.count("redis-cluster")) {
    if (vm["redis-cluster"].as<bool>()) {
      redis_cluster_flag = true;
    }
  }

  SetUpTracer("config/jaeger-config.yml", "social-graph-service");

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  int port = config_json["social-graph-service"]["port"];

  int mongodb_conns = config_json["social-graph-mongodb"]["connections"];
  int mongodb_timeout = config_json["social-graph-mongodb"]["timeout_ms"];

  std::string user_addr = config_json["user-service"]["addr"];
  int user_port = config_json["user-service"]["port"];
  int user_conns = config_json["user-service"]["connections"];
  int user_timeout = config_json["user-service"]["timeout_ms"];
  int user_keepalive = config_json["user-service"]["keepalive_ms"];

  int redis_cluster_config_flag = config_json["social-graph-redis"]["use_cluster"];
  int redis_replica_config_flag = config_json["social-graph-redis"]["use_replica"];
  mongoc_client_pool_t *mongodb_client_pool =
      init_mongodb_client_pool(config_json, "social-graph", mongodb_conns);

  if (mongodb_client_pool == nullptr) {
    return EXIT_FAILURE;
  }

  if (redis_replica_config_flag && (redis_cluster_config_flag || redis_cluster_flag)) {
      LOG(error) << "Can't start service when Redis Cluster and Redis Replica are enabled at the same time";
      exit(EXIT_FAILURE);
  }

  ClientPool<ThriftClient<UserServiceClient>> user_client_pool(
      "social-graph", user_addr, user_port, 0, user_conns, user_timeout,
      user_keepalive, config_json);

  mongoc_client_t *mongodb_client = mongoc_client_pool_pop(mongodb_client_pool);
  if (!mongodb_client) {
    LOG(fatal) << "Failed to pop mongoc client";
    return EXIT_FAILURE;
  }
  bool r = false;
  while (!r) {
    r = CreateIndex(mongodb_client, "social-graph", "user_id", true);
    if (!r) {
      LOG(error) << "Failed to create mongodb index, try again";
      sleep(1);
    }
  }
  mongoc_client_pool_push(mongodb_client_pool, mongodb_client);

  std::shared_ptr<CustomNonblockingServerSocket> server_socket =
    std::make_shared<CustomNonblockingServerSocket>("0.0.0.0", port);

  // Add thread management configuration
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  for (const auto& elem : config_json["social-graph-service"]["cpuset_worker"]) {
    CPU_SET(elem.get<int>(), &cpuset);
  }

  auto workerThreadFactory = std::make_shared<CustomThreadFactory>(false, &cpuset);
  auto workerThreadManager = CustomThreadManager::newSimpleCustomThreadManager(
    config_json["social-graph-service"]["num_worker_threads"]);
  workerThreadManager->threadFactory(workerThreadFactory);
  workerThreadManager->start();

  cpu_set_t cpusetio;
  CPU_ZERO(&cpusetio);
  for (const auto& elem : config_json["social-graph-service"]["cpuset_io"]) {
    CPU_SET(elem.get<int>(), &cpusetio);
  }

  if (redis_cluster_flag || redis_cluster_config_flag) {
    RedisCluster redis_cluster_client_pool =
        init_redis_cluster_client_pool(config_json, "social-graph");
    CustomNonblockingServer server(
        std::make_shared<SocialGraphServiceProcessor>(
            std::make_shared<SocialGraphHandler>(mongodb_client_pool,
                                               &redis_cluster_client_pool,
                                               &user_client_pool)),
        std::make_shared<TBinaryProtocolFactory>(),
        server_socket,
        std::make_shared<CustomThreadFactory>(false, &cpusetio),
        workerThreadManager
    );
    server.setThreadManager(workerThreadManager);
    server.setNumIOThreads(config_json["social-graph-service"]["num_io_threads"]);
    LOG(info) << "Starting the social-graph-service server with Redis Cluster support...";

    std::thread serverThread;
    serverThread = std::thread([&]() {
      server.serve();
    });

    int updateCpusetPort = config_json["social-graph-service"]["update_cpuset_port"];

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
      while (true);
    });

    serverThread.join();
    updateCpusetThread.join();
  }
  else if (redis_replica_config_flag) {
    Redis redis_replica_client_pool = init_redis_replica_client_pool(config_json, "redis-replica");
    Redis redis_primary_client_pool = init_redis_replica_client_pool(config_json, "redis-primary");
    CustomNonblockingServer server(
        std::make_shared<SocialGraphServiceProcessor>(
            std::make_shared<SocialGraphHandler>(
                mongodb_client_pool, &redis_replica_client_pool, &redis_primary_client_pool, &user_client_pool)),
        std::make_shared<TBinaryProtocolFactory>(),
        server_socket,
        std::make_shared<CustomThreadFactory>(false, &cpusetio),
        workerThreadManager
    );
    server.setThreadManager(workerThreadManager);
    server.setNumIOThreads(config_json["social-graph-service"]["num_io_threads"]);
    LOG(info) << "Starting the social-graph-service server with Redis replica support";

    std::thread serverThread;
    serverThread = std::thread([&]() {
      server.serve();
    });

    int updateCpusetPort = config_json["social-graph-service"]["update_cpuset_port"];

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
      while (true);
    });

    serverThread.join();
    updateCpusetThread.join();
  }
  else {
    Redis redis_client_pool = init_redis_client_pool(config_json, "social-graph");
    CustomNonblockingServer server(
        std::make_shared<SocialGraphServiceProcessor>(
            std::make_shared<SocialGraphHandler>(
                mongodb_client_pool, &redis_client_pool, &user_client_pool)),
        std::make_shared<TBinaryProtocolFactory>(),
        server_socket,
        std::make_shared<CustomThreadFactory>(false, &cpusetio),
        workerThreadManager
    );
    server.setThreadManager(workerThreadManager);
    server.setNumIOThreads(config_json["social-graph-service"]["num_io_threads"]);
    LOG(info) << "Starting the social-graph-service server ...";

    std::thread serverThread;
    serverThread = std::thread([&]() {
      server.serve();
    });

    int updateCpusetPort = config_json["social-graph-service"]["update_cpuset_port"];

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
      while (true);
    });

    serverThread.join();
    updateCpusetThread.join();
  }
}