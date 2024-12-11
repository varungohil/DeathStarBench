#include <signal.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <boost/program_options.hpp>

#include "../../gen-cpp/social_network_types.h"
#include "../ClientPool.h"
#include "../logger.h"
#include "../tracing.h"
#include "../utils.h"
#include "../utils_mongodb.h"
#include "../utils_redis.h"
#include "../utils_thrift.h"
#include "UserTimelineHandler.h"
#include "../CustomNonBlockingServer.h"
#include "../CustomNonBlockingServerSocket.h"
#include "../CustomThreadFactory.h"
#include "../CustomThreadManager.h"
#include "httplib.h"
using namespace httplib;

using apache::thrift::protocol::TBinaryProtocolFactory;
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

  SetUpTracer("config/jaeger-config.yml", "user-timeline-service");

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  int port = config_json["user-timeline-service"]["port"];

  int post_storage_port = config_json["post-storage-service"]["port"];
  std::string post_storage_addr = config_json["post-storage-service"]["addr"];
  int post_storage_conns = config_json["post-storage-service"]["connections"];
  int post_storage_timeout = config_json["post-storage-service"]["timeout_ms"];
  int post_storage_keepalive =
      config_json["post-storage-service"]["keepalive_ms"];

  int mongodb_conns = config_json["user-timeline-mongodb"]["connections"];
  int mongodb_timeout = config_json["user-timeline-mongodb"]["timeout_ms"];

  int redis_cluster_config_flag = config_json["user-timeline-redis"]["use_cluster"];
  int redis_replica_config_flag = config_json["user-timeline-redis"]["use_replica"];

  auto mongodb_client_pool =
      init_mongodb_client_pool(config_json, "user-timeline", mongodb_conns);

  if (mongodb_client_pool == nullptr) {
    return EXIT_FAILURE;
  }

  if (redis_replica_config_flag && (redis_cluster_config_flag || redis_cluster_flag)) {
      LOG(error) << "Can't start service when Redis Cluster and Redis Replica are enabled at the same time";
      exit(EXIT_FAILURE);
  }

  ClientPool<ThriftClient<PostStorageServiceClient>> post_storage_client_pool(
      "post-storage-client", post_storage_addr, post_storage_port, 0,
      post_storage_conns, post_storage_timeout, post_storage_keepalive,
      config_json);

  mongoc_client_t *mongodb_client = mongoc_client_pool_pop(mongodb_client_pool);
  if (!mongodb_client) {
    LOG(fatal) << "Failed to pop mongoc client";
    return EXIT_FAILURE;
  }
  bool r = false;
  while (!r) {
    r = CreateIndex(mongodb_client, "user-timeline", "user_id", true);
    if (!r) {
      LOG(error) << "Failed to create mongodb index, try again";
      sleep(1);
    }
  }
  mongoc_client_pool_push(mongodb_client_pool, mongodb_client);
  std::shared_ptr<CustomNonblockingServerSocket> server_socket =
      std::make_shared<CustomNonblockingServerSocket>("0.0.0.0", port);

  // Add CPU set configuration
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  for (const auto& elem : config_json["user-timeline-service"]["cpuset_worker"]) {
    CPU_SET(elem.get<int>(), &cpuset);
  }

  auto workerThreadFactory = std::make_shared<CustomThreadFactory>(false, &cpuset);
  auto workerThreadManager = CustomThreadManager::newSimpleCustomThreadManager(
      config_json["user-timeline-service"]["num_worker_threads"]);
  workerThreadManager->threadFactory(workerThreadFactory);
  workerThreadManager->start();

  cpu_set_t cpusetio;
  CPU_ZERO(&cpusetio);
  for (const auto& elem : config_json["user-timeline-service"]["cpuset_io"]) {
    CPU_SET(elem.get<int>(), &cpusetio);
  }

  std::thread serverThread;
  CustomNonblockingServer server(
      std::make_shared<UserTimelineServiceProcessor>(nullptr),  // Temporary processor
      std::make_shared<TBinaryProtocolFactory>(),
      server_socket,
      std::make_shared<CustomThreadFactory>(false, &cpusetio),
      workerThreadManager
  );

  if (redis_replica_config_flag) {
      Redis redis_replica_client_pool = init_redis_replica_client_pool(config_json, "redis-replica");
      Redis redis_primary_client_pool = init_redis_replica_client_pool(config_json, "redis-primary");
      server = CustomNonblockingServer(
          std::make_shared<UserTimelineServiceProcessor>(
              std::make_shared<UserTimelineHandler>(
                  &redis_replica_client_pool, &redis_primary_client_pool, mongodb_client_pool,
                  &post_storage_client_pool)),
          std::make_shared<TBinaryProtocolFactory>(),
          server_socket,
          std::make_shared<CustomThreadFactory>(false, &cpusetio),
          workerThreadManager
      );
      server.setThreadManager(workerThreadManager);
      server.setNumIOThreads(config_json["user-timeline-service"]["num_io_threads"]);
      LOG(info) << "Starting the user-timeline-service server with replicated Redis support...";
      serverThread = std::thread([&]() {
        server.serve();
      });
  }
  else if (redis_cluster_flag || redis_cluster_config_flag) {
    RedisCluster redis_client_pool =
        init_redis_cluster_client_pool(config_json, "user-timeline");
    server = CustomNonblockingServer(
        std::make_shared<UserTimelineServiceProcessor>(
            std::make_shared<UserTimelineHandler>(
                &redis_client_pool, mongodb_client_pool,
                &post_storage_client_pool)),
        std::make_shared<TBinaryProtocolFactory>(),
        server_socket,
        std::make_shared<CustomThreadFactory>(false, &cpusetio),
        workerThreadManager
    );
    server.setThreadManager(workerThreadManager);
    server.setNumIOThreads(config_json["user-timeline-service"]["num_io_threads"]);
    LOG(info) << "Starting the user-timeline-service server with Redis Cluster support...";
    serverThread = std::thread([&]() {
      server.serve();
    });
  }
  else {
    Redis redis_client_pool =
        init_redis_client_pool(config_json, "user-timeline");
    server = CustomNonblockingServer(
        std::make_shared<UserTimelineServiceProcessor>(
            std::make_shared<UserTimelineHandler>(
                &redis_client_pool, mongodb_client_pool,
                &post_storage_client_pool)),
        std::make_shared<TBinaryProtocolFactory>(),
        server_socket,
        std::make_shared<CustomThreadFactory>(false, &cpusetio),
        workerThreadManager
    );
    server.setThreadManager(workerThreadManager);
    server.setNumIOThreads(config_json["user-timeline-service"]["num_io_threads"]);
    LOG(info) << "Starting the user-timeline-service server...";
    serverThread = std::thread([&]() {
      server.serve();
    });
  }

  int updateCpusetPort = config_json["user-timeline-service"]["update_cpuset_port"];

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