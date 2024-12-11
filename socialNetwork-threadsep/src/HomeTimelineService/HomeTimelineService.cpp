#include <signal.h>
#include <thrift/protocol/TBinaryProtocol.h>
// #include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
// #include <thrift/transport/TServerSocket.h>

#include <boost/program_options.hpp>

#include "../ClientPool.h"
#include "../logger.h"
#include "../tracing.h"
#include "../utils.h"
#include "../utils_redis.h"
#include "../utils_thrift.h"
#include "HomeTimelineHandler.h"

#include "../CustomNonBlockingServer.h"
#include "../CustomNonBlockingServerSocket.h"
#include "../CustomThreadFactory.h"
#include "../CustomThreadManager.h"
// #include "../../gen-cpp/HomeTimelineService.h"

#include "httplib.h"
using namespace httplib;

using apache::thrift::protocol::TBinaryProtocolFactory;
// using apache::thrift::server::TThreadedServer;
using apache::thrift::transport::TFramedTransportFactory;
// using apache::thrift::transport::TServerSocket;
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

  SetUpTracer("config/jaeger-config.yml", "home-timeline-service");

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  int port = config_json["home-timeline-service"]["port"];
  int redis_cluster_config_flag = config_json["home-timeline-redis"]["use_cluster"];

  int redis_replica_config_flag = config_json["home-timeline-redis"]["use_replica"];

  int post_storage_port = config_json["post-storage-service"]["port"];
  std::string post_storage_addr = config_json["post-storage-service"]["addr"];
  int post_storage_conns = config_json["post-storage-service"]["connections"];
  int post_storage_timeout = config_json["post-storage-service"]["timeout_ms"];
  int post_storage_keepalive =
      config_json["post-storage-service"]["keepalive_ms"];

  int social_graph_port = config_json["social-graph-service"]["port"];
  std::string social_graph_addr = config_json["social-graph-service"]["addr"];
  int social_graph_conns = config_json["social-graph-service"]["connections"];
  int social_graph_timeout = config_json["social-graph-service"]["timeout_ms"];
  int social_graph_keepalive =
      config_json["social-graph-service"]["keepalive_ms"];

  if (redis_replica_config_flag && (redis_cluster_config_flag || redis_cluster_flag)) {
      LOG(error) << "Can't start service when Redis Cluster and Redis Replica are enabled at the same time";
      exit(EXIT_FAILURE);
  }

  ClientPool<ThriftClient<PostStorageServiceClient>> post_storage_client_pool(
      "post-storage-client", post_storage_addr, post_storage_port, 0,
      post_storage_conns, post_storage_timeout, post_storage_keepalive,
      config_json);

  ClientPool<ThriftClient<SocialGraphServiceClient>> social_graph_client_pool(
      "social-graph-client", social_graph_addr, social_graph_port, 0,
      social_graph_conns, social_graph_timeout, social_graph_keepalive,
      config_json);

  std::shared_ptr<CustomNonblockingServerSocket> server_socket =
      std::make_shared<CustomNonblockingServerSocket>( "0.0.0.0", port);


  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  for (const auto& elem : config_json["home-timeline-service"]["cpuset_worker"]) {
    CPU_SET(elem.get<int>(), &cpuset);
  }

  auto workerThreadFactory = std::make_shared<CustomThreadFactory>(false, &cpuset);
  auto workerThreadManager = CustomThreadManager::newSimpleCustomThreadManager(config_json["home-timeline-service"]["num_worker_threads"]); // Number of worker threads
  workerThreadManager->threadFactory(workerThreadFactory);
  workerThreadManager->start(); 

  cpu_set_t cpusetio;
  CPU_ZERO(&cpusetio);
  for (const auto& elem : config_json["home-timeline-service"]["cpuset_io"]) {
    CPU_SET(elem.get<int>(), &cpusetio);
  }


  std::thread serverThread;
  CustomNonblockingServer server(nullptr, nullptr, nullptr, nullptr, nullptr); // Default construct first

  if (redis_replica_config_flag) {
          Redis redis_replica_client_pool = init_redis_replica_client_pool(config_json, "redis-replica");
          Redis redis_primary_client_pool = init_redis_replica_client_pool(config_json, "redis-primary");

          server = CustomNonblockingServer(
              std::make_shared<HomeTimelineServiceProcessor>(
                  std::make_shared<HomeTimelineHandler>(&redis_replica_client_pool,
                                                        &redis_primary_client_pool,
                                                        &post_storage_client_pool,
                                                        &social_graph_client_pool)),
              std::make_shared<TBinaryProtocolFactory>(),
              server_socket,
              std::make_shared<CustomThreadFactory>(false, &cpusetio),
              workerThreadManager
          );

          LOG(info) << "Starting the home-timeline-service server with replicated Redis support...";
          server.setThreadManager(workerThreadManager);
          server.setNumIOThreads(config_json["home-timeline-service"]["num_io_threads"]);
          serverThread = std::thread([&]() {
            server.serve();
          });      
  }
  else if (redis_cluster_flag || redis_cluster_config_flag) {
          RedisCluster redis_cluster_client_pool =
              init_redis_cluster_client_pool(config_json, "home-timeline");

          server = CustomNonblockingServer(
              std::make_shared<HomeTimelineServiceProcessor>(
                  std::make_shared<HomeTimelineHandler>(&redis_cluster_client_pool,
                                                        &post_storage_client_pool,
                                                        &social_graph_client_pool)),
              std::make_shared<TBinaryProtocolFactory>(),
              server_socket,
              std::make_shared<CustomThreadFactory>(false, &cpusetio),
              workerThreadManager
          );

          LOG(info) << "Starting the home-timeline-service server with Redis Cluster support...";
          server.setThreadManager(workerThreadManager);
          server.setNumIOThreads(config_json["home-timeline-service"]["num_io_threads"]);
          serverThread = std::thread([&]() {
            server.serve();
          });
  } else {
          Redis redis_client_pool =
              init_redis_client_pool(config_json, "home-timeline");

          server = CustomNonblockingServer(
              std::make_shared<HomeTimelineServiceProcessor>(
                  std::make_shared<HomeTimelineHandler>(&redis_client_pool,
                                                        &post_storage_client_pool,
                                                        &social_graph_client_pool)),
              std::make_shared<TBinaryProtocolFactory>(),
              server_socket,
              std::make_shared<CustomThreadFactory>(false, &cpusetio),
              workerThreadManager
          );

          LOG(info) << "Starting the home-timeline-service server...";
          server.setThreadManager(workerThreadManager);
          server.setNumIOThreads(config_json["home-timeline-service"]["num_io_threads"]);
          serverThread = std::thread([&]() {
            server.serve();
          });
  }
  int updateCpusetPort =  config_json["home-timeline-service"]["update_cpuset_port"];

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
