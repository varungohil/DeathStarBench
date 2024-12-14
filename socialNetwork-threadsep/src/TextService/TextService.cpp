#include <signal.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include "../CustomNonBlockingServer.h"
#include "../CustomNonBlockingServerSocket.h"
#include "../CustomThreadFactory.h"
#include "../CustomThreadManager.h"

#include "../utils.h"
#include "../utils_thrift.h"
#include "TextHandler.h"
#include "httplib.h"
using namespace httplib;

using apache::thrift::protocol::TBinaryProtocolFactory;
using namespace social_network;

void sigintHandler(int sig) { exit(EXIT_SUCCESS); }

int main(int argc, char *argv[]) {
  signal(SIGINT, sigintHandler);
  init_logger();
  SetUpTracer("config/jaeger-config.yml", "text-service");

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) == 0) {
    int port = config_json["text-service"]["port"];

    std::string url_addr = config_json["url-shorten-service"]["addr"];
    int url_port = config_json["url-shorten-service"]["port"];
    int url_conns = config_json["url-shorten-service"]["connections"];
    int url_timeout = config_json["url-shorten-service"]["timeout_ms"];
    int url_keepalive = config_json["url-shorten-service"]["keepalive_ms"];

    std::string user_mention_addr = config_json["user-mention-service"]["addr"];
    int user_mention_port = config_json["user-mention-service"]["port"];
    int user_mention_conns = config_json["user-mention-service"]["connections"];
    int user_mention_timeout =
        config_json["user-mention-service"]["timeout_ms"];
    int user_mention_keepalive =
        config_json["user-mention-service"]["keepalive_ms"];

    ClientPool<ThriftClient<UrlShortenServiceClient>> url_client_pool(
        "url-shorten-service", url_addr, url_port, 0, url_conns, url_timeout,
        url_keepalive, config_json);

    ClientPool<ThriftClient<UserMentionServiceClient>> user_mention_pool(
        "user-mention-service", user_mention_addr, user_mention_port, 0,
        user_mention_conns, user_mention_timeout, user_mention_keepalive, config_json);

    std::shared_ptr<CustomNonblockingServerSocket> server_socket =
        std::make_shared<CustomNonblockingServerSocket>("0.0.0.0", port);

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for (const auto& elem : config_json["text-service"]["cpuset_worker"]) {
        CPU_SET(elem.get<int>(), &cpuset);
    }

    auto workerThreadFactory = std::make_shared<CustomThreadFactory>(false, &cpuset);
    auto workerThreadManager = CustomThreadManager::newSimpleCustomThreadManager(
        config_json["text-service"]["num_worker_threads"]);
    workerThreadManager->threadFactory(workerThreadFactory);
    workerThreadManager->start();

    cpu_set_t cpusetio;
    CPU_ZERO(&cpusetio);
    for (const auto& elem : config_json["text-service"]["cpuset_io"]) {
        CPU_SET(elem.get<int>(), &cpusetio);
    }

    CustomNonblockingServer server(
        std::make_shared<TextServiceProcessor>(
            std::make_shared<TextHandler>(&url_client_pool, &user_mention_pool)),
        std::make_shared<TBinaryProtocolFactory>(),
        server_socket,
        std::make_shared<CustomThreadFactory>(false, &cpusetio),
        workerThreadManager
    );
    server.setThreadManager(workerThreadManager);
    server.setNumIOThreads(config_json["text-service"]["num_io_threads"]);

    LOG(info) << "Starting the text-service server...";
    std::thread serverThread([&]() {
      server.serve();
    });

    int updateCpusetPort = config_json["text-service"]["update_cpuset_port"];

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
  } else
    exit(EXIT_FAILURE);
}
