/*
 * 64-bit Unique Id Generator
 *
 * ------------------------------------------------------------------------
 * |0| 11 bit machine ID |      40-bit timestamp         | 12-bit counter |
 * ------------------------------------------------------------------------
 *
 * 11-bit machine Id code by hasing the MAC address
 * 40-bit UNIX timestamp in millisecond precision with custom epoch
 * 12 bit counter which increases monotonically on single process
 *
 */

#include <signal.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include "../CustomNonBlockingServer.h"
#include "../CustomNonBlockingServerSocket.h"
#include "../CustomThreadFactory.h"
#include "../CustomThreadManager.h"

#include "../utils.h"
#include "../utils_thrift.h"
#include "UniqueIdHandler.h"
#include "httplib.h"
using namespace httplib;

using apache::thrift::protocol::TBinaryProtocolFactory;
using namespace social_network;

void sigintHandler(int sig) { exit(EXIT_SUCCESS); }

int main(int argc, char *argv[]) {
  signal(SIGINT, sigintHandler);
  init_logger();
  SetUpTracer("config/jaeger-config.yml", "unique-id-service");

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  int port = config_json["unique-id-service"]["port"];
  std::string netif = config_json["unique-id-service"]["netif"];

  std::string machine_id = GetMachineId(netif);
  if (machine_id == "") {
    exit(EXIT_FAILURE);
  }
  LOG(info) << "machine_id = " << machine_id;

  std::mutex thread_lock;
  std::shared_ptr<CustomNonblockingServerSocket> server_socket =
    std::make_shared<CustomNonblockingServerSocket>("0.0.0.0", port);

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  for (const auto& elem : config_json["unique-id-service"]["cpuset_worker"]) {
    CPU_SET(elem.get<int>(), &cpuset);
  }

  auto workerThreadFactory = std::make_shared<CustomThreadFactory>(false, &cpuset);
  auto workerThreadManager = CustomThreadManager::newSimpleCustomThreadManager(
    config_json["unique-id-service"]["num_worker_threads"]);
  workerThreadManager->threadFactory(workerThreadFactory);
  workerThreadManager->start();

  cpu_set_t cpusetio;
  CPU_ZERO(&cpusetio);
  for (const auto& elem : config_json["unique-id-service"]["cpuset_io"]) {
    CPU_SET(elem.get<int>(), &cpusetio);
  }

  std::thread serverThread;
  CustomNonblockingServer server(
      std::make_shared<UniqueIdServiceProcessor>(
          std::make_shared<UniqueIdHandler>(&thread_lock, machine_id)),
      std::make_shared<TBinaryProtocolFactory>(),
      server_socket,
      std::make_shared<CustomThreadFactory>(false, &cpusetio),
      workerThreadManager
  );
  server.setThreadManager(workerThreadManager);
  server.setNumIOThreads(config_json["unique-id-service"]["num_io_threads"]);

  LOG(info) << "Starting the unique-id-service server...";
  serverThread = std::thread([&]() {
    server.serve();
  });

  int updateCpusetPort = config_json["unique-id-service"]["update_cpuset_port"];

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
