#include <signal.h>
#include <thrift/protocol/TBinaryProtocol.h>
// #include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
// #include <thrift/transport/TServerSocket.h>

#include "../utils.h"
#include "../utils_thrift.h"
#include "MediaHandler.h"

#include "../CustomNonBlockingServer.h"
#include "../CustomNonBlockingServerSocket.h"
#include "../CustomThreadFactory.h"
#include "../CustomThreadManager.h"

using apache::thrift::protocol::TBinaryProtocolFactory;
// using apache::thrift::server::TThreadedServer;
using apache::thrift::transport::TFramedTransportFactory;
// using apache::thrift::transport::TServerSocket;
using namespace social_network;

void sigintHandler(int sig) { exit(EXIT_SUCCESS); }

int main(int argc, char *argv[]) {
  signal(SIGINT, sigintHandler);
  init_logger();
  SetUpTracer("config/jaeger-config.yml", "media-service");
  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  int port = config_json["media-service"]["port"];
  std::shared_ptr<CustomNonblockingServerSocket> server_socket =
    std::make_shared<CustomNonblockingServerSocket>( "0.0.0.0", port);


  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  for (const auto& elem : config_json["media-service"]["cpuset_worker"]) {
    CPU_SET(elem.get<int>(), &cpuset);
  }

  auto workerThreadFactory = std::make_shared<CustomThreadFactory>(false, &cpuset);
  auto workerThreadManager = CustomThreadManager::newSimpleCustomThreadManager(config_json["home-timeline-service"]["num_worker_threads"]); // Number of worker threads
  workerThreadManager->threadFactory(workerThreadFactory);
  workerThreadManager->start(); 

  cpu_set_t cpusetio;
  CPU_ZERO(&cpusetio);
  for (const auto& elem : config_json["media-service"]["cpuset_io"]) {
    CPU_SET(elem.get<int>(), &cpusetio);
  }

  CustomNonblockingServer server(
      std::make_shared<MediaServiceProcessor>(
          std::make_shared<MediaHandler>()),
      std::make_shared<TBinaryProtocolFactory>(),
      server_socket,
      std::make_shared<CustomThreadFactory>(false, &cpusetio),
      workerThreadManager
  );
  server.setThreadManager(workerThreadManager);
  server.setNumIOThreads(config_json["media-service"]["num_io_threads"]);

  LOG(info) << "Starting the media-service server...";
  server.serve();
}
