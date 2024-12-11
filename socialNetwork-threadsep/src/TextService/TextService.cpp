#include <signal.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include "../CustomNonBlockingServer.h"
#include "../CustomNonBlockingServerSocket.h"
#include "../CustomThreadFactory.h"
#include "../CustomThreadManager.h"

#include "../utils.h"
#include "../utils_thrift.h"
#include "TextHandler.h"

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
    server.serve();
  } else
    exit(EXIT_FAILURE);
}
