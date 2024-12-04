package main

import (
    "context"
    "encoding/json"
    "fmt"
    // "log"
    "net/http"
    "strconv"
    // "sync"
    "time"
	"io"
    // "strings"


	"net/http/httputil"
	"net/url"
    "os"
    // "bytes"

    "github.com/apache/thrift/lib/go/thrift"
    "github.com/opentracing/opentracing-go"
    "github.com/opentracing/opentracing-go/ext"
    jaegercfg "github.com/uber/jaeger-client-go/config"
    jaegerlog "github.com/uber/jaeger-client-go/log"
    // jaegerprom "github.com/uber/jaeger-lib/metrics/prometheus"

	"gopkg.in/yaml.v2"
    "sn/gen-go/social_network"
    "github.com/sirupsen/logrus"
)

type ServiceConfig struct {
	SloUS            int       `json:"slo_us"`
	KeepaliveMs      int       `json:"keepalive_ms"`
	Addr             string    `json:"addr"`
	TimeoutMs        int       `json:"timeout_ms"`
	Port             int       `json:"port"`
	CompositionPort  int       `json:"composition_port"`
	Connections      int       `json:"connections"`
	CpusetWorker     []int     `json:"cpuset_worker"`
	CpusetIo         []int     `json:"cpuset_io"`
	CpuPairs         [][]int   `json:"cpu_pairs"`
	Freqs            []float32 `json:"freqs"`
	InitialCpuFreqMap map[string][]int `json:"initial_cpu_freq_map"`
	ModelMap         map[string][]float32 `json:"model_map"`
}

type Config struct {
	HomeTimelineService ServiceConfig `json:"home-timeline-service"`
	PostStorageService  ServiceConfig `json:"post-storage-service"`
	ComposePostService  ServiceConfig `json:"compose-post-service"`
	UserTimelineService ServiceConfig `json:"user-timeline-service"`
	UserService          ServiceConfig `json:"user-service"`
	SocialGraphService   ServiceConfig `json:"social-graph-service"`
}

var (
	frontendURL         = "http://" + os.Getenv("FRONTEND_ADDR") + ":" + os.Getenv("FRONTEND_PORT")
	frontendProxy       = httputil.NewSingleHostReverseProxy(parseURL(frontendURL))

	HomeTimelineServiceCompositionURL string
	PostStorageServiceCompositionURL  string
	ComposePostServiceCompositionURL   string
	UserTimelineServiceCompositionURL   string

	homeTimelineServiceConfig ServiceConfig
	postStorageServiceConfig  ServiceConfig
	composePostServiceConfig  ServiceConfig
	userTimelineServiceConfig ServiceConfig
	userServiceConfig         ServiceConfig
	socialGraphServiceConfig ServiceConfig

	log *logrus.Logger
)


type thriftClientWrapper struct {
    client    *social_network.HomeTimelineServiceClient
    transport thrift.TTransport
}

type ThriftClientPool struct {
    clients chan *thriftClientWrapper
    addr    string
}

func NewThriftClientPool(addr string, capacity int) *ThriftClientPool {
    pool := &ThriftClientPool{
        addr:    addr,
        clients: make(chan *thriftClientWrapper, capacity),
    }
    // Pre-allocate clients
    for i := 0; i < capacity; i++ {
        clientWrapper, err := pool.newClient()
        if err == nil {
            pool.clients <- clientWrapper
        }
    }
    return pool
}

func (p *ThriftClientPool) newClient() (*thriftClientWrapper, error) {
    transport, err := thrift.NewTSocket(p.addr)
    if err != nil {
        return nil, fmt.Errorf("error opening socket: %v", err)
    }
    protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
    transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())

    useTransport, err := transportFactory.GetTransport(transport)
    if err != nil {
        return nil, fmt.Errorf("error creating transport: %v", err)
    }

    if err := useTransport.Open(); err != nil {
        return nil, fmt.Errorf("error opening transport: %v", err)
    }

    client := social_network.NewHomeTimelineServiceClientFactory(useTransport, protocolFactory)
    return &thriftClientWrapper{client: client, transport: useTransport}, nil
}

func (p *ThriftClientPool) getClient(ctx context.Context) (*thriftClientWrapper, error) {
    select {
    case client := <-p.clients:
        return client, nil
    default:
        return p.newClient()
    }
}

func (p *ThriftClientPool) returnClient(clientWrapper *thriftClientWrapper) {
    select {
    case p.clients <- clientWrapper:
        // Client returned to pool
    default:
        // Pool is full, close the client
        clientWrapper.transport.Close()
    }
}

func (p *ThriftClientPool) ReadHomeTimeline(ctx context.Context, reqID, userID int64, start, stop int32) ([]*social_network.Post, error) {
    gcspan, ctx := opentracing.StartSpanFromContext(ctx, "GetClient")
    clientWrapper, err := p.getClient(ctx)
    if err != nil {
        return nil, err
    }
    gcspan.Finish()

    // Start a new span for the ReadHomeTimeline operation
    span, ctx := opentracing.StartSpanFromContext(ctx, "ReadHomeTimeline")
    defer span.Finish()

    // Add tags to the span
    span.SetTag("reqID", reqID)
    span.SetTag("userID", userID)
    span.SetTag("start", start)
    span.SetTag("stop", stop)
    
    // Inject the span context into the carrier (request headers)
    carrier := make(map[string]string)
    err = opentracing.GlobalTracer().Inject(span.Context(), opentracing.TextMap, opentracing.TextMapCarrier(carrier))
    if err != nil {
        log.Printf("Failed to inject span context: %v", err)
    }
    // Execute the operation with the traced context
    result, err := clientWrapper.client.ReadHomeTimeline(ctx, reqID, userID, start, stop, carrier)

    // Start a span for the returnClient call
    rcspan, _ := opentracing.StartSpanFromContext(ctx, "ReturnClient")
    defer rcspan.Finish()

    p.returnClient(clientWrapper)

    return result, err
}


type JaegerConfig struct {
    Disabled  bool `yaml:"disabled"`
    Reporter  struct {
        LogSpans           bool   `yaml:"logSpans"`
        LocalAgentHostPort string `yaml:"localAgentHostPort"`
        QueueSize          int    `yaml:"queueSize"`
        BufferFlushInterval int   `yaml:"bufferFlushInterval"`
    } `yaml:"reporter"`
    Sampler struct {
        Type  string  `yaml:"type"`
        Param float64 `yaml:"param"`
    } `yaml:"sampler"`
}

func initJaeger(service string) (opentracing.Tracer, io.Closer, error) {
    // Read Jaeger config from jaeger-config.yml
    jaegerConfigFile, err := os.Open("jaeger-config.yml")
    if err != nil {
        return nil, nil, fmt.Errorf("could not open Jaeger config file: %w", err)
    }
    defer jaegerConfigFile.Close()

    var cfg JaegerConfig
    decoder := yaml.NewDecoder(jaegerConfigFile)
    if err := decoder.Decode(&cfg); err != nil {
        return nil, nil, fmt.Errorf("could not decode Jaeger config file: %w", err)
    }

    // Map JaegerConfig to jaegercfg.Configuration
    jaegerCfg := jaegercfg.Configuration{
        ServiceName: service,
        Sampler: &jaegercfg.SamplerConfig{
            Type:  cfg.Sampler.Type,
            Param: cfg.Sampler.Param,
        },
        Reporter: &jaegercfg.ReporterConfig{
            LogSpans:           cfg.Reporter.LogSpans,
            LocalAgentHostPort: cfg.Reporter.LocalAgentHostPort,
        },
    }

    // Initialize a logger
    jLogger := jaegerlog.StdLogger

    // Initialize a tracer
    tracer, closer, err := jaegerCfg.NewTracer(
        jaegercfg.Logger(jLogger),
    )
    if err != nil {
        return nil, nil, fmt.Errorf("could not initialize jaeger tracer: %w", err)
    }

    // Set the global tracer
    opentracing.SetGlobalTracer(tracer)

    return tracer, closer, nil
}

func parseURL(rawURL string) *url.URL {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		panic(fmt.Sprintf("Error parsing URL: %s", err))
	}
	return parsedURL
}

func initLogger() {
    log = logrus.New()
    
    // Get log level from environment variable (default to "info")
    logLevel := os.Getenv("LOG_LEVEL")
    if logLevel == "" {
        logLevel = "info"
    }
    
    // Parse log level
    level, err := logrus.ParseLevel(logLevel)
    if err != nil {
        level = logrus.InfoLevel
    }
    
    // Configure logger
    log.SetLevel(level)
    log.SetFormatter(&logrus.TextFormatter{
        FullTimestamp: true,
    })
}

type composePostClientWrapper struct {
    client    *social_network.ComposePostServiceClient
    transport thrift.TTransport
}

type ComposePostClientPool struct {
    clients chan *composePostClientWrapper
    addr    string
}

func NewComposePostClientPool(addr string, capacity int) *ComposePostClientPool {
    pool := &ComposePostClientPool{
        addr:    addr,
        clients: make(chan *composePostClientWrapper, capacity),
    }
    // Pre-allocate clients
    for i := 0; i < capacity; i++ {
        clientWrapper, err := pool.newClient()
        if err == nil {
            pool.clients <- clientWrapper
        }
    }
    return pool
}

func (p *ComposePostClientPool) newClient() (*composePostClientWrapper, error) {
    transport, err := thrift.NewTSocket(p.addr)
    if err != nil {
        return nil, fmt.Errorf("error opening socket: %v", err)
    }
    protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
    transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())

    useTransport, err := transportFactory.GetTransport(transport)
    if err != nil {
        return nil, fmt.Errorf("error creating transport: %v", err)
    }

    if err := useTransport.Open(); err != nil {
        return nil, fmt.Errorf("error opening transport: %v", err)
    }

    client := social_network.NewComposePostServiceClientFactory(useTransport, protocolFactory)
    return &composePostClientWrapper{client: client, transport: useTransport}, nil
}

func (p *ComposePostClientPool) getClient(ctx context.Context) (*composePostClientWrapper, error) {
    select {
    case client := <-p.clients:
        return client, nil
    default:
        return p.newClient()
    }
}

func (p *ComposePostClientPool) returnClient(clientWrapper *composePostClientWrapper) {
    select {
    case p.clients <- clientWrapper:
        // Client returned to pool
    default:
        // Pool is full, close the client
        clientWrapper.transport.Close()
    }
}

func (p *ComposePostClientPool) ComposePost(ctx context.Context, reqID int64, username string, userID int64, text string, mediaIDs []int64, mediaTypes []string, postType int32) error {
    span, ctx := opentracing.StartSpanFromContext(ctx, "ComposePost")
    defer span.Finish()

    // Add tags to the span
    span.SetTag("reqID", reqID)
    span.SetTag("userID", userID)
    span.SetTag("username", username)
    postTypeEnum := social_network.PostType(postType)
    
    // Get client from pool
    clientWrapper, err := p.getClient(ctx)
    if err != nil {
        return fmt.Errorf("failed to get client: %v", err)
    }
    defer p.returnClient(clientWrapper)

    // Inject the span context into the carrier
    carrier := make(map[string]string)
    err = opentracing.GlobalTracer().Inject(
        span.Context(),
        opentracing.TextMap,
        opentracing.TextMapCarrier(carrier))
    if err != nil {
        log.Printf("Failed to inject span context: %v", err)
    }

    err = clientWrapper.client.ComposePost(
        ctx,
        reqID,
        username,
        userID,
        text,
        mediaIDs,
        mediaTypes,
        postTypeEnum, // Use the converted type
        carrier,
    )

    if err != nil {
        return fmt.Errorf("failed to compose post: %v", err)
    }

    return nil
}

type userTimelineClientWrapper struct {
    client    *social_network.UserTimelineServiceClient
    transport thrift.TTransport
}

type UserTimelineClientPool struct {
    clients chan *userTimelineClientWrapper
    addr    string
}

func NewUserTimelineClientPool(addr string, capacity int) *UserTimelineClientPool {
    pool := &UserTimelineClientPool{
        addr:    addr,
        clients: make(chan *userTimelineClientWrapper, capacity),
    }
    // Pre-allocate clients
    for i := 0; i < capacity; i++ {
        clientWrapper, err := pool.newClient()
        if err == nil {
            pool.clients <- clientWrapper
        }
    }
    return pool
}

func (p *UserTimelineClientPool) newClient() (*userTimelineClientWrapper, error) {
    transport, err := thrift.NewTSocket(p.addr)
    if err != nil {
        return nil, fmt.Errorf("error opening socket: %v", err)
    }
    protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
    transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())

    useTransport, err := transportFactory.GetTransport(transport)
    if err != nil {
        return nil, fmt.Errorf("error creating transport: %v", err)
    }

    if err := useTransport.Open(); err != nil {
        return nil, fmt.Errorf("error opening transport: %v", err)
    }

    client := social_network.NewUserTimelineServiceClientFactory(useTransport, protocolFactory)
    return &userTimelineClientWrapper{client: client, transport: useTransport}, nil
}

func (p *UserTimelineClientPool) getClient(ctx context.Context) (*userTimelineClientWrapper, error) {
    select {
    case client := <-p.clients:
        return client, nil
    default:
        return p.newClient()
    }
}

func (p *UserTimelineClientPool) returnClient(clientWrapper *userTimelineClientWrapper) {
    select {
    case p.clients <- clientWrapper:
        // Client returned to pool
    default:
        // Pool is full, close the client
        clientWrapper.transport.Close()
    }
}

func (p *UserTimelineClientPool) ReadUserTimeline(ctx context.Context, reqID int64, userID int64, start int32, stop int32) ([]*social_network.Post, error) {
    span, ctx := opentracing.StartSpanFromContext(ctx, "ReadUserTimeline")
    defer span.Finish()

    span.SetTag("reqID", reqID)
    span.SetTag("userID", userID)
    span.SetTag("start", start)
    span.SetTag("stop", stop)

    clientWrapper, err := p.getClient(ctx)
    if err != nil {
        return nil, fmt.Errorf("failed to get client: %v", err)
    }
    defer p.returnClient(clientWrapper)

    carrier := make(map[string]string)
    err = opentracing.GlobalTracer().Inject(
        span.Context(),
        opentracing.TextMap,
        opentracing.TextMapCarrier(carrier))
    if err != nil {
        log.Printf("Failed to inject span context: %v", err)
    }

    posts, err := clientWrapper.client.ReadUserTimeline(ctx, reqID, userID, start, stop, carrier)
    if err != nil {
        return nil, fmt.Errorf("failed to read user timeline: %v", err)
    }

    return posts, nil
}

type userServiceClientWrapper struct {
    client    *social_network.UserServiceClient
    transport thrift.TTransport
}

type UserServiceClientPool struct {
    clients chan *userServiceClientWrapper
    addr    string
}

func NewUserServiceClientPool(addr string, capacity int) *UserServiceClientPool {
    pool := &UserServiceClientPool{
        addr:    addr,
        clients: make(chan *userServiceClientWrapper, capacity),
    }
    // Pre-allocate clients
    for i := 0; i < capacity; i++ {
        clientWrapper, err := pool.newClient()
        if err == nil {
            pool.clients <- clientWrapper
        }
    }
    return pool
}

func (p *UserServiceClientPool) newClient() (*userServiceClientWrapper, error) {
    transport, err := thrift.NewTSocket(p.addr)
    if err != nil {
        return nil, fmt.Errorf("error opening socket: %v", err)
    }
    protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
    transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())

    useTransport, err := transportFactory.GetTransport(transport)
    if err != nil {
        return nil, fmt.Errorf("error creating transport: %v", err)
    }

    if err := useTransport.Open(); err != nil {
        return nil, fmt.Errorf("error opening transport: %v", err)
    }

    client := social_network.NewUserServiceClientFactory(useTransport, protocolFactory)
    return &userServiceClientWrapper{client: client, transport: useTransport}, nil
}

func (p *UserServiceClientPool) getClient(ctx context.Context) (*userServiceClientWrapper, error) {
    select {
    case client := <-p.clients:
        return client, nil
    default:
        return p.newClient()
    }
}

func (p *UserServiceClientPool) returnClient(clientWrapper *userServiceClientWrapper) {
    select {
    case p.clients <- clientWrapper:
        // Client returned to pool
    default:
        // Pool is full, close the client
        clientWrapper.transport.Close()
    }
}

func (p *UserServiceClientPool) RegisterUser(ctx context.Context, reqID int64, firstName string, lastName string, username string, password string, userID int64) error {
    span, ctx := opentracing.StartSpanFromContext(ctx, "RegisterUser")
    defer span.Finish()

    span.SetTag("reqID", reqID)
    span.SetTag("username", username)
    span.SetTag("userID", userID)

    clientWrapper, err := p.getClient(ctx)
    if err != nil {
        return fmt.Errorf("failed to get client: %v", err)
    }
    defer p.returnClient(clientWrapper)

    carrier := make(map[string]string)
    err = opentracing.GlobalTracer().Inject(
        span.Context(),
        opentracing.TextMap,
        opentracing.TextMapCarrier(carrier))
    if err != nil {
        log.Printf("Failed to inject span context: %v", err)
    }

    err = clientWrapper.client.RegisterUserWithId(
        ctx,
        reqID,
        firstName,
        lastName,
        username,
        password,
        userID,
        carrier,
    )
    if err != nil {
        return fmt.Errorf("failed to register user: %v", err)
    }

    return nil
}

type socialGraphClientWrapper struct {
    client    *social_network.SocialGraphServiceClient
    transport thrift.TTransport
}

type SocialGraphClientPool struct {
    clients chan *socialGraphClientWrapper
    addr    string
}

func NewSocialGraphClientPool(addr string, capacity int) *SocialGraphClientPool {
    pool := &SocialGraphClientPool{
        addr:    addr,
        clients: make(chan *socialGraphClientWrapper, capacity),
    }
    // Pre-allocate clients
    for i := 0; i < capacity; i++ {
        clientWrapper, err := pool.newClient()
        if err == nil {
            pool.clients <- clientWrapper
        }
    }
    return pool
}

func (p *SocialGraphClientPool) newClient() (*socialGraphClientWrapper, error) {
    transport, err := thrift.NewTSocket(p.addr)
    if err != nil {
        return nil, fmt.Errorf("error opening socket: %v", err)
    }
    protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
    transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())

    useTransport, err := transportFactory.GetTransport(transport)
    if err != nil {
        return nil, fmt.Errorf("error creating transport: %v", err)
    }

    if err := useTransport.Open(); err != nil {
        return nil, fmt.Errorf("error opening transport: %v", err)
    }

    client := social_network.NewSocialGraphServiceClientFactory(useTransport, protocolFactory)
    return &socialGraphClientWrapper{client: client, transport: useTransport}, nil
}

func (p *SocialGraphClientPool) getClient(ctx context.Context) (*socialGraphClientWrapper, error) {
    select {
    case client := <-p.clients:
        return client, nil
    default:
        return p.newClient()
    }
}

func (p *SocialGraphClientPool) returnClient(clientWrapper *socialGraphClientWrapper) {
    select {
    case p.clients <- clientWrapper:
        // Client returned to pool
    default:
        // Pool is full, close the client
        clientWrapper.transport.Close()
    }
}

func (p *SocialGraphClientPool) Follow(ctx context.Context, reqID int64, userID int64, followeeID int64) error {
    span, ctx := opentracing.StartSpanFromContext(ctx, "Follow")
    defer span.Finish()

    span.SetTag("reqID", reqID)
    span.SetTag("userID", userID)
    span.SetTag("followeeID", followeeID)

    clientWrapper, err := p.getClient(ctx)
    if err != nil {
        return fmt.Errorf("failed to get client: %v", err)
    }
    defer p.returnClient(clientWrapper)

    carrier := make(map[string]string)
    err = opentracing.GlobalTracer().Inject(
        span.Context(),
        opentracing.TextMap,
        opentracing.TextMapCarrier(carrier))
    if err != nil {
        log.Printf("Failed to inject span context: %v", err)
    }

    err = clientWrapper.client.Follow(ctx, reqID, userID, followeeID, carrier)
    if err != nil {
        return fmt.Errorf("failed to follow user: %v", err)
    }

    return nil
}

func (p *SocialGraphClientPool) FollowWithUsername(ctx context.Context, reqID int64, username string, followeeName string) error {
    span, ctx := opentracing.StartSpanFromContext(ctx, "FollowWithUsername")
    defer span.Finish()

    span.SetTag("reqID", reqID)
    span.SetTag("username", username)
    span.SetTag("followeeName", followeeName)

    clientWrapper, err := p.getClient(ctx)
    if err != nil {
        return fmt.Errorf("failed to get client: %v", err)
    }
    defer p.returnClient(clientWrapper)

    carrier := make(map[string]string)
    err = opentracing.GlobalTracer().Inject(
        span.Context(),
        opentracing.TextMap,
        opentracing.TextMapCarrier(carrier))
    if err != nil {
        log.Printf("Failed to inject span context: %v", err)
    }

    err = clientWrapper.client.FollowWithUsername(ctx, reqID, username, followeeName, carrier)
    if err != nil {
        return fmt.Errorf("failed to follow user by username: %v", err)
    }

    return nil
}

func (p *SocialGraphClientPool) Unfollow(ctx context.Context, reqID int64, userID int64, followeeID int64) error {
    span, ctx := opentracing.StartSpanFromContext(ctx, "Unfollow")
    defer span.Finish()

    span.SetTag("reqID", reqID)
    span.SetTag("userID", userID)
    span.SetTag("followeeID", followeeID)

    clientWrapper, err := p.getClient(ctx)
    if err != nil {
        return fmt.Errorf("failed to get client: %v", err)
    }
    defer p.returnClient(clientWrapper)

    carrier := make(map[string]string)
    err = opentracing.GlobalTracer().Inject(
        span.Context(),
        opentracing.TextMap,
        opentracing.TextMapCarrier(carrier))
    if err != nil {
        log.Printf("Failed to inject span context: %v", err)
    }

    err = clientWrapper.client.Unfollow(ctx, reqID, userID, followeeID, carrier)
    if err != nil {
        return fmt.Errorf("failed to unfollow user: %v", err)
    }

    return nil
}

func (p *SocialGraphClientPool) UnfollowWithUsername(ctx context.Context, reqID int64, username string, followeeName string) error {
    span, ctx := opentracing.StartSpanFromContext(ctx, "UnfollowWithUsername")
    defer span.Finish()

    span.SetTag("reqID", reqID)
    span.SetTag("username", username)
    span.SetTag("followeeName", followeeName)

    clientWrapper, err := p.getClient(ctx)
    if err != nil {
        return fmt.Errorf("failed to get client: %v", err)
    }
    defer p.returnClient(clientWrapper)

    carrier := make(map[string]string)
    err = opentracing.GlobalTracer().Inject(
        span.Context(),
        opentracing.TextMap,
        opentracing.TextMapCarrier(carrier))
    if err != nil {
        log.Printf("Failed to inject span context: %v", err)
    }

    err = clientWrapper.client.UnfollowWithUsername(ctx, reqID, username, followeeName, carrier)
    if err != nil {
        return fmt.Errorf("failed to unfollow user by username: %v", err)
    }

    return nil
}

func main() {
    // Initialize logger before anything else
    initLogger()
    log.Info("Starting frontend service")

    // Initialize Jaeger tracer
    _, closer, err := initJaeger("frontend-service")
    if err != nil {
        log.WithError(err).Fatal("Could not initialize Jaeger tracer")
    }
    defer closer.Close()

    file, err := os.Open("service-config.json")
    if err != nil {
        log.WithError(err).Fatal("Error opening service-config.json")
        return
    }
    defer file.Close()

    // Decode the JSON into a map
    var configMap map[string]json.RawMessage
    err = json.NewDecoder(file).Decode(&configMap)
    if err != nil {
        log.WithError(err).Fatal("Error decoding JSON")
        return
    }

    // Extract service configurations
    err = json.Unmarshal(configMap["home-timeline-service"], &homeTimelineServiceConfig)
    if err != nil {
        log.WithError(err).Fatal("Error unmarshaling home-timeline-service config")
        return
    }

    err = json.Unmarshal(configMap["post-storage-service"], &postStorageServiceConfig)
    if err != nil {
        log.WithError(err).Fatal("Error unmarshaling post-storage-service config")
        return
    }

    err = json.Unmarshal(configMap["compose-post-service"], &composePostServiceConfig)
    if err != nil {
        log.WithError(err).Fatal("Error unmarshaling compose-post-service config")
        return
    }

    err = json.Unmarshal(configMap["user-timeline-service"], &userTimelineServiceConfig)
    if err != nil {
        log.WithError(err).Fatal("Error unmarshaling user-timeline-service config")
        return
    }

    err = json.Unmarshal(configMap["user-service"], &userServiceConfig)
    if err != nil {
        log.WithError(err).Fatal("Error unmarshaling user-service config")
        return
    }

    err = json.Unmarshal(configMap["social-graph-service"], &socialGraphServiceConfig)
    if err != nil {
        log.WithError(err).Fatal("Error unmarshaling social-graph-service config")
        return
    }

    log.WithFields(logrus.Fields{
        "config": homeTimelineServiceConfig,
    }).Info("Home Timeline Service Configuration")
    log.WithFields(logrus.Fields{
        "config": postStorageServiceConfig,
    }).Info("Post Storage Service Configuration")
    log.WithFields(logrus.Fields{
        "config": composePostServiceConfig,
    }).Info("Compose Post Service Configuration")
    log.WithFields(logrus.Fields{
        "config": userTimelineServiceConfig,
    }).Info("User Timeline Service Configuration")
    log.WithFields(logrus.Fields{
        "config": userServiceConfig,
    }).Info("User Service Configuration")
    log.WithFields(logrus.Fields{
        "config": socialGraphServiceConfig,
    }).Info("Social Graph Service Configuration")

    // Initialize Thrift client pools
    poolSize := 1024
    if poolSizeStr := os.Getenv("THRIFT_POOL_SIZE"); poolSizeStr != "" {
        if size, err := strconv.Atoi(poolSizeStr); err == nil {
            poolSize = size
        }
    }

    // Create client pools
    timelinePool := NewThriftClientPool(
        homeTimelineServiceConfig.Addr+":"+strconv.Itoa(homeTimelineServiceConfig.Port),
        poolSize,
    )
    composePool := NewComposePostClientPool(
        composePostServiceConfig.Addr+":"+strconv.Itoa(composePostServiceConfig.Port),
        poolSize,
    )
    userTimelinePool := NewUserTimelineClientPool(
        userTimelineServiceConfig.Addr+":"+strconv.Itoa(userTimelineServiceConfig.Port),
        poolSize,
    )
    userPool := NewUserServiceClientPool(
        userServiceConfig.Addr+":"+strconv.Itoa(userServiceConfig.Port),
        poolSize,
    )
    socialGraphPool := NewSocialGraphClientPool(
        socialGraphServiceConfig.Addr+":"+strconv.Itoa(socialGraphServiceConfig.Port),
        poolSize,
    )

    // HTTP handler for /wrk2-api/home-timeline/read endpoint
    http.HandleFunc("/wrk2-api/home-timeline/read", func(w http.ResponseWriter, r *http.Request) {
        startTime := time.Now()

        spanCtx, _ := opentracing.GlobalTracer().Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(r.Header))
        span := opentracing.GlobalTracer().StartSpan("HTTP /wrk2-api/home-timeline/read", ext.RPCServerOption(spanCtx))
        defer span.Finish()
        ctx := opentracing.ContextWithSpan(r.Context(), span)

        // Extract parameters from request
        userIDStr := r.URL.Query().Get("user_id")
        startStr := r.URL.Query().Get("start")
        stopStr := r.URL.Query().Get("stop")
        reqIDStr := r.URL.Query().Get("req_id")

        if userIDStr == "" || startStr == "" || stopStr == "" {
            http.Error(w, "Incomplete arguments", http.StatusBadRequest)
            return
        }

        userID, err := strconv.ParseInt(userIDStr, 10, 64)
        if err != nil {
            http.Error(w, "Invalid user_id", http.StatusBadRequest)
            return
        }

        start, err := strconv.ParseInt(startStr, 10, 32)
        if err != nil {
            http.Error(w, "Invalid start", http.StatusBadRequest)
            return
        }

        stop, err := strconv.ParseInt(stopStr, 10, 32)
        if err != nil {
            http.Error(w, "Invalid stop", http.StatusBadRequest)
            return
        }

        reqID, err := strconv.ParseInt(reqIDStr, 10, 64)
        if err != nil {
            reqID := time.Now().UnixNano()
            // http.Error(w, "Invalid req_id", http.StatusBadRequest)
            // return
        }

        posts, err := timelinePool.ReadHomeTimeline(ctx, reqID, userID, int32(start), int32(stop))
        if err != nil {
            log.WithFields(logrus.Fields{
                "error":   err,
                "req_id":  reqID,
                "user_id": userID,
            }).Error("Error reading home timeline")
            http.Error(w, fmt.Sprintf("Error reading home timeline: %v", err), http.StatusInternalServerError)
            return
        }

        if err := json.NewEncoder(w).Encode(posts); err != nil {
            log.WithFields(logrus.Fields{
                "error":   err,
                "req_id":  reqID,
                "user_id": userID,
            }).Error("Error encoding response")
            http.Error(w, fmt.Sprintf("Error encoding response: %v", err), http.StatusInternalServerError)
        }

        duration := time.Since(startTime).Microseconds()
        log.WithFields(logrus.Fields{
            "duration_us": duration,
            "req_id":     reqID,
            "user_id":    userID,
            "start":      start,
            "stop":       stop,
        }).Trace("Request completed")
    })

    // HTTP handler for /wrk2-api/post/compose endpoint
    http.HandleFunc("/wrk2-api/post/compose", func(w http.ResponseWriter, r *http.Request) {
        startTime := time.Now()

        spanCtx, _ := opentracing.GlobalTracer().Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(r.Header))
        span := opentracing.GlobalTracer().StartSpan("HTTP /wrk2-api/post/compose", ext.RPCServerOption(spanCtx))
        defer span.Finish()
        ctx := opentracing.ContextWithSpan(r.Context(), span)

        if err := r.ParseForm(); err != nil {
            http.Error(w, "Error parsing form data", http.StatusBadRequest)
            return
        }

        // Parse request parameters
        userID, err := strconv.ParseInt(r.FormValue("user_id"), 10, 64)
        if err != nil {
            http.Error(w, "Invalid user_id", http.StatusBadRequest)
            return
        }

        username := r.FormValue("username")
        postType, err := strconv.ParseInt(r.FormValue("post_type"), 10, 32)
        if err != nil {
            http.Error(w, "Invalid post_type", http.StatusBadRequest)
            return
        }

        text := r.FormValue("text")
        reqID := time.Now().UnixNano()

        var mediaIDs []int64
        var mediaTypes []string
        var mediaIDStrings []string

        if mediaIDsStr := r.FormValue("media_ids"); mediaIDsStr != "" {
            if err := json.Unmarshal([]byte(mediaIDsStr), &mediaIDStrings); err != nil {
                http.Error(w, fmt.Sprintf("Invalid media_ids format: %v", err), http.StatusBadRequest)
                return
            }
        }

        // Convert each string to int64
        mediaIDs = make([]int64, len(mediaIDStrings))
        for i, idStr := range mediaIDStrings {
            id, err := strconv.ParseInt(idStr, 10, 64)
            if err != nil {
                http.Error(w, fmt.Sprintf("Invalid media_id value: %v", err), http.StatusBadRequest)
                return
            }
            mediaIDs[i] = id
        }
        
        if mediaTypesStr := r.FormValue("media_types"); mediaTypesStr != "" {
            if err := json.Unmarshal([]byte(mediaTypesStr), &mediaTypes); err != nil {
                http.Error(w, "Invalid media_types format", http.StatusBadRequest)
                return
            }
        }

        err = composePool.ComposePost(
            ctx,
            reqID,
            username,
            userID,
            text,
            mediaIDs,
            mediaTypes,
            int32(postType),
        )

        if err != nil {
            log.WithFields(logrus.Fields{
                "error":    err,
                "req_id":   reqID,
                "user_id":  userID,
                "username": username,
            }).Error("Failed to compose post")
            http.Error(w, "Failed to compose post", http.StatusInternalServerError)
            return
        }

        w.WriteHeader(http.StatusOK)
        w.Write([]byte("Successfully composed post"))

        duration := time.Since(startTime).Microseconds()
        log.WithFields(logrus.Fields{
            "duration_us": duration,
            "req_id":     reqID,
            "user_id":    userID,
            "username":   username,
        }).Trace("Compose request completed")
    })

    // HTTP handler for /wrk2-api/user-timeline/read endpoint
    http.HandleFunc("/wrk2-api/user-timeline/read", func(w http.ResponseWriter, r *http.Request) {
        startTime := time.Now()

        spanCtx, _ := opentracing.GlobalTracer().Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(r.Header))
        span := opentracing.GlobalTracer().StartSpan("HTTP /wrk2-api/user-timeline/read", ext.RPCServerOption(spanCtx))
        defer span.Finish()
        ctx := opentracing.ContextWithSpan(r.Context(), span)

        // Extract parameters from request
        userIDStr := r.URL.Query().Get("user_id")
        startStr := r.URL.Query().Get("start")
        stopStr := r.URL.Query().Get("stop")

        if userIDStr == "" || startStr == "" || stopStr == "" {
            http.Error(w, "Incomplete arguments", http.StatusBadRequest)
            return
        }

        userID, err := strconv.ParseInt(userIDStr, 10, 64)
        if err != nil {
            http.Error(w, "Invalid user_id", http.StatusBadRequest)
            return
        }

        start, err := strconv.ParseInt(startStr, 10, 32)
        if err != nil {
            http.Error(w, "Invalid start", http.StatusBadRequest)
            return
        }

        stop, err := strconv.ParseInt(stopStr, 10, 32)
        if err != nil {
            http.Error(w, "Invalid stop", http.StatusBadRequest)
            return
        }

        reqID, err := strconv.ParseInt(reqIDStr, 10, 64)
        if err != nil {
            reqID := time.Now().UnixNano()
            // http.Error(w, "Invalid req_id", http.StatusBadRequest)
            // return
        }
        

        posts, err := userTimelinePool.ReadUserTimeline(ctx, reqID, userID, int32(start), int32(stop))
        if err != nil {
            log.WithFields(logrus.Fields{
                "error":   err,
                "req_id":  reqID,
                "user_id": userID,
            }).Error("Error reading user timeline")
            http.Error(w, fmt.Sprintf("Error reading user timeline: %v", err), http.StatusInternalServerError)
            return
        }

        w.Header().Set("Content-Type", "application/json")
        if err := json.NewEncoder(w).Encode(posts); err != nil {
            log.WithFields(logrus.Fields{
                "error":   err,
                "req_id":  reqID,
                "user_id": userID,
            }).Error("Error encoding response")
            http.Error(w, fmt.Sprintf("Error encoding response: %v", err), http.StatusInternalServerError)
            return
        }

        duration := time.Since(startTime).Microseconds()
        log.WithFields(logrus.Fields{
            "duration_us": duration,
            "req_id":     reqID,
            "user_id":    userID,
            "start":      start,
            "stop":       stop,
        }).Trace("User timeline request completed")
    })

    // HTTP handler for /wrk2-api/user/register endpoint
    http.HandleFunc("/wrk2-api/user/register", func(w http.ResponseWriter, r *http.Request) {
        startTime := time.Now()

        if r.Method != "POST" {
            http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
            return
        }

        spanCtx, _ := opentracing.GlobalTracer().Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(r.Header))
        span := opentracing.GlobalTracer().StartSpan("HTTP /wrk2-api/user/register", ext.RPCServerOption(spanCtx))
        defer span.Finish()
        ctx := opentracing.ContextWithSpan(r.Context(), span)

        if err := r.ParseForm(); err != nil {
            http.Error(w, "Error parsing form data", http.StatusBadRequest)
            return
        }

        // Extract form values
        firstName := r.FormValue("first_name")
        lastName := r.FormValue("last_name")
        username := r.FormValue("username")
        password := r.FormValue("password")
        userIDStr := r.FormValue("user_id")

        // Validate required fields
        if firstName == "" || lastName == "" || username == "" || 
           password == "" || userIDStr == "" {
            http.Error(w, "Incomplete arguments", http.StatusBadRequest)
            return
        }

        userID, err := strconv.ParseInt(userIDStr, 10, 64)
        if err != nil {
            http.Error(w, "Invalid user_id", http.StatusBadRequest)
            return
        }

        reqID := time.Now().UnixNano()

        err = userPool.RegisterUser(
            ctx,
            reqID,
            firstName,
            lastName,
            username,
            password,
            userID,
        )

        if err != nil {
            log.WithFields(logrus.Fields{
                "error":     err,
                "req_id":    reqID,
                "username":  username,
                "user_id":   userID,
            }).Error("Failed to register user")
            http.Error(w, fmt.Sprintf("Failed to register user: %v", err), http.StatusInternalServerError)
            return
        }

        w.WriteHeader(http.StatusOK)
        w.Write([]byte("Success!"))

        duration := time.Since(startTime).Microseconds()
        log.WithFields(logrus.Fields{
            "duration_us": duration,
            "req_id":     reqID,
            "username":   username,
            "user_id":    userID,
        }).Trace("User registration completed")
    })

    // Add handler for follow endpoint
    http.HandleFunc("/wrk2-api/user/follow", func(w http.ResponseWriter, r *http.Request) {
        startTime := time.Now()

        if r.Method != "POST" {
            http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
            return
        }

        spanCtx, _ := opentracing.GlobalTracer().Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(r.Header))
        span := opentracing.GlobalTracer().StartSpan("HTTP /wrk2-api/user/follow", ext.RPCServerOption(spanCtx))
        defer span.Finish()
        ctx := opentracing.ContextWithSpan(r.Context(), span)

        if err := r.ParseForm(); err != nil {
            http.Error(w, "Error parsing form data", http.StatusBadRequest)
            return
        }

        reqID := time.Now().UnixNano()
        var err error

        // Check if using IDs or usernames
        if userIDStr := r.FormValue("user_id"); userIDStr != "" {
            followeeIDStr := r.FormValue("followee_id")
            if followeeIDStr == "" {
                http.Error(w, "Incomplete arguments", http.StatusBadRequest)
                return
            }

            userID, err := strconv.ParseInt(userIDStr, 10, 64)
            if err != nil {
                http.Error(w, "Invalid user_id", http.StatusBadRequest)
                return
            }

            followeeID, err := strconv.ParseInt(followeeIDStr, 10, 64)
            if err != nil {
                http.Error(w, "Invalid followee_id", http.StatusBadRequest)
                return
            }

            err = socialGraphPool.Follow(ctx, reqID, userID, followeeID)
        } else if userName := r.FormValue("user_name"); userName != "" {
            followeeName := r.FormValue("followee_name")
            if followeeName == "" {
                http.Error(w, "Incomplete arguments", http.StatusBadRequest)
                return
            }

            err = socialGraphPool.FollowWithUsername(ctx, reqID, userName, followeeName)
        } else {
            http.Error(w, "Incomplete arguments", http.StatusBadRequest)
            return
        }

        if err != nil {
            log.WithFields(logrus.Fields{
                "error":  err,
                "req_id": reqID,
            }).Error("Failed to follow user")
            http.Error(w, fmt.Sprintf("Follow failed: %v", err), http.StatusInternalServerError)
            return
        }

        w.WriteHeader(http.StatusOK)
        w.Write([]byte("Success!"))

        duration := time.Since(startTime).Microseconds()
        log.WithFields(logrus.Fields{
            "duration_us": duration,
            "req_id":     reqID,
        }).Trace("Follow request completed")
    })

    // Add handler for unfollow endpoint
    http.HandleFunc("/wrk2-api/user/unfollow", func(w http.ResponseWriter, r *http.Request) {
        startTime := time.Now()

        if r.Method != "POST" {
            http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
            return
        }

        spanCtx, _ := opentracing.GlobalTracer().Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(r.Header))
        span := opentracing.GlobalTracer().StartSpan("HTTP /wrk2-api/user/unfollow", ext.RPCServerOption(spanCtx))
        defer span.Finish()
        ctx := opentracing.ContextWithSpan(r.Context(), span)

        if err := r.ParseForm(); err != nil {
            http.Error(w, "Error parsing form data", http.StatusBadRequest)
            return
        }

        reqID := time.Now().UnixNano()
        var err error

        // Check if using IDs or usernames
        if userIDStr := r.FormValue("user_id"); userIDStr != "" {
            followeeIDStr := r.FormValue("followee_id")
            if followeeIDStr == "" {
                http.Error(w, "Incomplete arguments", http.StatusBadRequest)
                return
            }

            userID, err := strconv.ParseInt(userIDStr, 10, 64)
            if err != nil {
                http.Error(w, "Invalid user_id", http.StatusBadRequest)
                return
            }

            followeeID, err := strconv.ParseInt(followeeIDStr, 10, 64)
            if err != nil {
                http.Error(w, "Invalid followee_id", http.StatusBadRequest)
                return
            }

            err = socialGraphPool.Unfollow(ctx, reqID, userID, followeeID)
        } else if userName := r.FormValue("user_name"); userName != "" {
            followeeName := r.FormValue("followee_name")
            if followeeName == "" {
                http.Error(w, "Incomplete arguments", http.StatusBadRequest)
                return
            }

            err = socialGraphPool.UnfollowWithUsername(ctx, reqID, userName, followeeName)
        } else {
            http.Error(w, "Incomplete arguments", http.StatusBadRequest)
            return
        }

        if err != nil {
            log.WithFields(logrus.Fields{
                "error":  err,
                "req_id": reqID,
            }).Error("Failed to unfollow user")
            http.Error(w, fmt.Sprintf("Unfollow failed: %v", err), http.StatusInternalServerError)
            return
        }

        w.WriteHeader(http.StatusOK)
        w.Write([]byte("Success!"))

        duration := time.Since(startTime).Microseconds()
        log.WithFields(logrus.Fields{
            "duration_us": duration,
            "req_id":     reqID,
        }).Trace("Unfollow request completed")
    })

    // Start HTTP server
    port := os.Getenv("FRONTEND_PORT")
    if port == "" {
        port = "8081" // Default port if not set
    }
    log.Info("Starting server on :" + port)
    if err := http.ListenAndServe(":"+port, nil); err != nil {
        log.WithError(err).Fatal("Failed to start server")
    }
}
