// Autogenerated by Thrift Compiler (0.12.0)
// DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING

package main

import (
        "context"
        "flag"
        "fmt"
        "math"
        "net"
        "net/url"
        "os"
        "strconv"
        "strings"
        "github.com/apache/thrift/lib/go/thrift"
        "social_network"
)


func Usage() {
  fmt.Fprintln(os.Stderr, "Usage of ", os.Args[0], " [-h host:port] [-u url] [-f[ramed]] function [arg1 [arg2...]]:")
  flag.PrintDefaults()
  fmt.Fprintln(os.Stderr, "\nFunctions:")
  fmt.Fprintln(os.Stderr, "  void RegisterUser(i64 req_id, string first_name, string last_name, string username, string password,  carrier)")
  fmt.Fprintln(os.Stderr, "  void RegisterUserWithId(i64 req_id, string first_name, string last_name, string username, string password, i64 user_id,  carrier)")
  fmt.Fprintln(os.Stderr, "  string Login(i64 req_id, string username, string password,  carrier)")
  fmt.Fprintln(os.Stderr, "  Creator ComposeCreatorWithUserId(i64 req_id, i64 user_id, string username,  carrier)")
  fmt.Fprintln(os.Stderr, "  Creator ComposeCreatorWithUsername(i64 req_id, string username,  carrier)")
  fmt.Fprintln(os.Stderr, "  i64 GetUserId(i64 req_id, string username,  carrier)")
  fmt.Fprintln(os.Stderr)
  os.Exit(0)
}

type httpHeaders map[string]string

func (h httpHeaders) String() string {
  var m map[string]string = h
  return fmt.Sprintf("%s", m)
}

func (h httpHeaders) Set(value string) error {
  parts := strings.Split(value, ": ")
  if len(parts) != 2 {
    return fmt.Errorf("header should be of format 'Key: Value'")
  }
  h[parts[0]] = parts[1]
  return nil
}

func main() {
  flag.Usage = Usage
  var host string
  var port int
  var protocol string
  var urlString string
  var framed bool
  var useHttp bool
  headers := make(httpHeaders)
  var parsedUrl *url.URL
  var trans thrift.TTransport
  _ = strconv.Atoi
  _ = math.Abs
  flag.Usage = Usage
  flag.StringVar(&host, "h", "localhost", "Specify host and port")
  flag.IntVar(&port, "p", 9090, "Specify port")
  flag.StringVar(&protocol, "P", "binary", "Specify the protocol (binary, compact, simplejson, json)")
  flag.StringVar(&urlString, "u", "", "Specify the url")
  flag.BoolVar(&framed, "framed", false, "Use framed transport")
  flag.BoolVar(&useHttp, "http", false, "Use http")
  flag.Var(headers, "H", "Headers to set on the http(s) request (e.g. -H \"Key: Value\")")
  flag.Parse()
  
  if len(urlString) > 0 {
    var err error
    parsedUrl, err = url.Parse(urlString)
    if err != nil {
      fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
      flag.Usage()
    }
    host = parsedUrl.Host
    useHttp = len(parsedUrl.Scheme) <= 0 || parsedUrl.Scheme == "http" || parsedUrl.Scheme == "https"
  } else if useHttp {
    _, err := url.Parse(fmt.Sprint("http://", host, ":", port))
    if err != nil {
      fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
      flag.Usage()
    }
  }
  
  cmd := flag.Arg(0)
  var err error
  if useHttp {
    trans, err = thrift.NewTHttpClient(parsedUrl.String())
    if len(headers) > 0 {
      httptrans := trans.(*thrift.THttpClient)
      for key, value := range headers {
        httptrans.SetHeader(key, value)
      }
    }
  } else {
    portStr := fmt.Sprint(port)
    if strings.Contains(host, ":") {
           host, portStr, err = net.SplitHostPort(host)
           if err != nil {
                   fmt.Fprintln(os.Stderr, "error with host:", err)
                   os.Exit(1)
           }
    }
    trans, err = thrift.NewTSocket(net.JoinHostPort(host, portStr))
    if err != nil {
      fmt.Fprintln(os.Stderr, "error resolving address:", err)
      os.Exit(1)
    }
    if framed {
      trans = thrift.NewTFramedTransport(trans)
    }
  }
  if err != nil {
    fmt.Fprintln(os.Stderr, "Error creating transport", err)
    os.Exit(1)
  }
  defer trans.Close()
  var protocolFactory thrift.TProtocolFactory
  switch protocol {
  case "compact":
    protocolFactory = thrift.NewTCompactProtocolFactory()
    break
  case "simplejson":
    protocolFactory = thrift.NewTSimpleJSONProtocolFactory()
    break
  case "json":
    protocolFactory = thrift.NewTJSONProtocolFactory()
    break
  case "binary", "":
    protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
    break
  default:
    fmt.Fprintln(os.Stderr, "Invalid protocol specified: ", protocol)
    Usage()
    os.Exit(1)
  }
  iprot := protocolFactory.GetProtocol(trans)
  oprot := protocolFactory.GetProtocol(trans)
  client := social_network.NewUserServiceClient(thrift.NewTStandardClient(iprot, oprot))
  if err := trans.Open(); err != nil {
    fmt.Fprintln(os.Stderr, "Error opening socket to ", host, ":", port, " ", err)
    os.Exit(1)
  }
  
  switch cmd {
  case "RegisterUser":
    if flag.NArg() - 1 != 6 {
      fmt.Fprintln(os.Stderr, "RegisterUser requires 6 args")
      flag.Usage()
    }
    argvalue0, err58 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err58 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    argvalue2 := flag.Arg(3)
    value2 := argvalue2
    argvalue3 := flag.Arg(4)
    value3 := argvalue3
    argvalue4 := flag.Arg(5)
    value4 := argvalue4
    arg63 := flag.Arg(6)
    mbTrans64 := thrift.NewTMemoryBufferLen(len(arg63))
    defer mbTrans64.Close()
    _, err65 := mbTrans64.WriteString(arg63)
    if err65 != nil { 
      Usage()
      return
    }
    factory66 := thrift.NewTJSONProtocolFactory()
    jsProt67 := factory66.GetProtocol(mbTrans64)
    containerStruct5 := social_network.NewUserServiceRegisterUserArgs()
    err68 := containerStruct5.ReadField6(jsProt67)
    if err68 != nil {
      Usage()
      return
    }
    argvalue5 := containerStruct5.Carrier
    value5 := argvalue5
    fmt.Print(client.RegisterUser(context.Background(), value0, value1, value2, value3, value4, value5))
    fmt.Print("\n")
    break
  case "RegisterUserWithId":
    if flag.NArg() - 1 != 7 {
      fmt.Fprintln(os.Stderr, "RegisterUserWithId requires 7 args")
      flag.Usage()
    }
    argvalue0, err69 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err69 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    argvalue2 := flag.Arg(3)
    value2 := argvalue2
    argvalue3 := flag.Arg(4)
    value3 := argvalue3
    argvalue4 := flag.Arg(5)
    value4 := argvalue4
    argvalue5, err74 := (strconv.ParseInt(flag.Arg(6), 10, 64))
    if err74 != nil {
      Usage()
      return
    }
    value5 := argvalue5
    arg75 := flag.Arg(7)
    mbTrans76 := thrift.NewTMemoryBufferLen(len(arg75))
    defer mbTrans76.Close()
    _, err77 := mbTrans76.WriteString(arg75)
    if err77 != nil { 
      Usage()
      return
    }
    factory78 := thrift.NewTJSONProtocolFactory()
    jsProt79 := factory78.GetProtocol(mbTrans76)
    containerStruct6 := social_network.NewUserServiceRegisterUserWithIdArgs()
    err80 := containerStruct6.ReadField7(jsProt79)
    if err80 != nil {
      Usage()
      return
    }
    argvalue6 := containerStruct6.Carrier
    value6 := argvalue6
    fmt.Print(client.RegisterUserWithId(context.Background(), value0, value1, value2, value3, value4, value5, value6))
    fmt.Print("\n")
    break
  case "Login":
    if flag.NArg() - 1 != 4 {
      fmt.Fprintln(os.Stderr, "Login requires 4 args")
      flag.Usage()
    }
    argvalue0, err81 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err81 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    argvalue2 := flag.Arg(3)
    value2 := argvalue2
    arg84 := flag.Arg(4)
    mbTrans85 := thrift.NewTMemoryBufferLen(len(arg84))
    defer mbTrans85.Close()
    _, err86 := mbTrans85.WriteString(arg84)
    if err86 != nil { 
      Usage()
      return
    }
    factory87 := thrift.NewTJSONProtocolFactory()
    jsProt88 := factory87.GetProtocol(mbTrans85)
    containerStruct3 := social_network.NewUserServiceLoginArgs()
    err89 := containerStruct3.ReadField4(jsProt88)
    if err89 != nil {
      Usage()
      return
    }
    argvalue3 := containerStruct3.Carrier
    value3 := argvalue3
    fmt.Print(client.Login(context.Background(), value0, value1, value2, value3))
    fmt.Print("\n")
    break
  case "ComposeCreatorWithUserId":
    if flag.NArg() - 1 != 4 {
      fmt.Fprintln(os.Stderr, "ComposeCreatorWithUserId requires 4 args")
      flag.Usage()
    }
    argvalue0, err90 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err90 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1, err91 := (strconv.ParseInt(flag.Arg(2), 10, 64))
    if err91 != nil {
      Usage()
      return
    }
    value1 := argvalue1
    argvalue2 := flag.Arg(3)
    value2 := argvalue2
    arg93 := flag.Arg(4)
    mbTrans94 := thrift.NewTMemoryBufferLen(len(arg93))
    defer mbTrans94.Close()
    _, err95 := mbTrans94.WriteString(arg93)
    if err95 != nil { 
      Usage()
      return
    }
    factory96 := thrift.NewTJSONProtocolFactory()
    jsProt97 := factory96.GetProtocol(mbTrans94)
    containerStruct3 := social_network.NewUserServiceComposeCreatorWithUserIdArgs()
    err98 := containerStruct3.ReadField4(jsProt97)
    if err98 != nil {
      Usage()
      return
    }
    argvalue3 := containerStruct3.Carrier
    value3 := argvalue3
    fmt.Print(client.ComposeCreatorWithUserId(context.Background(), value0, value1, value2, value3))
    fmt.Print("\n")
    break
  case "ComposeCreatorWithUsername":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "ComposeCreatorWithUsername requires 3 args")
      flag.Usage()
    }
    argvalue0, err99 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err99 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    arg101 := flag.Arg(3)
    mbTrans102 := thrift.NewTMemoryBufferLen(len(arg101))
    defer mbTrans102.Close()
    _, err103 := mbTrans102.WriteString(arg101)
    if err103 != nil { 
      Usage()
      return
    }
    factory104 := thrift.NewTJSONProtocolFactory()
    jsProt105 := factory104.GetProtocol(mbTrans102)
    containerStruct2 := social_network.NewUserServiceComposeCreatorWithUsernameArgs()
    err106 := containerStruct2.ReadField3(jsProt105)
    if err106 != nil {
      Usage()
      return
    }
    argvalue2 := containerStruct2.Carrier
    value2 := argvalue2
    fmt.Print(client.ComposeCreatorWithUsername(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "GetUserId":
    if flag.NArg() - 1 != 3 {
      fmt.Fprintln(os.Stderr, "GetUserId requires 3 args")
      flag.Usage()
    }
    argvalue0, err107 := (strconv.ParseInt(flag.Arg(1), 10, 64))
    if err107 != nil {
      Usage()
      return
    }
    value0 := argvalue0
    argvalue1 := flag.Arg(2)
    value1 := argvalue1
    arg109 := flag.Arg(3)
    mbTrans110 := thrift.NewTMemoryBufferLen(len(arg109))
    defer mbTrans110.Close()
    _, err111 := mbTrans110.WriteString(arg109)
    if err111 != nil { 
      Usage()
      return
    }
    factory112 := thrift.NewTJSONProtocolFactory()
    jsProt113 := factory112.GetProtocol(mbTrans110)
    containerStruct2 := social_network.NewUserServiceGetUserIdArgs()
    err114 := containerStruct2.ReadField3(jsProt113)
    if err114 != nil {
      Usage()
      return
    }
    argvalue2 := containerStruct2.Carrier
    value2 := argvalue2
    fmt.Print(client.GetUserId(context.Background(), value0, value1, value2))
    fmt.Print("\n")
    break
  case "":
    Usage()
    break
  default:
    fmt.Fprintln(os.Stderr, "Invalid function ", cmd)
  }
}