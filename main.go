package kvraft

//
//import (
//	"flag"
//	"github.com/gin-gonic/gin"
//	"os"
//	"strings"
//    "log"
//)
//
//const (
//	DEFAULT_LOG_DIR           = "./log/"
//	ACCESS_LOG_NAME           = "access.log"
//	ERROR_LOG_NAME            = "error.log"
//	SERVER_LOG_NAME           = "kvraft.log"
//    DEFAULT_SERVER_CONF_PATH  = "./conf/server.conf"
//)
//
////Global Logger
//var ServerLogger *log.Logger
////Global Server
//var localServer *KVRaftServer
//// Local Cache
//var ShCache *ShareCache
//
//func main() {
//	gin.SetMode(gin.ReleaseMode)
//	router := gin.New()
//
//	var cfgPath, joinAddr string
//	flag.StringVar(&cfgPath, "c", DEFAULT_SERVER_CONF_PATH, "conf file path")
//	flag.StringVar(&joinAddr, "join", "", "join addr host:port")
//	flag.Parse()
//
//	//============== Server Config ==============//
//	config, err := NewServerConfig(cfgPath)
//	if err != nil {
//		panic(err)
//		return
//	}
//
//	//============== Create all Logs ==============//
//	var logDir string
//	if config.LogDir == "" {
//		logDir = DEFAULT_LOG_DIR
//	} else {
//		logDir = config.LogDir
//	}
//
//	err = os.MkdirAll(logDir, 0755)
//	if err != nil {
//		panic(err)
//	}
//
//	if !strings.HasSuffix(logDir, "/") {
//		logDir = logDir + "/"
//	}
//	serverLogFile, err := os.OpenFile(logDir+SERVER_LOG_NAME, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
//	if err != nil {
//		panic("Failed to open log file " + logDir + SERVER_LOG_NAME + ":" + err.Error())
//		return
//	}
//	defer serverLogFile.Close()
//
//	// 建立 AccessLogger
//	accessLogFile, err := os.OpenFile(logDir+ACCESS_LOG_NAME, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
//	if err != nil {
//		panic("Failed to open log file " + logDir + ACCESS_LOG_NAME + ":" + err.Error())
//		return
//	}
//	defer accessLogFile.Close()
//
//	// 建立 ErrorLogger
//	errorLogFile, err := os.OpenFile(logDir+ERROR_LOG_NAME, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
//	if err != nil {
//		panic("Failed to open log file " + logDir + ERROR_LOG_NAME + ":" + err.Error())
//		return
//	}
//	defer errorLogFile.Close()
//
//	ServerLogger = InitServerLogger(serverLogFile, "[kvraft]")
//
//	//Use AccessLogger and ErrorLogger
//	router.Use(LoggerWithWriter(accessLogFile, "[kvraft]"), gin.RecoveryWithWriter(errorLogFile))
//
//    StartKVRaftServer(router, config, serverLogFile)
//
//}

func main() {

}
