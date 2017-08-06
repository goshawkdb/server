package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/go-kit/kit/log"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	"goshawkdb.io/common/certs"
	goshawk "goshawkdb.io/server"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/network"
	"goshawkdb.io/server/stats"
	"io/ioutil"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)

	logger.Log("product", common.ProductName, "version", goshawk.ServerVersion, "mdbVersion", mdb.Version(), "args", fmt.Sprint(os.Args))

	if s, err := newServer(logger); err != nil {
		fmt.Printf("\n%v\n\n", err)
		flag.Usage()
		fmt.Println("\nSee https://goshawkdb.io/starting.html for the Getting Started guide.")
		os.Exit(1)
	} else if s != nil {
		s.start()
	}
}

func newServer(logger log.Logger) (*server, error) {
	var configFile, dataDir, certFile string
	var port, wssPort, promPort int
	var httpProf, noWSS, noProm, version, genClusterCert, genClientCert bool

	flag.StringVar(&configFile, "config", "", "`Path` to configuration file (required to start server).")
	flag.StringVar(&dataDir, "dir", "", "`Path` to data directory (required to run server).")
	flag.StringVar(&certFile, "cert", "", "`Path` to cluster certificate and key file (required to run server).")
	flag.IntVar(&port, "port", common.DefaultPort, "Port to listen on (required if non-default).")
	flag.BoolVar(&version, "version", false, "Display version and exit.")
	flag.BoolVar(&genClusterCert, "gen-cluster-cert", false, "Generate new cluster certificate key pair.")
	flag.BoolVar(&genClientCert, "gen-client-cert", false, "Generate client certificate key pair.")
	flag.BoolVar(&noWSS, "noWSS", false, "Disable the WebSocket service.")
	flag.IntVar(&wssPort, "wssPort", common.DefaultWSSPort, "Port to provide WebSocket service on.")
	flag.BoolVar(&noProm, "noPrometheus", false, "Disable the HTTP Prometheus metrics service.")
	flag.IntVar(&promPort, "prometheusPort", common.DefaultPrometheusPort, "Port to provide HTTP for Prometheus metrics service on.")
	flag.BoolVar(&httpProf, "httpProfile", false, fmt.Sprintf("Enable Go HTTP Profiling on port localhost:%d.", goshawk.HttpProfilePort))
	flag.Parse()

	if version {
		fmt.Println(common.ProductName, "version", goshawk.ServerVersion)
		return nil, nil
	}

	if genClusterCert {
		certificatePrivateKeyPair, err := certs.NewClusterCertificate()
		if err != nil {
			return nil, err
		}
		fmt.Printf("%v%v", certificatePrivateKeyPair.CertificatePEM, certificatePrivateKeyPair.PrivateKeyPEM)
		return nil, nil
	}

	if len(certFile) == 0 {
		return nil, fmt.Errorf("No certificate supplied (missing -cert parameter). Use -gen-cluster-cert to create cluster certificate.")
	}
	certificate, err := ioutil.ReadFile(certFile)
	if err != nil {
		return nil, err
	}

	if genClientCert {
		certificatePrivateKeyPair, err := certs.NewClientCertificate(certificate)
		if err != nil {
			return nil, err
		}
		fmt.Printf("%v%v", certificatePrivateKeyPair.CertificatePEM, certificatePrivateKeyPair.PrivateKeyPEM)
		fingerprint := sha256.Sum256(certificatePrivateKeyPair.Certificate)
		logger.Log("fingerprint", hex.EncodeToString(fingerprint[:]))
		return nil, nil
	}

	if dataDir == "" {
		dataDir, err = ioutil.TempDir("", common.ProductName+"_Data_")
		if err != nil {
			return nil, err
		}
		logger.Log("msg", "No data dir supplied (missing -dir parameter).", "dataDir", dataDir)
	}
	err = os.MkdirAll(dataDir, 0750)
	if err != nil {
		return nil, err
	}

	if configFile != "" {
		_, err := ioutil.ReadFile(configFile)
		if err != nil {
			return nil, err
		}
	}

	if !(0 < port && port < 65536) {
		return nil, fmt.Errorf("Supplied port is illegal (%d). Port must be > 0 and < 65536", port)
	}

	if noWSS {
		wssPort = 0
	}
	if noProm {
		promPort = 0
	}

	if wssPort != 0 && !(0 < wssPort && wssPort < 65536 && wssPort != port) {
		return nil, fmt.Errorf("Supplied wss port is illegal (%d). WSS Port must be > 0 and < 65536 and not equal to the main communication port (%d)", wssPort, port)
	}

	if promPort != 0 && !(0 < promPort && promPort < 65536 && promPort != port) {
		return nil, fmt.Errorf("Supplied Prometheus port is illegal (%d). Prometheus Port must be > 0 and < 65536 and not equal to the main communication port (%d)", promPort, port)
	}

	s := &server{
		logger:       logger,
		configFile:   configFile,
		certificate:  certificate,
		dataDir:      dataDir,
		port:         uint16(port),
		onShutdown:   []func(){},
		shutdownChan: make(chan goshawk.EmptyStruct),
		wssPort:      uint16(wssPort),
		promPort:     uint16(promPort),
		httpProf:     httpProf,
	}

	if err = s.ensureRMId(); err != nil {
		return nil, err
	}
	if err = s.ensureBootCount(); err != nil {
		return nil, err
	}

	return s, nil
}

type server struct {
	logger            log.Logger
	configFile        string
	certificate       []byte
	dataDir           string
	port              uint16
	wssPort           uint16
	promPort          uint16
	httpProf          bool
	rmId              common.RMId
	bootCount         uint32
	connectionManager *network.ConnectionManager
	transmogrifier    *network.TopologyTransmogrifier
	profileFile       *os.File
	traceFile         *os.File
	onShutdown        []func()
	shutdownChan      chan goshawk.EmptyStruct
	shutdownCounter   int32
}

func (s *server) start() {
	if s.httpProf {
		go func() {
			s.logger.Log("pprofResult", http.ListenAndServe(fmt.Sprintf("localhost:%d", goshawk.HttpProfilePort), nil))
		}()
	}

	os.Stdin.Close()

	procs := runtime.NumCPU()
	if procs < 2 {
		procs = 2
	}
	runtime.GOMAXPROCS(procs)

	commandLineConfig, err := s.commandLineConfig()
	s.maybeShutdown(err)

	disk, err := mdbs.NewMDBServer(s.dataDir, 0, 0600, goshawk.MDBInitialSize, 500*time.Microsecond, db.DB, s.logger)
	s.maybeShutdown(err)
	db := disk.(*db.Databases)
	s.addOnShutdown(db.Shutdown)

	cm, transmogrifier, lc := network.NewConnectionManager(s.rmId, s.bootCount, uint8(procs), db, s.certificate, s.port, s, commandLineConfig, s.logger)
	s.certificate = nil
	s.addOnShutdown(transmogrifier.Shutdown)
	sp := stats.NewStatsPublisher(cm, lc, s.logger)
	s.addOnShutdown(sp.Shutdown)
	s.addOnShutdown(cm.ShutdownSync)
	s.connectionManager = cm
	s.transmogrifier = transmogrifier

	go s.signalHandler()

	listener, err := network.NewListener(s.port, cm, s.logger)
	s.maybeShutdown(err)
	s.addOnShutdown(listener.ShutdownSync)

	var wssMux, promMux *network.HttpListenerWithMux
	var wssWG, promWG *sync.WaitGroup
	if s.wssPort != 0 {
		wssWG := new(sync.WaitGroup)
		if s.wssPort == s.promPort {
			wssWG.Add(2)
		} else {
			wssWG.Add(1)
		}
		wssMux, err = network.NewHttpListenerWithMux(s.wssPort, cm, s.logger, wssWG)
		s.maybeShutdown(err)
		wssListener := network.NewWebsocketListener(wssMux, cm, s.logger)
		s.addOnShutdown(wssListener.ShutdownSync)
	}

	if s.promPort != 0 {
		if s.wssPort == s.promPort {
			promWG = wssWG
			promMux = wssMux
		} else {
			promWG = new(sync.WaitGroup)
			promWG.Add(1)
			promMux, err = network.NewHttpListenerWithMux(s.promPort, cm, s.logger, promWG)
			s.maybeShutdown(err)
		}
		promListener := stats.NewPrometheusListener(promMux, cm, s.logger)
		s.addOnShutdown(promListener.Shutdown)
	}

	defer s.shutdown(nil)
	<-s.shutdownChan
}

func (s *server) addOnShutdown(f func()) {
	if f != nil {
		s.onShutdown = append(s.onShutdown, f)
	}
}

func (s *server) shutdown(err error) {
	if err != nil {
		s.logger.Log("msg", "Shutting down due to fatal error.", "error", err)
	}
	for idx := len(s.onShutdown) - 1; idx >= 0; idx-- {
		s.onShutdown[idx]()
	}
	if err == nil {
		s.logger.Log("msg", "Shutdown.")
	} else {
		s.logger.Log("msg", "Shutdown due to fatal error.", "error", err)
		os.Exit(1)
	}
}

func (s *server) maybeShutdown(err error) {
	if err != nil {
		s.shutdown(err)
	}
}

func (s *server) ensureRMId() error {
	path := s.dataDir + "/rmid"
	if b, err := ioutil.ReadFile(path); err == nil {
		s.rmId = common.RMId(binary.BigEndian.Uint32(b))
		return nil

	} else {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for s.rmId == common.RMIdEmpty {
			s.rmId = common.RMId(rng.Uint32())
		}
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(s.rmId))
		return ioutil.WriteFile(path, b, 0400)
	}
}

func (s *server) ensureBootCount() error {
	path := s.dataDir + "/bootcount"
	if b, err := ioutil.ReadFile(path); err == nil {
		s.bootCount = binary.BigEndian.Uint32(b) + 1
	} else {
		s.bootCount = 1
	}
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, s.bootCount)
	return ioutil.WriteFile(path, b, 0600)
}

func (s *server) commandLineConfig() (*configuration.Configuration, error) {
	if s.configFile != "" {
		configJSON, err := configuration.LoadJSONFromPath(s.configFile)
		if err != nil {
			return nil, err
		}
		return configJSON.ToConfiguration(), nil
	}
	return nil, nil
}

func (s *server) SignalShutdown() {
	// this may fail if stdout has died
	s.logger.Log("msg", "Shutdown requested.")
	if atomic.AddInt32(&s.shutdownCounter, 1) == 1 {
		s.shutdownChan <- goshawk.EmptyStructVal
	}
}

func (s *server) signalStatus() {
	sc := goshawk.NewStatusConsumer()
	go sc.Consume(func(str string) {
		s.logger.Log("msg", "System Status Start", "RMId", s.rmId)
		os.Stderr.WriteString(str + "\n")
		s.logger.Log("msg", "System Status End", "RMId", s.rmId)
	})
	sc.Emit(fmt.Sprintf("Configuration File: %v", s.configFile))
	sc.Emit(fmt.Sprintf("Data Directory: %v", s.dataDir))
	sc.Emit(fmt.Sprintf("Port: %v", s.port))
	s.connectionManager.Status(sc)
}

func (s *server) signalReloadConfig() {
	if s.configFile == "" {
		s.logger.Log("msg", "Attempt to reload config failed as no path to configuration provided on command line.")
		return
	}
	config, err := configuration.LoadJSONFromPath(s.configFile)
	if err != nil {
		s.logger.Log("msg", "Cannot reload config due to error.", "error", err)
		return
	}
	s.transmogrifier.RequestConfigurationChange(config.ToConfiguration())
}

func (s *server) signalDumpStacks() {
	size := 16384
	for {
		buf := make([]byte, size)
		if l := runtime.Stack(buf, true); l <= size {
			s.logger.Log("msg", "Stacks Dump Start", "RMId", s.rmId)
			os.Stderr.Write(buf[:l])
			s.logger.Log("msg", "Stacks Dump End", "RMId", s.rmId)
			return
		} else {
			size += size
		}
	}
}

func (s *server) signalToggleCpuProfile() {
	memFile, err := ioutil.TempFile("", common.ProductName+"_Mem_Profile_")
	if goshawk.CheckWarn(err, s.logger) {
		return
	}
	if goshawk.CheckWarn(pprof.Lookup("heap").WriteTo(memFile, 0), s.logger) {
		return
	}
	if !goshawk.CheckWarn(memFile.Close(), s.logger) {
		s.logger.Log("msg", "Memory profile written.", "file", memFile.Name())
	}

	if s.profileFile == nil {
		profFile, err := ioutil.TempFile("", common.ProductName+"_CPU_Profile_")
		if goshawk.CheckWarn(err, s.logger) {
			return
		}
		if goshawk.CheckWarn(pprof.StartCPUProfile(profFile), s.logger) {
			return
		}
		s.profileFile = profFile
		s.logger.Log("msg", "Profiling started.", "file", profFile.Name())

	} else {
		pprof.StopCPUProfile()
		if !goshawk.CheckWarn(s.profileFile.Close(), s.logger) {
			s.logger.Log("msg", "Profiling stopped.", "file", s.profileFile.Name())
		}
		s.profileFile = nil
	}
}

func (s *server) signalToggleTrace() {
	if s.traceFile == nil {
		traceFile, err := ioutil.TempFile("", common.ProductName+"_Trace_")
		if goshawk.CheckWarn(err, s.logger) {
			return
		}
		if goshawk.CheckWarn(trace.Start(traceFile), s.logger) {
			return
		}
		s.traceFile = traceFile
		s.logger.Log("msg", "Tracing started.", "file", traceFile.Name())

	} else {
		trace.Stop()
		if !goshawk.CheckWarn(s.traceFile.Close(), s.logger) {
			s.logger.Log("msg", "Tracing stopped.", "file", s.traceFile.Name())
		}
		s.traceFile = nil
	}
}

func (s *server) signalHandler() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGPIPE, syscall.SIGQUIT, syscall.SIGHUP, syscall.SIGUSR1, syscall.SIGUSR2, os.Interrupt)
	for {
		sig := <-sigs
		switch sig {
		case syscall.SIGPIPE:
			if _, err := os.Stdout.WriteString("Socket has closed\n"); err != nil {
				// stdout has errored; probably whatever we were being
				// piped to has died.
				s.SignalShutdown()
			} else if _, err := os.Stderr.WriteString("Socket has closed\n"); err != nil {
				// ahh, it's stderr that has errored. Same reasoning as above.
				s.SignalShutdown()
			}
		case syscall.SIGTERM, syscall.SIGINT:
			s.SignalShutdown()
		case syscall.SIGHUP:
			s.signalReloadConfig()
		case syscall.SIGQUIT:
			s.signalDumpStacks()
		case syscall.SIGUSR1:
			s.signalStatus()
		case syscall.SIGUSR2:
			s.signalToggleCpuProfile()
			//s.signalToggleTrace()
		}
	}
}
