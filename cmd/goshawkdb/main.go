package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"github.com/go-kit/kit/log"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	"goshawkdb.io/common/certs"
	goshawk "goshawkdb.io/server"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/connectionmanager"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/localconnection"
	ghttp "goshawkdb.io/server/network/http"
	"goshawkdb.io/server/network/tcpcapnproto"
	"goshawkdb.io/server/network/websocketmsgpack"
	"goshawkdb.io/server/paxos"
	"goshawkdb.io/server/router"
	"goshawkdb.io/server/stats"
	"goshawkdb.io/server/topologytransmogrifier"
	"goshawkdb.io/server/types"
	"goshawkdb.io/server/utils"
	"goshawkdb.io/server/utils/status"
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
	"syscall"
	"time"
)

func main() {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)

	logger.Log("product", common.ProductName, "version", goshawk.ServerVersion, "mdbVersion", mdb.Version(), "args", fmt.Sprint(os.Args))

	f := &flags{}
	f.registerFlags()
	f.parse()

	if terminate, err := f.validate(logger); err != nil {
		fmt.Printf("\n%v\n\n", err)
		flag.Usage()
		fmt.Println("\nSee https://goshawkdb.io/starting.html for the Getting Started guide.")
		os.Exit(1)

	} else if terminate {
		os.Exit(0)

	} else {
		newServer(f, logger).start()
	}
}

type flags struct {
	configFile     string
	dataDir        string
	certFile       string
	port           uint64
	wssPort        uint64
	promPort       uint64
	httpProf       bool
	version        bool
	genClusterCert bool
	genClientCert  bool

	certificate []byte
}

func (f *flags) registerFlags() {
	flag.StringVar(&f.configFile, "config", "", "`Path` to configuration file (required to start server).")
	flag.StringVar(&f.dataDir, "dir", "", "`Path` to data directory (required to run server).")
	flag.StringVar(&f.certFile, "cert", "", "`Path` to cluster certificate and key file (required to run server).")
	flag.Uint64Var(&f.port, "port", common.DefaultPort, "Port to listen on (required if non-default).")
	flag.Uint64Var(&f.wssPort, "wssPort", common.DefaultWSSPort, "Port to provide WebSocket service on (required if non-default. Set to 0 to disable WebSocket service).")
	flag.Uint64Var(&f.promPort, "prometheusPort", common.DefaultPrometheusPort, "Port to provide HTTP for Prometheus metrics service on (required if non-default. Set to 0 to disable Prometheus metrics service).")

	flag.BoolVar(&f.httpProf, "httpProfile", false, fmt.Sprintf("Enable Go HTTP Profiling on port localhost:%d.", goshawk.HttpProfilePort))

	flag.BoolVar(&f.genClusterCert, "gen-cluster-cert", false, "Generate new cluster certificate key pair and exit.")
	flag.BoolVar(&f.genClientCert, "gen-client-cert", false, "Generate client certificate key pair and exit.")
	flag.BoolVar(&f.version, "version", false, "Display version and exit.")
}

func (f *flags) parse() {
	flag.Parse()
}

func (f *flags) validate(logger log.Logger) (bool, error) {
	if f.version {
		return true, nil
	}

	var err error
	if f.genClusterCert {
		certificatePrivateKeyPair, err := certs.NewClusterCertificate()
		if err != nil {
			return false, err
		}
		fmt.Printf("%v%v", certificatePrivateKeyPair.CertificatePEM, certificatePrivateKeyPair.PrivateKeyPEM)
		return true, nil
	}

	if len(f.certFile) == 0 {
		return false, errors.New("No certificate supplied (missing -cert parameter). Use -gen-cluster-cert to create cluster certificate.")
	}
	f.certificate, err = ioutil.ReadFile(f.certFile)
	if err != nil {
		return false, err
	}

	if f.genClientCert {
		certificatePrivateKeyPair, err := certs.NewClientCertificate(f.certificate)
		if err != nil {
			return false, err
		}
		fmt.Printf("%v%v", certificatePrivateKeyPair.CertificatePEM, certificatePrivateKeyPair.PrivateKeyPEM)
		fingerprint := sha256.Sum256(certificatePrivateKeyPair.Certificate)
		fmt.Printf("fingerprint: %s", hex.EncodeToString(fingerprint[:]))
		return true, nil
	}

	if len(f.dataDir) == 0 {
		f.dataDir, err = ioutil.TempDir("", common.ProductName+"_Data_")
		if err != nil {
			return false, err
		}
		logger.Log("msg", "No data dir supplied (missing -dir parameter). Using temporary dir.", "dataDir", f.dataDir)
	}

	if !(0 < f.port && f.port < 65536) {
		return false, fmt.Errorf("Supplied port is illegal (%d). Port must be > 0 and < 65536", f.port)
	}

	if f.wssPort > 0 && !(f.wssPort < 65536 && f.wssPort != f.port) {
		return false, fmt.Errorf("Supplied WSS port is illegal (%d). WSS Port must be > 0 and < 65536 and not equal to the main communication port (%d)", f.wssPort, f.port)
	}

	if f.promPort > 0 && !(f.promPort < 65536 && f.promPort != f.port) {
		return false, fmt.Errorf("Supplied Prometheus port is illegal (%d). Prometheus Port must be > 0 and < 65536 and not equal to the main communication port (%d)", f.promPort, f.port)
	}

	return false, nil
}

type server struct {
	*flags
	logger   log.Logger
	port     uint16
	wssPort  uint16
	promPort uint16

	self      common.RMId
	bootCount uint32

	lock           sync.Mutex
	transmogrifier *topologytransmogrifier.TopologyTransmogrifier
	statusEmitters []status.StatusEmitter
	onShutdown     []func()

	profileFile *os.File
	traceFile   *os.File

	shutdownChan chan types.EmptyStruct
}

func newServer(f *flags, logger log.Logger) *server {
	return &server{
		flags:    f,
		logger:   logger,
		port:     uint16(f.port),
		wssPort:  uint16(f.wssPort),
		promPort: uint16(f.promPort),

		statusEmitters: []status.StatusEmitter{},
		onShutdown:     []func(){},

		shutdownChan: make(chan types.EmptyStruct),
	}
}

func (s *server) start() {
	os.Stdin.Close()

	procs := runtime.NumCPU()
	if procs < 2 {
		procs = 2
	}
	runtime.GOMAXPROCS(procs)

	go s.signalHandler()

	s.maybeShutdown(os.MkdirAll(s.dataDir, 0700))
	s.maybeShutdown(s.ensureRMId())
	s.maybeShutdown(s.ensureBootCount())

	commandLineConfig, err := s.commandLineConfig()
	s.maybeShutdown(err)

	if s.httpProf {
		go func() {
			s.logger.Log("pprofResult", http.ListenAndServe(fmt.Sprintf("localhost:%d", goshawk.HttpProfilePort), nil))
		}()
	}

	disk, err := mdbs.NewMDBServer(s.dataDir, 0, 0600, goshawk.MDBInitialSize, 500*time.Microsecond, db.DB, s.logger)
	s.maybeShutdown(err)
	db := disk.(*db.Databases)
	s.addOnShutdown(db.Shutdown)

	router := router.NewRouter(s.self, s.logger)
	cm := connectionmanager.NewConnectionManager(s.self, s.bootCount, s.certificate, router, s.logger)
	s.certificate = nil
	s.addOnShutdown(cm.ShutdownSync)
	// this is safe because cm only uses router when it's creating new
	// dialers, and it won't be doing that until after
	// TopologyTransmogrifier starts up.
	router.ConnectionManager = cm
	s.addStatusEmitter(cm)

	lc := localconnection.NewLocalConnection(s.self, s.bootCount, cm, s.logger)
	// localConnection registers as a client with connectionManager, so
	// we rely on connectionManager to do shutdown and status calls.

	dispatchers := paxos.NewDispatchers(cm, s.self, s.bootCount, uint8(procs), db, lc, s.logger)
	// same reasoning as before: this write is done before
	// TopologyTransmogrifier starts and cm will only dial out due to a
	// msg from TopologyTransmogrifier so there is still sufficient
	// write barriers.
	router.Dispatchers = dispatchers
	s.addStatusEmitter(router)
	s.addOnShutdown(router.ShutdownSync)

	// now all our dispatchers are up, we register ourselves.
	cm.RegisterSelf()

	transmogrifier, localEstablished := topologytransmogrifier.NewTopologyTransmogrifier(s.self, db, router, cm, lc, s.port, s, commandLineConfig, s.logger)
	s.lock.Lock()
	s.transmogrifier = transmogrifier
	s.lock.Unlock()
	s.addOnShutdown(transmogrifier.ShutdownSync)

	<-localEstablished

	sp := stats.NewStatsPublisher(cm, lc, s.logger)
	s.addOnShutdown(sp.ShutdownSync)

	listener, err := tcpcapnproto.NewListener(s.port, s.self, s.bootCount, router, cm, lc, s.logger)
	s.maybeShutdown(err)
	s.addOnShutdown(listener.ShutdownSync)

	var wssMux, promMux *ghttp.HttpListenerWithMux
	var wssWG, promWG *sync.WaitGroup
	if s.wssPort != 0 {
		wssWG := new(sync.WaitGroup)
		if s.wssPort == s.promPort {
			wssWG.Add(2)
		} else {
			wssWG.Add(1)
		}
		wssMux, err = ghttp.NewHttpListenerWithMux(s.wssPort, cm, s.logger, wssWG)
		s.maybeShutdown(err)
		wssListener := websocketmsgpack.NewWebsocketListener(wssMux, s.self, s.bootCount, cm, lc, s.logger)
		s.addOnShutdown(wssListener.ShutdownSync)
	}

	if s.promPort != 0 {
		if s.wssPort == s.promPort {
			promWG = wssWG
			promMux = wssMux
		} else {
			promWG = new(sync.WaitGroup)
			promWG.Add(1)
			promMux, err = ghttp.NewHttpListenerWithMux(s.promPort, cm, s.logger, promWG)
			s.maybeShutdown(err)
		}
		promListener := stats.NewPrometheusListener(promMux, s.self, cm, router, s.logger)
		s.addOnShutdown(promListener.ShutdownSync)
	}

	<-s.shutdownChan
	s.shutdown(nil)
}

func (s *server) ensureRMId() error {
	path := s.dataDir + "/rmid"
	if b, err := ioutil.ReadFile(path); err == nil {
		s.self = common.RMId(binary.BigEndian.Uint32(b))
		return nil

	} else {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for s.self == common.RMIdEmpty {
			s.self = common.RMId(rng.Uint32())
		}
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(s.self))
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

func (s *server) addStatusEmitter(emitter status.StatusEmitter) {
	s.lock.Lock()
	s.statusEmitters = append(s.statusEmitters, emitter)
	s.lock.Unlock()
}

func (s *server) addOnShutdown(f func()) {
	s.lock.Lock()
	s.onShutdown = append(s.onShutdown, f)
	s.lock.Unlock()
}

func (s *server) shutdown(err error) {
	if err != nil {
		s.logger.Log("msg", "Shutting down due to fatal error.", "error", err)
	}
	s.lock.Lock()
	for idx := len(s.onShutdown) - 1; idx >= 0; idx-- {
		s.onShutdown[idx]()
	}
	s.lock.Unlock()
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

func (s *server) commandLineConfig() (*configuration.Configuration, error) {
	if len(s.configFile) > 0 {
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
	select {
	case <-s.shutdownChan:
	default:
		close(s.shutdownChan)
	}
}

func (s *server) ShutdownSync() {
	s.SignalShutdown()
}

func (s *server) signalStatus() {
	sc := status.NewStatusConsumer()
	go func() {
		str := sc.Wait()
		s.logger.Log("msg", "System Status Start", "RMId", s.self)
		os.Stderr.WriteString(str + "\n")
		s.logger.Log("msg", "System Status End", "RMId", s.self)
	}()
	sc.Emit(fmt.Sprintf("Configuration File: %v", s.configFile))
	sc.Emit(fmt.Sprintf("Data Directory: %v", s.dataDir))
	sc.Emit(fmt.Sprintf("Port: %v", s.port))

	s.lock.Lock()
	for _, emitter := range s.statusEmitters {
		emitter.Status(sc.Fork())
	}
	s.lock.Unlock()
	sc.Join()
}

func (s *server) signalReloadConfig() {
	if len(s.configFile) == 0 {
		s.logger.Log("msg", "Attempt to reload config failed as no path to configuration provided on command line.")
		return
	}
	config, err := configuration.LoadJSONFromPath(s.configFile)
	if err != nil {
		s.logger.Log("msg", "Cannot reload config due to error.", "error", err)
		return
	}
	s.lock.Lock()
	s.transmogrifier.RequestConfigurationChange(config.ToConfiguration())
	s.lock.Unlock()
}

func (s *server) signalDumpStacks() {
	size := 16384
	for {
		buf := make([]byte, size)
		if l := runtime.Stack(buf, true); l <= size {
			s.logger.Log("msg", "Stacks Dump Start", "RMId", s.self)
			os.Stderr.Write(buf[:l])
			s.logger.Log("msg", "Stacks Dump End", "RMId", s.self)
			return
		} else {
			size += size
		}
	}
}

func (s *server) signalToggleCpuProfile() {
	memFile, err := ioutil.TempFile("", common.ProductName+"_Mem_Profile_")
	if utils.CheckWarn(err, s.logger) {
		return
	}
	if utils.CheckWarn(pprof.Lookup("heap").WriteTo(memFile, 0), s.logger) {
		return
	}
	if !utils.CheckWarn(memFile.Close(), s.logger) {
		s.logger.Log("msg", "Memory profile written.", "file", memFile.Name())
	}

	if s.profileFile == nil {
		profFile, err := ioutil.TempFile("", common.ProductName+"_CPU_Profile_")
		if utils.CheckWarn(err, s.logger) {
			return
		}
		if utils.CheckWarn(pprof.StartCPUProfile(profFile), s.logger) {
			return
		}
		s.profileFile = profFile
		s.logger.Log("msg", "Profiling started.", "file", profFile.Name())

	} else {
		pprof.StopCPUProfile()
		if !utils.CheckWarn(s.profileFile.Close(), s.logger) {
			s.logger.Log("msg", "Profiling stopped.", "file", s.profileFile.Name())
		}
		s.profileFile = nil
	}
}

func (s *server) signalToggleTrace() {
	if s.traceFile == nil {
		traceFile, err := ioutil.TempFile("", common.ProductName+"_Trace_")
		if utils.CheckWarn(err, s.logger) {
			return
		}
		if utils.CheckWarn(trace.Start(traceFile), s.logger) {
			return
		}
		s.traceFile = traceFile
		s.logger.Log("msg", "Tracing started.", "file", traceFile.Name())

	} else {
		trace.Stop()
		if !utils.CheckWarn(s.traceFile.Close(), s.logger) {
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
			go s.signalStatus()
		case syscall.SIGUSR2:
			s.signalToggleCpuProfile()
			//s.signalToggleTrace()
		}
	}
}
