package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"goshawkdb.io/common"
	"goshawkdb.io/common/certs"
	goshawk "goshawkdb.io/server"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/server/db"
	"goshawkdb.io/server/network"
	"io/ioutil"
	"log"
	"math/rand"
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
	log.SetPrefix(common.ProductName + " ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Println(os.Args)

	s, err := newServer()
	goshawk.CheckFatal(err)
	s.start()
}

func newServer() (*server, error) {
	var configFile, dataDir, certFile string
	var port int
	var version, genClusterCert, genClientCert bool

	flag.StringVar(&configFile, "config", "", "`Path` to configuration file")
	flag.StringVar(&dataDir, "dir", "", "`Path` to data directory")
	flag.StringVar(&certFile, "cert", "", "`Path` to cluster certificate and key file")
	flag.IntVar(&port, "port", common.DefaultPort, "Port to listen on")
	flag.BoolVar(&version, "version", false, "Display version and exit")
	flag.BoolVar(&genClusterCert, "gen-cluster-cert", false, "Generate new cluster certificate key pair")
	flag.BoolVar(&genClientCert, "gen-client-cert", false, "Generate client certificate key pair")
	flag.Parse()

	if version {
		log.Printf("%v version %v", common.ProductName, goshawk.ServerVersion)
		os.Exit(0)
	}

	if genClusterCert {
		certificatePrivateKeyPair, err := certs.NewClusterCertificate()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%v%v", certificatePrivateKeyPair.CertificatePEM, certificatePrivateKeyPair.PrivateKeyPEM)
		os.Exit(0)
	}

	if len(certFile) == 0 {
		return nil, fmt.Errorf("No certificate supplied (missing -cert parameter). Use -gen-cluster-cert to create cluster certificate")
	}
	certificate, err := ioutil.ReadFile(certFile)
	if err != nil {
		return nil, err
	}

	if genClientCert {
		certificatePrivateKeyPair, err := certs.NewClientCertificate(certificate)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%v%v", certificatePrivateKeyPair.CertificatePEM, certificatePrivateKeyPair.PrivateKeyPEM)
		fingerprint := sha256.Sum256(certificatePrivateKeyPair.Certificate)
		log.Printf("Fingerprint: %v\n", hex.EncodeToString(fingerprint[:]))
		os.Exit(0)
	}

	if dataDir == "" {
		dataDir, err = ioutil.TempDir("", common.ProductName+"_Data_")
		if err != nil {
			return nil, err
		}
		log.Printf("No data dir supplied (missing -dir parameter). Using %v for data.\n", dataDir)
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
		return nil, fmt.Errorf("Supplied port is illegal (%v). Port must be > 0 and < 65536", port)
	}

	s := &server{
		configFile:  configFile,
		certificate: certificate,
		dataDir:     dataDir,
		port:        uint16(port),
		onShutdown:  []func(){},
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
	sync.WaitGroup
	configFile        string
	certificate       []byte
	dataDir           string
	port              uint16
	rmId              common.RMId
	bootCount         uint32
	connectionManager *network.ConnectionManager
	transmogrifier    *network.TopologyTransmogrifier
	profileFile       *os.File
	traceFile         *os.File
	onShutdown        []func()
}

func (s *server) start() {
	procs := runtime.NumCPU()
	if procs < 2 {
		procs = 2
	}
	runtime.GOMAXPROCS(procs)

	nodeCertPrivKeyPair, err := certs.GenerateNodeCertificatePrivateKeyPair(s.certificate)
	s.certificate = nil

	s.maybeShutdown(err)

	disk, err := mdbs.NewMDBServer(s.dataDir, mdb.WRITEMAP, 0600, goshawk.MDBInitialSize, procs/2, time.Millisecond, db.DB)
	s.addOnShutdown(disk.Shutdown)
	s.maybeShutdown(err)

	commandLineConfig, err := s.commandLineConfig()
	s.maybeShutdown(err)
	clusterId := ""
	if commandLineConfig != nil {
		clusterId = commandLineConfig.ClusterId
	}

	cm, transmogrifier, err := network.NewConnectionManager(s.rmId, s.bootCount, procs, disk, nodeCertPrivKeyPair, clusterId, s.port)
	s.addOnShutdown(cm.Shutdown)
	s.addOnShutdown(transmogrifier.Shutdown)
	s.maybeShutdown(err)
	s.connectionManager = cm
	s.transmogrifier = transmogrifier

	s.Add(1)
	go s.signalHandler()

	if commandLineConfig != nil {
		resultPromise := transmogrifier.RequestConfigurationChange(commandLineConfig)
		go func() {
			err := resultPromise()
			if err != nil {
				log.Println(err)
			}
		}()
	}

	listener, err := network.NewListener(s.port, cm)
	s.addOnShutdown(listener.Shutdown)
	s.maybeShutdown(err)

	defer s.shutdown(nil)
	s.Wait()
}

func (s *server) addOnShutdown(f func()) {
	if f != nil {
		s.onShutdown = append(s.onShutdown, f)
	}
}

func (s *server) shutdown(err error) {
	for idx := len(s.onShutdown) - 1; idx >= 0; idx-- {
		s.onShutdown[idx]()
	}
	if err == nil {
		log.Println("Shutdown.")
	} else {
		log.Fatal("Shutdown due to fatal error: ", err)
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
		return configuration.LoadConfigurationFromPath(s.configFile)
	}
	return nil, nil
}

/*
func (s *server) chooseTopology(topology *configuration.Topology) (*configuration.Topology, error) {
	var config *configuration.Configuration
	if s.configFile != "" {
		var err error
		config, err = configuration.LoadConfigurationFromPath(s.configFile)
		if err != nil {
			return nil, err
		}
	}

	switch {
	case topology == nil && config == nil:
		return nil, fmt.Errorf("Local data store is empty and no external config supplied. Must supply config with -config")
	case topology == nil:
		return configuration.NewTopology(config), nil
	case config == nil:
		return topology, nil
	case topology.Configuration.Equal(config):
		return topology, nil
	case topology.ClusterId != config.ClusterId:
		return nil, fmt.Errorf("Local data store is configured for cluster '%v', but supplied config is for cluster '%v'. Cannot continue. Either adjust config or use clean data directory", topology.ClusterId, config.ClusterId)
	case topology.Version > config.Version:
		return topology, nil
	case topology.Version == config.Version:
		return nil, fmt.Errorf("Configuration has same version number but different contents. Did you forget to increase the version number? (%v)", topology.Version)
	default:
		return configuration.NewTopology(config), nil
	}
}
*/

func (s *server) signalShutdown() {
	log.Println("Shutting down.")
	s.Done()
}

func (s *server) signalStatus() {
	sc := goshawk.NewStatusConsumer()
	go sc.Consume(func(str string) {
		log.Printf("System Status for %v\n%v\nStatus End\n", s.rmId, str)
	})
	sc.Emit(fmt.Sprintf("Configuration File: %v", s.configFile))
	sc.Emit(fmt.Sprintf("Data Directory: %v", s.dataDir))
	sc.Emit(fmt.Sprintf("Port: %v", s.port))
	s.connectionManager.Status(sc)
}

func (s *server) signalReloadConfig() {
	if s.configFile == "" {
		log.Println("Attempt to reload config failed as no path to configuration provided on command line.")
		return
	}
	config, err := configuration.LoadConfigurationFromPath(s.configFile)
	if err != nil {
		log.Println("Cannot reload config due to error:", err)
		return
	}
	s.transmogrifier.RequestConfigurationChange(config)
}

func (s *server) signalDumpStacks() {
	size := 16384
	for {
		buf := make([]byte, size)
		if l := runtime.Stack(buf, true); l < size {
			log.Printf("Stacks dump\n%s\nStacks dump end", buf[:l])
			return
		} else {
			size += size
		}
	}
}

func (s *server) signalToggleCpuProfile() {
	if s.profileFile == nil {
		memFile, err := ioutil.TempFile("", common.ProductName+"_Mem_Profile_")
		if goshawk.CheckWarn(err) {
			return
		}
		if goshawk.CheckWarn(pprof.Lookup("heap").WriteTo(memFile, 0)) {
			return
		}
		if !goshawk.CheckWarn(memFile.Close()) {
			log.Println("Memory profile written to", memFile.Name())
		}

		profFile, err := ioutil.TempFile("", common.ProductName+"_CPU_Profile_")
		if goshawk.CheckWarn(err) {
			return
		}
		if goshawk.CheckWarn(pprof.StartCPUProfile(profFile)) {
			return
		}
		s.profileFile = profFile
		log.Println("Profiling started in", profFile.Name())

	} else {
		pprof.StopCPUProfile()
		if !goshawk.CheckWarn(s.profileFile.Close()) {
			log.Println("Profiling stopped in", s.profileFile.Name())
		}
		s.profileFile = nil
	}
}

func (s *server) signalToggleTrace() {
	if s.traceFile == nil {
		traceFile, err := ioutil.TempFile("", common.ProductName+"_Trace_")
		if goshawk.CheckWarn(err) {
			return
		}
		if goshawk.CheckWarn(trace.Start(traceFile)) {
			return
		}
		s.traceFile = traceFile
		log.Println("Tracing started in", traceFile.Name())

	} else {
		trace.Stop()
		if !goshawk.CheckWarn(s.traceFile.Close()) {
			log.Println("Tracing stopped in", s.traceFile.Name())
		}
		s.traceFile = nil
	}
}

func (s *server) signalHandler() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP, syscall.SIGUSR1, syscall.SIGUSR2, os.Interrupt)
	for {
		sig := <-sigs
		switch sig {
		case syscall.SIGTERM, syscall.SIGINT:
			s.signalShutdown()
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
