package configuration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	"net"
	"os"
	"strconv"
)

type Configuration struct {
	ClusterId                     string
	Version                       uint32
	Hosts                         []string
	F                             uint8
	MaxRMCount                    uint8
	AsyncFlush                    bool
	ClientCertificateFingerprints []string
	rms                           common.RMIds
	rmsRemoved                    map[common.RMId]server.EmptyStruct
	fingerprints                  map[[sha256.Size]byte]server.EmptyStruct
	nextConfiguration             *Configuration
}

func LoadConfigurationFromPath(path string) (*Configuration, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	config, err := decodeConfiguration(decoder)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func decodeConfiguration(decoder *json.Decoder) (*Configuration, error) {
	var config Configuration
	err := decoder.Decode(&config)
	if err != nil {
		return nil, err
	}
	if config.ClusterId == "" {
		return nil, fmt.Errorf("Invalid configuration cluster id must not be empty")
	}
	if config.Version < 1 {
		return nil, fmt.Errorf("Invalid configuration version (must be > 0): %v", config.Version)
	}
	if len(config.Hosts) == 0 {
		return nil, fmt.Errorf("Invalid configuration: empty hosts")
	}
	twoFInc := (2 * int(config.F)) + 1
	if twoFInc > len(config.Hosts) {
		return nil, fmt.Errorf("F given as %v, requires minimum 2F+1=%v hosts but only %v hosts specified.",
			config.F, twoFInc, len(config.Hosts))
	}
	if config.MaxRMCount == 0 && twoFInc < 128 {
		config.MaxRMCount = uint8(2 * twoFInc)
	} else if int(config.MaxRMCount) < twoFInc {
		return nil, fmt.Errorf("MaxRMCount given as %v but must be at least 2F+1=%v.", config.MaxRMCount, twoFInc)
	}
	for idx, hostPort := range config.Hosts {
		port := common.DefaultPort
		hostOnly := hostPort
		if host, portStr, err := net.SplitHostPort(hostPort); err == nil {
			portInt64, err := strconv.ParseUint(portStr, 0, 16)
			if err != nil {
				return nil, err
			}
			port = int(portInt64)
			hostOnly = host
		}
		hostPort = net.JoinHostPort(hostOnly, fmt.Sprint(port))
		config.Hosts[idx] = hostPort
		if _, err := net.ResolveTCPAddr("tcp", hostPort); err != nil {
			return nil, err
		}
	}
	if len(config.ClientCertificateFingerprints) == 0 {
		return nil, errors.New("No ClientCertificateFingerprints defined")
	} else {
		fingerprints := make(map[[sha256.Size]byte]server.EmptyStruct, len(config.ClientCertificateFingerprints))
		for _, fingerprint := range config.ClientCertificateFingerprints {
			fingerprintBytes, err := hex.DecodeString(fingerprint)
			if err != nil {
				return nil, err
			} else if l := len(fingerprintBytes); l != sha256.Size {
				return nil, fmt.Errorf("Invalid fingerprint: expected %v bytes, and found %v", sha256.Size, l)
			}
			ary := [sha256.Size]byte{}
			copy(ary[:], fingerprintBytes)
			fingerprints[ary] = server.EmptyStructVal
		}
		config.fingerprints = fingerprints
		config.ClientCertificateFingerprints = nil
	}
	return &config, err
}

func ConfigurationFromCap(config *msgs.Configuration) *Configuration {
	c := &Configuration{
		ClusterId:  config.ClusterId(),
		Version:    config.Version(),
		Hosts:      config.Hosts().ToArray(),
		F:          config.F(),
		MaxRMCount: config.MaxRMCount(),
		AsyncFlush: config.AsyncFlush(),
	}

	rms := config.Rms()
	c.rms = make([]common.RMId, rms.Len())
	for idx := range c.rms {
		c.rms[idx] = common.RMId(rms.At(idx))
	}

	rmsRemoved := config.RmsRemoved()
	c.rmsRemoved = make(map[common.RMId]server.EmptyStruct, rmsRemoved.Len())
	for idx, l := 0, rmsRemoved.Len(); idx < l; idx++ {
		c.rmsRemoved[common.RMId(rmsRemoved.At(idx))] = server.EmptyStructVal
	}

	fingerprints := config.Fingerprints()
	fingerprintsMap := make(map[[sha256.Size]byte]server.EmptyStruct, fingerprints.Len())
	for idx, l := 0, fingerprints.Len(); idx < l; idx++ {
		ary := [sha256.Size]byte{}
		copy(ary[:], fingerprints.At(idx))
		fingerprintsMap[ary] = server.EmptyStructVal
	}
	c.fingerprints = fingerprintsMap

	if config.Which() == msgs.CONFIGURATION_TRANSITIONINGTO {
		nextConfig := config.TransitioningTo()
		c.nextConfiguration = ConfigurationFromCap(&nextConfig)
	}

	return c
}

func (a *Configuration) Equal(b *Configuration) bool {
	if !(a.ClusterId == b.ClusterId && a.Version == b.Version && a.F == b.F && a.MaxRMCount == b.MaxRMCount && a.AsyncFlush == b.AsyncFlush && len(a.Hosts) == len(b.Hosts) && len(a.fingerprints) == len(b.fingerprints) && len(a.rms) == len(b.rms) && len(a.rmsRemoved) == len(b.rmsRemoved)) {
		return false
	}
	for idx, aHost := range a.Hosts {
		if aHost != b.Hosts[idx] {
			return false
		}
	}
	for idx, aRM := range a.rms {
		if aRM != b.rms[idx] {
			return false
		}
	}
	for aRM := range a.rmsRemoved {
		if _, found := b.rmsRemoved[aRM]; !found {
			return false
		}
	}
	for fingerprint := range b.fingerprints {
		if _, found := a.fingerprints[fingerprint]; !found {
			return false
		}
	}
	if a.nextConfiguration == nil || b.nextConfiguration == nil {
		return a.nextConfiguration == b.nextConfiguration
	}
	return a.nextConfiguration.Equal(b.nextConfiguration)
}

func (config *Configuration) String() string {
	return fmt.Sprintf("Configuration{ClusterId: %v, Version: %v, Hosts: %v, F: %v, MaxRMCount: %v, AsyncFlush: %v, RMs: %v}",
		config.ClusterId, config.Version, config.Hosts, config.F, config.MaxRMCount, config.AsyncFlush, config.rms)
}

func (config *Configuration) Fingerprints() map[[sha256.Size]byte]server.EmptyStruct {
	return config.fingerprints
}

func (config *Configuration) Next() *Configuration {
	return config.nextConfiguration
}

func (config *Configuration) SetNext(next *Configuration) {
	config.nextConfiguration = next
}

func (config *Configuration) RMs() common.RMIds {
	return config.rms
}

func (config *Configuration) SetRMs(rms common.RMIds) {
	config.rms = rms
}

func (config *Configuration) RMsRemoved() map[common.RMId]server.EmptyStruct {
	return config.rmsRemoved
}

func (config *Configuration) Clone() *Configuration {
	clone := &Configuration{
		ClusterId:                     config.ClusterId,
		Version:                       config.Version,
		Hosts:                         make([]string, len(config.Hosts)),
		F:                             config.F,
		MaxRMCount:                    config.MaxRMCount,
		AsyncFlush:                    config.AsyncFlush,
		ClientCertificateFingerprints: make([]string, len(config.ClientCertificateFingerprints)),
		rms:               make([]common.RMId, len(config.rms)),
		rmsRemoved:        make(map[common.RMId]server.EmptyStruct, len(config.rmsRemoved)),
		fingerprints:      make(map[[sha256.Size]byte]server.EmptyStruct, len(config.fingerprints)),
		nextConfiguration: config.nextConfiguration,
	}

	copy(clone.Hosts, config.Hosts)
	copy(clone.ClientCertificateFingerprints, config.ClientCertificateFingerprints)
	copy(clone.rms, config.rms)
	for k, v := range config.rmsRemoved {
		clone.rmsRemoved[k] = v
	}
	for k, v := range config.fingerprints {
		clone.fingerprints[k] = v
	}
	if clone.nextConfiguration != nil {
		clone.nextConfiguration = clone.nextConfiguration.Clone()
	}
	return clone
}

func (config *Configuration) AddToSegAutoRoot(seg *capn.Segment) msgs.Configuration {
	cap := msgs.AutoNewConfiguration(seg)
	cap.SetClusterId(config.ClusterId)
	cap.SetVersion(config.Version)

	hosts := seg.NewTextList(len(config.Hosts))
	cap.SetHosts(hosts)
	for idx, host := range config.Hosts {
		hosts.Set(idx, host)
	}

	cap.SetF(config.F)
	cap.SetMaxRMCount(config.MaxRMCount)
	cap.SetAsyncFlush(config.AsyncFlush)

	rms := seg.NewUInt32List(len(config.rms))
	cap.SetRms(rms)
	for idx, rmId := range config.rms {
		rms.Set(idx, uint32(rmId))
	}

	rmsRemoved := seg.NewUInt32List(len(config.rmsRemoved))
	cap.SetRmsRemoved(rmsRemoved)
	idx := 0
	for rmId := range config.rmsRemoved {
		rmsRemoved.Set(idx, uint32(rmId))
		idx++
	}

	fingerprintsMap := config.fingerprints
	fingerprints := seg.NewDataList(len(fingerprintsMap))
	cap.SetFingerprints(fingerprints)
	idx = 0
	for fingerprint := range fingerprintsMap {
		fingerprints.Set(idx, fingerprint[:])
		idx++
	}

	if config.nextConfiguration == nil {
		cap.SetStable()
	} else {
		cap.SetTransitioningTo(config.nextConfiguration.AddToSegAutoRoot(seg))
	}
	return cap
}

func (config *Configuration) Serialize() []byte {
	seg := capn.NewBuffer(nil)
	config.AddToSegAutoRoot(seg)
	return server.SegToBytes(seg)
}

// Also checks we are in there somewhere
func (config *Configuration) LocalRemoteHosts(listenPort uint16) (string, []string, error) {
	listenPortStr := fmt.Sprint(listenPort)
	localIPs, err := LocalAddresses()
	if err != nil {
		return "", nil, err
	}
	var localHost string
	localCount := 0
	remoteHosts := make([]string, 0, len(config.Hosts)-1)
	for _, configHostPort := range config.Hosts {
		configHost, configPort, err := net.SplitHostPort(configHostPort)
		if err != nil {
			return "", nil, err
		}
		if listenPortStr != configPort {
			remoteHosts = append(remoteHosts, configHostPort)
			continue
		}
		configIPs, err := net.LookupIP(configHost)
		if err != nil {
			return "", nil, err
		}
		isLocal := false
	Outer:
		for _, configIP := range configIPs {
			for _, localIP := range localIPs {
				if isLocal = localIP.Equal(configIP); isLocal {
					break Outer
				}
			}
		}
		if isLocal {
			localCount++
			if localCount > 1 {
				return "", nil, fmt.Errorf("Multiple hosts in config map to local interfaces. %v", localIPs)
			}
			localHost = configHostPort
		} else {
			remoteHosts = append(remoteHosts, configHostPort)
		}
	}
	if localCount == 0 {
		return "", nil, fmt.Errorf("Unable to find any local interface in configuration. %v", localIPs)
	} else {
		return localHost, remoteHosts, nil
	}
}

func LocalAddresses() ([]net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	result := make([]net.IP, len(addrs))
	for idx, addr := range addrs {
		ip, _, err := net.ParseCIDR(addr.String())
		if err != nil {
			return nil, err
		}
		result[idx] = ip
	}
	return result, nil
}
