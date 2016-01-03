package configuration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
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
	fingerprints                  map[[sha256.Size]byte]server.EmptyStruct
}

func (a *Configuration) Equal(b *Configuration) bool {
	if !(a.ClusterId == b.ClusterId && a.Version == b.Version && a.F == b.F && a.MaxRMCount == b.MaxRMCount && a.AsyncFlush == b.AsyncFlush && len(a.Hosts) == len(b.Hosts)) {
		return false
	}
	for idx, aHost := range a.Hosts {
		if aHost != b.Hosts[idx] {
			return false
		}
	}
	if len(a.fingerprints) != len(b.fingerprints) {
		return false
	}
	for fingerprint := range b.fingerprints {
		if _, found := a.fingerprints[fingerprint]; !found {
			return false
		}
	}
	return true
}

func (c *Configuration) String() string {
	return fmt.Sprintf("Configuration{ClusterId: %v, Version: %v, Hosts: %v, F: %v, MaxRMCount: %v, AsyncFlush: %v}",
		c.ClusterId, c.Version, c.Hosts, c.F, c.MaxRMCount, c.AsyncFlush)
}

func (c *Configuration) Fingerprints() map[[sha256.Size]byte]server.EmptyStruct {
	return c.fingerprints
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
	}
	return &config, err
}

// Also checks we are in there somewhere
func (config *Configuration) LocalRemoteHosts(listenPort int) (string, []string, error) {
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
