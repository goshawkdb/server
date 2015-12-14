package configuration

import (
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/crypto/bcrypt"
	"goshawkdb.io/common"
	"net"
	"os"
	"strconv"
)

type Configuration struct {
	ClusterId  string
	Version    uint32
	Hosts      []string
	F          uint8
	MaxRMCount uint8
	AsyncFlush bool
	Accounts   map[string]string
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
	if len(a.Accounts) != len(b.Accounts) {
		return false
	}
	for un, pwA := range a.Accounts {
		if pwB, found := b.Accounts[un]; !found || pwA != pwB {
			return false
		}
	}
	return true
}

func (c *Configuration) String() string {
	return fmt.Sprintf("Configuration{ClusterId: %v, Version: %v, Hosts: %v, F: %v, MaxRMCount: %v, AsyncFlush: %v}",
		c.ClusterId, c.Version, c.Hosts, c.F, c.MaxRMCount, c.AsyncFlush)
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
	if len(config.Accounts) == 0 {
		return nil, errors.New("No accounts defined")
	} else {
		for un, pw := range config.Accounts {
			if cost, err := bcrypt.Cost([]byte(pw)); err != nil {
				return nil, fmt.Errorf("Error in password for account %v: %v", un, err)
			} else if cost < bcrypt.DefaultCost {
				return nil, fmt.Errorf("Error in password for account %v: cost too low (%v)", un, cost)
			}
		}
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
