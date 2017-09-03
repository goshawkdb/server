package configuration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"goshawkdb.io/common"
	"goshawkdb.io/server"
	"goshawkdb.io/server/types"
	"net"
	"os"
	"sort"
	"strconv"
)

type ConfigurationJSON struct {
	ClusterId                     string
	Version                       uint32
	Hosts                         []string
	F                             uint8
	MaxRMCount                    uint16
	NoSync                        bool
	ClientCertificateFingerprints map[string]map[string]*CapabilityJSON
}

type CapabilityJSON struct {
	Read  bool
	Write bool
}

func (a *CapabilityJSON) Equal(b *CapabilityJSON) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Read == b.Read && a.Write == b.Write
}

func LoadJSONFromPath(path string) (*ConfigurationJSON, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	config := &ConfigurationJSON{}
	if err = decoder.Decode(config); err != nil {
		return nil, err
	}
	if err = config.Validate(); err != nil {
		return nil, err
	}
	return config, nil
}

func (a *ConfigurationJSON) Equal(b *ConfigurationJSON) bool {
	if a == nil || b == nil {
		return a == b
	}
	if !(a.ClusterId == b.ClusterId && a.Version == b.Version && len(a.Hosts) == len(b.Hosts) && a.F == b.F && a.MaxRMCount == b.MaxRMCount && a.NoSync == b.NoSync && len(a.ClientCertificateFingerprints) == len(b.ClientCertificateFingerprints)) {
		return false
	}
	aHosts := make(map[string]types.EmptyStruct, len(a.Hosts))
	for _, aHost := range a.Hosts {
		aHosts[aHost] = types.EmptyStructVal
	}
	for _, bHost := range b.Hosts {
		if _, found := aHosts[bHost]; !found {
			return false
		}
	}
	for fingerprint, aRootsMap := range a.ClientCertificateFingerprints {
		if bRootsMap, found := b.ClientCertificateFingerprints[fingerprint]; found && len(aRootsMap) == len(bRootsMap) {
			for rootName, aCap := range aRootsMap {
				if bCap, found := bRootsMap[rootName]; !found || !aCap.Equal(bCap) {
					return false
				}
			}
		} else {
			return false
		}
	}
	return true
}

func (config *ConfigurationJSON) Validate() error {
	if config.ClusterId == "" {
		return fmt.Errorf("Invalid configuration: cluster id must not be empty.")
	}
	if config.Version < 1 {
		return fmt.Errorf("Invalid configuration: version must be > 0 (was %v).", config.Version)
	}
	if len(config.Hosts) == 0 {
		return fmt.Errorf("Invalid configuration: empty hosts.")
	}
	twoFInc := (2 * int(config.F)) + 1
	if twoFInc > len(config.Hosts) {
		return fmt.Errorf("Invalid configuration: F given as %v, requires minimum 2F+1=%v hosts but only %v hosts specified.",
			config.F, twoFInc, len(config.Hosts))
	}
	if int(config.MaxRMCount) < len(config.Hosts) {
		return fmt.Errorf("Invalid configuration: MaxRMCount given as %v, but must be at least the number of hosts (%v).", config.MaxRMCount, len(config.Hosts))
	}
	for idx, hostPort := range config.Hosts {
		port := common.DefaultPort
		hostOnly := hostPort
		if host, portStr, err := net.SplitHostPort(hostPort); err == nil {
			portInt64, err := strconv.ParseUint(portStr, 0, 16)
			if err != nil {
				return fmt.Errorf("Invalid configuration: Error when parsing host port %v: %v", hostPort, err)
			}
			port = int(portInt64)
			hostOnly = host
		}
		hostPort = net.JoinHostPort(hostOnly, fmt.Sprint(port))
		config.Hosts[idx] = hostPort
		if _, err := net.ResolveTCPAddr("tcp", hostPort); err != nil {
			return fmt.Errorf("Invalid configuration: Error when resolving host %v: %v", hostPort, err)
		}
	}
	if len(config.ClientCertificateFingerprints) == 0 {
		return errors.New("Invalid configuration: No ClientCertificateFingerprints defined")
	} else {
		for fingerprint, roots := range config.ClientCertificateFingerprints {
			fingerprintBytes, err := hex.DecodeString(fingerprint)
			if err != nil {
				return fmt.Errorf("Invalid configuration: Error when decoding fingerprint %v: %v", fingerprint, err)
			} else if l := len(fingerprintBytes); l != sha256.Size {
				return fmt.Errorf("Invalid configuration: When fingerprint %v has the wrong length. Require %v bytes, but got %v.", fingerprint, sha256.Size, l)
			}
			if len(roots) == 0 {
				return fmt.Errorf("Invalid configuration: Account with fingerprint %v has no roots configured. At least 1 needed.", fingerprint)
			}
			for rootName, capability := range roots {
				if !(capability.Read || capability.Write) {
					return fmt.Errorf("Invalid configuration: Account with fingerprint %v, root %s: no capability has been granted.",
						fingerprint, rootName)
				}
				if capability.Write && (rootName == server.ConfigRootName || rootName == server.MetricsRootName) {
					return fmt.Errorf("Invalid configuration: Write capability on root %s is not possible. (Account with fingerprint %v)", rootName, fingerprint)
				}
			}
		}
	}
	return nil
}

func (config *ConfigurationJSON) Clone() *ConfigurationJSON {
	c := &ConfigurationJSON{
		ClusterId:  config.ClusterId,
		Version:    config.Version,
		Hosts:      make([]string, len(config.Hosts)),
		F:          config.F,
		MaxRMCount: config.MaxRMCount,
		NoSync:     config.NoSync,
		ClientCertificateFingerprints: make(map[string]map[string]*CapabilityJSON, len(config.ClientCertificateFingerprints)),
	}
	for idx, host := range config.Hosts {
		c.Hosts[idx] = host
	}
	for fingerprint, roots := range config.ClientCertificateFingerprints {
		r := make(map[string]*CapabilityJSON, len(roots))
		c.ClientCertificateFingerprints[fingerprint] = r
		for root, cap := range roots {
			capCopy := *cap
			r[root] = &capCopy
		}
	}
	return c
}

func (config *ConfigurationJSON) ToConfiguration() *Configuration {
	result := &Configuration{
		ClusterId:    config.ClusterId,
		Version:      config.Version,
		Hosts:        config.Hosts,
		F:            config.F,
		MaxRMCount:   config.MaxRMCount,
		NoSync:       config.NoSync,
		Fingerprints: make(map[Fingerprint]map[string]*common.Capability, len(config.ClientCertificateFingerprints)),
	}

	allRootNamesMap := make(map[string]types.EmptyStruct)
	for fingerprint, rootsMap := range config.ClientCertificateFingerprints {
		fingerprintBytes, err := hex.DecodeString(fingerprint)
		if err != nil {
			panic(err) // should have already been validated, so just panic here.
		}
		fingerprintAry := [sha256.Size]byte{}
		copy(fingerprintAry[:], fingerprintBytes)

		accountRootsMap := make(map[string]*common.Capability, len(rootsMap))
		result.Fingerprints[fingerprintAry] = accountRootsMap

		for rootName, rootCaps := range rootsMap {
			allRootNamesMap[rootName] = types.EmptyStructVal

			switch {
			case rootCaps.Read && rootCaps.Write:
				accountRootsMap[rootName] = common.ReadWriteCapability
			case rootCaps.Read:
				accountRootsMap[rootName] = common.ReadOnlyCapability
			case rootCaps.Write:
				accountRootsMap[rootName] = common.WriteOnlyCapability
			default:
				accountRootsMap[rootName] = common.NoneCapability
			}
		}
	}
	result.Roots = make([]string, 0, len(allRootNamesMap))
	for rootName := range allRootNamesMap {
		result.Roots = append(result.Roots, rootName)
	}
	sort.Strings(result.Roots)

	return result
}
