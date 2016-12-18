package configuration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	cmsgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/server"
	msgs "goshawkdb.io/server/capnp"
	ch "goshawkdb.io/server/consistenthash"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"time"
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
	if err = config.validate(); err != nil {
		return nil, err
	}
	return config, nil
}

func (config *ConfigurationJSON) validate() error {
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
					return fmt.Errorf("Invalid configured: Account with fingerprint %v, root %v: no capability has been granted.",
						fingerprint, rootName)
				}
			}
		}
	}
	return nil
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
		Roots:        make([]string, 0, len(config.ClientCertificateFingerprints)),
	}

	seg := capn.NewBuffer(nil)
	allRootNamesMap := make(map[string]server.EmptyStruct)
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
			if _, found := allRootNamesMap[rootName]; !found {
				allRootNamesMap[rootName] = server.EmptyStructVal
				result.Roots = append(result.Roots, rootName)
			}

			var capability *common.Capability
			if rootCaps.Read && rootCaps.Write {
				capability = common.MaxCapability
			} else {
				cap := cmsgs.NewCapability(seg)
				switch {
				case rootCaps.Read && rootCaps.Write:
					cap.SetReadWrite()
				case rootCaps.Read:
					cap.SetRead()
				case rootCaps.Write:
					cap.SetWrite()
				default:
					cap.SetNone()
				}
				capability = common.NewCapability(cap)
			}
			accountRootsMap[rootName] = capability
		}
	}
	sort.Strings(result.Roots)

	return result
}

type Fingerprint [sha256.Size]byte

type Configuration struct {
	ClusterId         string
	ClusterUUId       uint64
	Version           uint32
	Hosts             []string
	F                 uint8
	MaxRMCount        uint16
	NoSync            bool
	RMs               common.RMIds
	RMsRemoved        map[common.RMId]server.EmptyStruct
	Fingerprints      map[Fingerprint]map[string]*common.Capability
	Roots             []string
	NextConfiguration *NextConfiguration
}

func BlankConfiguration() *Configuration {
	return &Configuration{
		ClusterId:   "",
		ClusterUUId: 0,
		Version:     0,
		Hosts:       []string{},
		F:           0,
		MaxRMCount:  0,
		NoSync:      false,
		RMs:         []common.RMId{},
	}
}

func (a *Configuration) Equal(b *Configuration) bool {
	if !a.EqualExternally(b) {
		return false
	}
	if a == nil {
		return true
	}
	if !(a.ClusterUUId == b.ClusterUUId && a.RMs.Equal(b.RMs) && len(a.RMsRemoved) == len(b.RMsRemoved) && len(a.Roots) == len(b.Roots)) {
		return false
	}
	for aRM := range a.RMsRemoved {
		if _, found := b.RMsRemoved[aRM]; !found {
			return false
		}
	}
	for idx, aRoot := range a.Roots {
		if aRoot != b.Roots[idx] {
			return false
		}
	}
	return a.NextConfiguration.Equal(b.NextConfiguration)
}

func (a *Configuration) EqualExternally(b *Configuration) bool {
	if a == nil || b == nil {
		return a == b
	}
	if !(a.ClusterId == b.ClusterId && a.Version == b.Version && len(a.Hosts) == len(b.Hosts) && a.F == b.F && a.MaxRMCount == b.MaxRMCount && a.NoSync == b.NoSync && len(a.Fingerprints) == len(b.Fingerprints)) {
		return false
	}
	for idx, aHost := range a.Hosts {
		if aHost != b.Hosts[idx] {
			return false
		}
	}
	for fingerprint, aRoots := range a.Fingerprints {
		if bRoots, found := b.Fingerprints[fingerprint]; !found || len(aRoots) != len(bRoots) {
			return false
		} else {
			for name, aRootCaps := range aRoots {
				if bRootCaps, found := bRoots[name]; !found || !aRootCaps.Equal(bRootCaps) {
					return false
				}
			}
		}
	}
	return true
}

func (config *Configuration) String() string {
	return fmt.Sprintf("Configuration{ClusterId: %v(%v), Version: %v, Hosts: %v, F: %v, MaxRMCount: %v, NoSync: %v, RMs: %v, Removed: %v, RootNames: %v, %v}",
		config.ClusterId, config.ClusterUUId, config.Version, config.Hosts, config.F, config.MaxRMCount, config.NoSync, config.RMs, config.RMsRemoved, config.Roots, config.NextConfiguration)
}

func (config *Configuration) Clone() *Configuration {
	clone := &Configuration{
		ClusterId:         config.ClusterId,
		ClusterUUId:       config.ClusterUUId,
		Version:           config.Version,
		Hosts:             make([]string, len(config.Hosts)),
		F:                 config.F,
		MaxRMCount:        config.MaxRMCount,
		NoSync:            config.NoSync,
		RMs:               make([]common.RMId, len(config.RMs)),
		RMsRemoved:        make(map[common.RMId]server.EmptyStruct, len(config.RMsRemoved)),
		Fingerprints:      make(map[Fingerprint]map[string]*common.Capability, len(config.Fingerprints)),
		Roots:             make([]string, len(config.Roots)),
		NextConfiguration: config.NextConfiguration.Clone(),
	}

	copy(clone.Hosts, config.Hosts)
	copy(clone.RMs, config.RMs)
	for k, v := range config.RMsRemoved {
		clone.RMsRemoved[k] = v
	}
	for k, v := range config.Fingerprints {
		clone.Fingerprints[k] = v
	}
	copy(clone.Roots, config.Roots)
	return clone
}

func (config *Configuration) EnsureClusterUUId(uuid uint64) uint64 {
	if config.ClusterUUId == 0 {
		if uuid == 0 {
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			r := uint64(rng.Int63())
			for r == 0 {
				r = uint64(rng.Int63())
			}
			config.ClusterUUId = r
		} else {
			config.ClusterUUId = uuid
		}
	}
	return config.ClusterUUId
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

func ConfigurationFromCap(config *msgs.Configuration) *Configuration {
	c := &Configuration{
		ClusterId:   config.ClusterId(),
		ClusterUUId: config.ClusterUUId(),
		Version:     config.Version(),
		Hosts:       config.Hosts().ToArray(),
		F:           config.F(),
		MaxRMCount:  config.MaxRMCount(),
		NoSync:      config.NoSync(),
	}

	rms := config.Rms()
	c.RMs = make([]common.RMId, rms.Len())
	for idx := range c.RMs {
		c.RMs[idx] = common.RMId(rms.At(idx))
	}

	rmsRemoved := config.RmsRemoved()
	c.RMsRemoved = make(map[common.RMId]server.EmptyStruct, rmsRemoved.Len())
	for idx, l := 0, rmsRemoved.Len(); idx < l; idx++ {
		c.RMsRemoved[common.RMId(rmsRemoved.At(idx))] = server.EmptyStructVal
	}

	rootsName := []string{}
	rootsMap := make(map[string]server.EmptyStruct)
	fingerprints := config.Fingerprints()
	fingerprintsMap := make(map[Fingerprint]map[string]*common.Capability, fingerprints.Len())
	for idx, l := 0, fingerprints.Len(); idx < l; idx++ {
		fingerprint := fingerprints.At(idx)
		ary := [sha256.Size]byte{}
		copy(ary[:], fingerprint.Sha256())
		rootsCap := fingerprint.Roots()
		roots := make(map[string]*common.Capability, rootsCap.Len())
		for idy, m := 0, rootsCap.Len(); idy < m; idy++ {
			rootCap := rootsCap.At(idy)
			name := rootCap.Name()
			capability := rootCap.Capability()
			roots[name] = common.NewCapability(capability)
			if _, found := rootsMap[name]; !found {
				rootsMap[name] = server.EmptyStructVal
				rootsName = append(rootsName, name)
			}
		}
		fingerprintsMap[ary] = roots
	}
	c.Fingerprints = fingerprintsMap
	sort.Strings(rootsName)
	c.Roots = rootsName

	if config.Which() == msgs.CONFIGURATION_TRANSITIONINGTO {
		next := config.TransitioningTo()
		nextConfig := next.Configuration()

		allHosts := next.AllHosts().ToArray()

		newRMIdsCap := next.NewRMIds()
		newRMIds := make([]common.RMId, newRMIdsCap.Len())
		for idx := range newRMIds {
			newRMIds[idx] = common.RMId(newRMIdsCap.At(idx))
		}

		survivingRMIdsCap := next.SurvivingRMIds()
		survivingRMIds := make([]common.RMId, survivingRMIdsCap.Len())
		for idx := range survivingRMIds {
			survivingRMIds[idx] = common.RMId(survivingRMIdsCap.At(idx))
		}

		lostRMIdsCap := next.LostRMIds()
		lostRMIds := make([]common.RMId, lostRMIdsCap.Len())
		for idx := range lostRMIds {
			lostRMIds[idx] = common.RMId(lostRMIdsCap.At(idx))
		}

		rootIndices := next.RootIndices().ToArray()

		installedOnNew := next.InstalledOnNew()

		barrierReached1Cap := next.BarrierReached1()
		barrierReached1 := make([]common.RMId, barrierReached1Cap.Len())
		for idx := range barrierReached1 {
			barrierReached1[idx] = common.RMId(barrierReached1Cap.At(idx))
		}

		barrierReached2Cap := next.BarrierReached2()
		barrierReached2 := make([]common.RMId, barrierReached2Cap.Len())
		for idx := range barrierReached2 {
			barrierReached2[idx] = common.RMId(barrierReached2Cap.At(idx))
		}

		pending := next.Pending()

		c.NextConfiguration = &NextConfiguration{
			Configuration:   ConfigurationFromCap(&nextConfig),
			AllHosts:        allHosts,
			NewRMIds:        newRMIds,
			SurvivingRMIds:  survivingRMIds,
			LostRMIds:       lostRMIds,
			RootIndices:     rootIndices,
			InstalledOnNew:  installedOnNew,
			BarrierReached1: barrierReached1,
			BarrierReached2: barrierReached2,
			Pending:         ConditionsFromCap(&pending),
		}
	}

	return c
}

func (config *Configuration) Serialize() []byte {
	seg := capn.NewBuffer(nil)
	config.AddToSegAutoRoot(seg)
	return server.SegToBytes(seg)
}

func (config *Configuration) AddToSegAutoRoot(seg *capn.Segment) msgs.Configuration {
	cap := msgs.AutoNewConfiguration(seg)
	cap.SetClusterId(config.ClusterId)
	cap.SetClusterUUId(config.ClusterUUId)
	cap.SetVersion(config.Version)

	hosts := seg.NewTextList(len(config.Hosts))
	cap.SetHosts(hosts)
	for idx, host := range config.Hosts {
		hosts.Set(idx, host)
	}

	cap.SetF(config.F)
	cap.SetMaxRMCount(config.MaxRMCount)
	cap.SetNoSync(config.NoSync)

	rms := seg.NewUInt32List(len(config.RMs))
	cap.SetRms(rms)
	for idx, rmId := range config.RMs {
		rms.Set(idx, uint32(rmId))
	}

	rmsRemoved := seg.NewUInt32List(len(config.RMsRemoved))
	cap.SetRmsRemoved(rmsRemoved)
	idx := 0
	for rmId := range config.RMsRemoved { // don't care about order
		rmsRemoved.Set(idx, uint32(rmId))
		idx++
	}

	fingerprintsCap := msgs.NewFingerprintList(seg, len(config.Fingerprints))
	idx = 0
	for fingerprint, roots := range config.Fingerprints {
		fingerprintCap := msgs.NewFingerprint(seg)
		fingerprintCap.SetSha256(fingerprint[:])
		rootsCap := msgs.NewRootList(seg, len(roots))
		idy := 0
		for name, capability := range roots {
			rootCap := msgs.NewRoot(seg)
			rootCap.SetName(name)
			rootCap.SetCapability(capability.Capability)
			rootsCap.Set(idy, rootCap)
			idy++
		}
		fingerprintCap.SetRoots(rootsCap)
		fingerprintsCap.Set(idx, fingerprintCap)
		idx++
	}
	cap.SetFingerprints(fingerprintsCap)

	if config.NextConfiguration == nil {
		cap.SetStable()
	} else {
		nextConfig := config.NextConfiguration
		cap.SetTransitioningTo()
		nextCap := cap.TransitioningTo()
		nextCap.SetConfiguration(nextConfig.Configuration.AddToSegAutoRoot(seg))

		allHostsCap := seg.NewTextList(len(nextConfig.AllHosts))
		for idx, host := range nextConfig.AllHosts {
			allHostsCap.Set(idx, host)
		}
		nextCap.SetAllHosts(allHostsCap)

		newRMIdsCap := seg.NewUInt32List(len(nextConfig.NewRMIds))
		for idx, rmId := range nextConfig.NewRMIds {
			newRMIdsCap.Set(idx, uint32(rmId))
		}
		nextCap.SetNewRMIds(newRMIdsCap)

		survivingRMIdsCap := seg.NewUInt32List(len(nextConfig.SurvivingRMIds))
		for idx, rmId := range nextConfig.SurvivingRMIds {
			survivingRMIdsCap.Set(idx, uint32(rmId))
		}
		nextCap.SetSurvivingRMIds(survivingRMIdsCap)

		lostRMIdsCap := seg.NewUInt32List(len(nextConfig.LostRMIds))
		for idx, rmId := range nextConfig.LostRMIds {
			lostRMIdsCap.Set(idx, uint32(rmId))
		}
		nextCap.SetLostRMIds(lostRMIdsCap)

		rootIndicesCap := seg.NewUInt32List(len(nextConfig.RootIndices))
		for idx, index := range nextConfig.RootIndices {
			rootIndicesCap.Set(idx, index)
		}
		nextCap.SetRootIndices(rootIndicesCap)

		nextCap.SetInstalledOnNew(nextConfig.InstalledOnNew)

		barrierReached1Cap := seg.NewUInt32List(len(nextConfig.BarrierReached1))
		for idx, rmId := range nextConfig.BarrierReached1 {
			barrierReached1Cap.Set(idx, uint32(rmId))
		}
		nextCap.SetBarrierReached1(barrierReached1Cap)

		barrierReached2Cap := seg.NewUInt32List(len(nextConfig.BarrierReached2))
		for idx, rmId := range nextConfig.BarrierReached2 {
			barrierReached2Cap.Set(idx, uint32(rmId))
		}
		nextCap.SetBarrierReached2(barrierReached2Cap)

		nextCap.SetPending(nextConfig.Pending.AddToSeg(seg))
	}
	return cap
}

// next

type NextConfiguration struct {
	*Configuration
	AllHosts        []string
	NewRMIds        common.RMIds
	SurvivingRMIds  common.RMIds
	LostRMIds       common.RMIds
	RootIndices     []uint32
	InstalledOnNew  bool
	BarrierReached1 common.RMIds
	BarrierReached2 common.RMIds
	Pending         Conds
}

func (next *NextConfiguration) String() string {
	return fmt.Sprintf("Next Configuration:\n AllHosts: %v;\n NewRMIds: %v;\n SurvivingRMIds: %v;\n LostRMIds: %v;\n RootIndices: %v;\n InstalledOnNew: %v;\n BarrierReached1: %v;\n BarrierReached2: %v;\n Pending:%v;\n Configuration: %v",
		next.AllHosts, next.NewRMIds, next.SurvivingRMIds, next.LostRMIds, next.RootIndices, next.InstalledOnNew, next.BarrierReached1, next.BarrierReached2, next.Pending, next.Configuration)
}

func (a *NextConfiguration) Equal(b *NextConfiguration) bool {
	if a == nil || b == nil {
		return a == b
	}
	if len(a.AllHosts) == len(b.AllHosts) {
		for idx, aHost := range a.AllHosts {
			if aHost != b.AllHosts[idx] {
				return false
			}
		}
	} else {
		return false
	}
	if len(a.RootIndices) == len(b.RootIndices) {
		for idx, aIndex := range a.RootIndices {
			if aIndex != b.RootIndices[idx] {
				return false
			}
		}
	} else {
		return false
	}
	return a.NewRMIds.Equal(b.NewRMIds) &&
		a.SurvivingRMIds.Equal(b.SurvivingRMIds) &&
		a.LostRMIds.Equal(b.LostRMIds) &&
		a.InstalledOnNew == b.InstalledOnNew &&
		a.BarrierReached1.Equal(b.BarrierReached1) &&
		a.BarrierReached2.Equal(b.BarrierReached2) &&
		a.Pending.Equal(b.Pending) &&
		a.Configuration.Equal(b.Configuration)
}

func (next *NextConfiguration) Clone() *NextConfiguration {
	if next == nil {
		return nil
	}

	allHosts := make([]string, len(next.AllHosts))
	copy(allHosts, next.AllHosts)

	newRMIds := make([]common.RMId, len(next.NewRMIds))
	copy(newRMIds, next.NewRMIds)

	survivingRMIds := make([]common.RMId, len(next.SurvivingRMIds))
	copy(survivingRMIds, next.SurvivingRMIds)

	lostRMIds := make([]common.RMId, len(next.LostRMIds))
	copy(lostRMIds, next.LostRMIds)

	rootIndices := make([]uint32, len(next.RootIndices))
	copy(rootIndices, next.RootIndices)

	barrierReached1 := make([]common.RMId, len(next.BarrierReached1))
	copy(barrierReached1, next.BarrierReached1)

	barrierReached2 := make([]common.RMId, len(next.BarrierReached2))
	copy(barrierReached2, next.BarrierReached2)

	// Assumption is that conditions are immutable. So the only thing
	// we'll want to do is shrink the suppliers, so we do a copy of the
	// suppliers, not a deep copy of the conditions.
	pending := make(map[common.RMId]*CondSuppliers, len(next.Pending))
	for k, v := range next.Pending {
		cs := &CondSuppliers{
			Cond:      v.Cond,
			Suppliers: make([]common.RMId, len(v.Suppliers)),
		}
		copy(cs.Suppliers, v.Suppliers)
		pending[k] = cs
	}

	return &NextConfiguration{
		Configuration:   next.Configuration.Clone(),
		AllHosts:        allHosts,
		NewRMIds:        newRMIds,
		SurvivingRMIds:  survivingRMIds,
		LostRMIds:       lostRMIds,
		RootIndices:     rootIndices,
		InstalledOnNew:  next.InstalledOnNew,
		BarrierReached1: barrierReached1,
		BarrierReached2: barrierReached2,
		Pending:         pending,
	}
}

func (next *NextConfiguration) BarrierReached1For(rmId common.RMId) bool {
	if next != nil {
		for _, r := range next.BarrierReached1 {
			if r == rmId {
				return true
			}
		}
	}
	return false
}

func (next *NextConfiguration) BarrierReached2For(rmId common.RMId) bool {
	if next != nil {
		for _, r := range next.BarrierReached2 {
			if r == rmId {
				return true
			}
		}
	}
	return false
}

type Conds map[common.RMId]*CondSuppliers

func ConditionsFromCap(condsCap *msgs.ConditionPair_List) Conds {
	condSups := make(map[common.RMId]*CondSuppliers, condsCap.Len())
	for idx, l := 0, condsCap.Len(); idx < l; idx++ {
		pairCap := condsCap.At(idx)
		condCap := pairCap.Condition()
		suppliersCap := pairCap.Suppliers()
		suppliers := make([]common.RMId, suppliersCap.Len())
		for idx := range suppliers {
			suppliers[idx] = common.RMId(suppliersCap.At(idx))
		}
		condSups[common.RMId(pairCap.RmId())] = &CondSuppliers{
			Cond:      conditionFromCap(&condCap),
			Suppliers: suppliers,
		}
	}
	return condSups
}

func (cs Conds) DisjoinWith(rmId common.RMId, c Cond) {
	if condSup, found := cs[rmId]; found {
		condSup.Cond = &Disjunction{
			Left:  condSup.Cond,
			Right: c,
		}
	} else {
		cs[rmId] = &CondSuppliers{Cond: c}
	}
}

func (cs Conds) SuppliedBy(requester, supplier common.RMId, maxSuppliers int) bool {
	if condSup, found := cs[requester]; found {
		for _, s := range condSup.Suppliers {
			if s == supplier {
				return false
			}
		}
		condSup.Suppliers = append(condSup.Suppliers, supplier)
		if len(condSup.Suppliers) == maxSuppliers {
			delete(cs, requester)
		}
		return true
	}
	return false
}

func (a Conds) Equal(b Conds) bool {
	if len(a) != len(b) {
		return false
	}
	for rmId, aCond := range a {
		if bCond, found := b[rmId]; !found || !aCond.Equal(bCond) {
			return false
		}
	}
	return true
}

func (c Conds) String() string {
	str := ""
	for k, v := range c {
		str += fmt.Sprintf("\n  %v requests all objects satisfying %v", k, v)
	}
	return str
}

func (c Conds) AddToSeg(seg *capn.Segment) msgs.ConditionPair_List {
	cap := msgs.NewConditionPairList(seg, len(c))
	idx := 0
	for rmId, condSup := range c {
		condSupCap := msgs.NewConditionPair(seg)
		condSupCap.SetRmId(uint32(rmId))
		condSupCap.SetCondition(condSup.Cond.AddToSeg(seg))
		suppliersCap := seg.NewUInt32List(len(condSup.Suppliers))
		for idx, rmId := range condSup.Suppliers {
			suppliersCap.Set(idx, uint32(rmId))
		}
		condSupCap.SetSuppliers(suppliersCap)
		cap.Set(idx, condSupCap)
		idx++
	}
	return cap
}

type CondSuppliers struct {
	Cond      Cond
	Suppliers common.RMIds
}

func (a *CondSuppliers) Equal(b *CondSuppliers) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Suppliers.Equal(b.Suppliers) && a.Cond.Equal(b.Cond)
}

func (cs *CondSuppliers) String() string {
	return fmt.Sprintf("%v (supplied by: %v)", cs.Cond, cs.Suppliers)
}

type Cond interface {
	Equal(Cond) bool
	AddToSeg(seg *capn.Segment) msgs.Condition
	SatisfiedBy(config *Configuration, positions *common.Positions) (bool, error)
}

func conditionFromCap(condCap *msgs.Condition) Cond {
	switch condCap.Which() {
	case msgs.CONDITION_AND:
		condAnd := condCap.And()
		left := condAnd.Left()
		right := condAnd.Right()
		return &Conjunction{
			Left:  conditionFromCap(&left),
			Right: conditionFromCap(&right),
		}
	case msgs.CONDITION_OR:
		condOr := condCap.Or()
		left := condOr.Left()
		right := condOr.Right()
		return &Disjunction{
			Left:  conditionFromCap(&left),
			Right: conditionFromCap(&right),
		}
	case msgs.CONDITION_GENERATOR:
		condGen := condCap.Generator()
		return &Generator{
			RMId:     common.RMId(condGen.RmId()),
			UseNext:  condGen.UseNext(),
			Includes: condGen.Includes(),
		}
	default:
		panic(fmt.Sprintf("Unexpected Condition type (%v)", condCap.Which()))
	}
}

type Conjunction struct {
	Left  Cond
	Right Cond
}

func (c *Conjunction) String() string { return fmt.Sprintf("(%v ∧ %v)", c.Left, c.Right) }

func (c *Conjunction) SatisfiedBy(config *Configuration, positions *common.Positions) (bool, error) {
	if b, err := c.Left.SatisfiedBy(config, positions); err != nil || !b {
		return b, err
	}
	return c.Right.SatisfiedBy(config, positions)
}

func (a *Conjunction) Equal(b Cond) bool {
	bConj, ok := b.(*Conjunction)
	if !ok {
		return false
	}
	if a == nil || b == nil || bConj == nil {
		return a == b || a == bConj
	}
	return a.Left.Equal(bConj.Left) && a.Right.Equal(bConj.Right)
}

func (c *Conjunction) AddToSeg(seg *capn.Segment) msgs.Condition {
	conjCap := msgs.NewConjunction(seg)
	conjCap.SetLeft(c.Left.AddToSeg(seg))
	conjCap.SetRight(c.Right.AddToSeg(seg))
	condCap := msgs.NewCondition(seg)
	condCap.SetAnd(conjCap)
	return condCap
}

type Disjunction struct {
	Left  Cond
	Right Cond
}

func (d *Disjunction) String() string { return fmt.Sprintf("(%v ∨ %v)", d.Left, d.Right) }

func (d *Disjunction) SatisfiedBy(config *Configuration, positions *common.Positions) (bool, error) {
	if b, err := d.Left.SatisfiedBy(config, positions); err != nil || b {
		return b, err
	}
	return d.Right.SatisfiedBy(config, positions)
}

func (a *Disjunction) Equal(b Cond) bool {
	bDisj, ok := b.(*Disjunction)
	if !ok {
		return false
	}
	if a == nil || b == nil || bDisj == nil {
		return a == b || a == bDisj
	}
	return a.Left.Equal(bDisj.Left) && a.Right.Equal(bDisj.Right)
}

func (d *Disjunction) AddToSeg(seg *capn.Segment) msgs.Condition {
	disjCap := msgs.NewDisjunction(seg)
	disjCap.SetLeft(d.Left.AddToSeg(seg))
	disjCap.SetRight(d.Right.AddToSeg(seg))
	condCap := msgs.NewCondition(seg)
	condCap.SetOr(disjCap)
	return condCap
}

type Generator struct {
	RMId     common.RMId
	UseNext  bool
	Includes bool
}

func (g *Generator) String() string {
	op := "∈"
	if !g.Includes {
		op = "∉"
	}
	which := "Old"
	if g.UseNext {
		which = "New"
	}
	return fmt.Sprintf("%v %v (p,RMs%s)[:2F%s+1]", g.RMId, op, which, which)
}

func (g *Generator) SatisfiedBy(config *Configuration, positions *common.Positions) (bool, error) {
	rms := config.RMs
	f := config.F
	if g.UseNext {
		next := config.NextConfiguration
		rms = next.RMs
		f = next.F
	}
	twoFInc := (uint16(f) * 2) + 1
	server.Log("Generator:SatisfiedBy:NewResolver:", rms, twoFInc)
	resolver := ch.NewResolver(rms, twoFInc)
	perm, err := resolver.ResolveHashCodes((*capn.UInt8List)(positions).ToArray())
	if err != nil {
		return false, err
	}
	for _, rmId := range perm {
		if rmId == g.RMId {
			return g.Includes, nil
		}
	}
	return !g.Includes, nil

}

func (a *Generator) Equal(b Cond) bool {
	bGen, ok := b.(*Generator)
	if !ok {
		return false
	}
	if a == nil || b == nil || bGen == nil {
		return a == b || a == bGen
	}
	return a.RMId == bGen.RMId && a.UseNext == bGen.UseNext && a.Includes == bGen.Includes
}

func (g *Generator) AddToSeg(seg *capn.Segment) msgs.Condition {
	genCap := msgs.NewGenerator(seg)
	genCap.SetRmId(uint32(g.RMId))
	genCap.SetUseNext(g.UseNext)
	genCap.SetIncludes(g.Includes)
	condCap := msgs.NewCondition(seg)
	condCap.SetGenerator(genCap)
	return condCap
}
