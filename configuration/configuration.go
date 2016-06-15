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

type Configuration struct {
	ClusterId                     string
	Version                       uint32
	Hosts                         []string
	F                             uint8
	MaxRMCount                    uint16
	NoSync                        bool
	ClientCertificateFingerprints map[string]map[string]*RootCapabilities
	clusterUUId                   uint64
	roots                         []string
	rms                           common.RMIds
	rmsRemoved                    map[common.RMId]server.EmptyStruct
	fingerprints                  map[[sha256.Size]byte]map[string]*cmsgs.Capabilities
	nextConfiguration             *NextConfiguration
}

type RootCapabilities struct {
	ValueRead           bool
	ValueWrite          bool
	ReferencesReadAll   bool
	ReferencesReadOnly  []uint32
	ReferencesWriteAll  bool
	ReferencesWriteOnly []uint32
}

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
	aAllHosts, bAllHosts := a.AllHosts, b.AllHosts
	if len(aAllHosts) != len(bAllHosts) {
		return false
	}
	for idx, aHost := range aAllHosts {
		if aHost != bAllHosts[idx] {
			return false
		}
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
	// we'll want to do is shrink the map, so we do a copy of the map,
	// not a deep copy of the conditions.
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
	if int(config.MaxRMCount) < len(config.Hosts) {
		return nil, fmt.Errorf("MaxRMCount given as %v but must be at least the number of hosts (%v).", config.MaxRMCount, len(config.Hosts))
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
		rootsMap := make(map[string]server.EmptyStruct)
		rootsName := []string{}
		fingerprints := make(map[[sha256.Size]byte]map[string]*cmsgs.Capabilities, len(config.ClientCertificateFingerprints))
		seg := capn.NewBuffer(nil)
		for fingerprint, rootsCapabilities := range config.ClientCertificateFingerprints {
			fingerprintBytes, err := hex.DecodeString(fingerprint)
			if err != nil {
				return nil, err
			} else if l := len(fingerprintBytes); l != sha256.Size {
				return nil, fmt.Errorf("Invalid fingerprint: expected %v bytes, and found %v", sha256.Size, l)
			}
			if len(rootsCapabilities) == 0 {
				return nil, fmt.Errorf("No roots configured for client fingerprint %v; at least 1 needed", fingerprint)
			}
			roots := make(map[string]*cmsgs.Capabilities, len(rootsCapabilities))
			for name, rootCapabilities := range rootsCapabilities {
				if _, found := rootsMap[name]; !found {
					rootsMap[name] = server.EmptyStructVal
					rootsName = append(rootsName, name)
				}
				SortUInt32(rootCapabilities.ReferencesReadOnly).Sort()
				SortUInt32(rootCapabilities.ReferencesWriteOnly).Sort()
				if rootCapabilities.ReferencesReadAll && len(rootCapabilities.ReferencesReadOnly) != 0 {
					return nil, fmt.Errorf("ReferencesReadAll and ReferencesReadOnly must be mutually exclusive for client fingerprint %v, root %s", fingerprint, name)
				}
				if rootCapabilities.ReferencesWriteAll && len(rootCapabilities.ReferencesWriteOnly) != 0 {
					return nil, fmt.Errorf("ReferencesWriteAll and ReferencesWriteOnly must be mutually exclusive for client fingerprint %v, root %s", fingerprint, name)
				}
				old := uint32(0)
				for idx, index := range rootCapabilities.ReferencesReadOnly {
					if index == old && idx > 0 {
						return nil, fmt.Errorf("Client fingerprint %v, root %s: Duplicate read only reference index %v",
							fingerprint, name, index)
					}
					old = index
				}
				old = uint32(0)
				for idx, index := range rootCapabilities.ReferencesWriteOnly {
					if index == old && idx > 0 {
						return nil, fmt.Errorf("Client fingerprint %v, root %s: Duplicate write only reference index %v",
							fingerprint, name, index)
					}
					old = index
				}
				if !rootCapabilities.ValueRead && !rootCapabilities.ValueWrite &&
					!rootCapabilities.ReferencesReadAll && !rootCapabilities.ReferencesWriteAll &&
					len(rootCapabilities.ReferencesReadOnly) == 0 && len(rootCapabilities.ReferencesWriteOnly) == 0 {
					return nil, fmt.Errorf("Client fingerprint %v, root %s: no capabilities have been granted.",
						fingerprint, name)
				}
				cap := cmsgs.NewCapabilities(seg)
				switch {
				case rootCapabilities.ValueRead && rootCapabilities.ValueWrite:
					cap.SetValue(cmsgs.VALUECAPABILITY_READWRITE)
				case rootCapabilities.ValueRead:
					cap.SetValue(cmsgs.VALUECAPABILITY_READ)
				case rootCapabilities.ValueWrite:
					cap.SetValue(cmsgs.VALUECAPABILITY_WRITE)
				default:
					cap.SetValue(cmsgs.VALUECAPABILITY_NONE)
				}
				capRefs := cap.References()
				if rootCapabilities.ReferencesReadAll {
					capRefs.Read().SetAll()
				} else {
					only := seg.NewUInt32List(len(rootCapabilities.ReferencesReadOnly))
					for idx, index := range rootCapabilities.ReferencesReadOnly {
						only.Set(idx, index)
					}
					capRefs.Read().SetOnly(only)
				}
				if rootCapabilities.ReferencesWriteAll {
					capRefs.Write().SetAll()
				} else {
					only := seg.NewUInt32List(len(rootCapabilities.ReferencesWriteOnly))
					for idx, index := range rootCapabilities.ReferencesWriteOnly {
						only.Set(idx, index)
					}
					capRefs.Write().SetOnly(only)
				}
				roots[name] = &cap
			}
			ary := [sha256.Size]byte{}
			copy(ary[:], fingerprintBytes)
			fingerprints[ary] = roots
		}
		config.fingerprints = fingerprints
		config.ClientCertificateFingerprints = nil
		sort.Strings(rootsName)
		config.roots = rootsName
	}
	return &config, err
}

func ConfigurationFromCap(config *msgs.Configuration) *Configuration {
	c := &Configuration{
		ClusterId:   config.ClusterId(),
		clusterUUId: config.ClusterUUId(),
		Version:     config.Version(),
		Hosts:       config.Hosts().ToArray(),
		F:           config.F(),
		MaxRMCount:  config.MaxRMCount(),
		NoSync:      config.NoSync(),
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

	rootsName := []string{}
	rootsMap := make(map[string]server.EmptyStruct)
	fingerprints := config.Fingerprints()
	fingerprintsMap := make(map[[sha256.Size]byte]map[string]*cmsgs.Capabilities, fingerprints.Len())
	for idx, l := 0, fingerprints.Len(); idx < l; idx++ {
		fingerprint := fingerprints.At(idx)
		ary := [sha256.Size]byte{}
		copy(ary[:], fingerprint.Sha256())
		rootsCap := fingerprint.Roots()
		roots := make(map[string]*cmsgs.Capabilities, rootsCap.Len())
		for idy, m := 0, rootsCap.Len(); idy < m; idy++ {
			rootCap := rootsCap.At(idy)
			name := rootCap.Name()
			capabilities := rootCap.Capabilities()
			roots[name] = &capabilities
			if _, found := rootsMap[name]; !found {
				rootsName = append(rootsName, name)
				rootsMap[name] = server.EmptyStructVal
			}
		}
		fingerprintsMap[ary] = roots
	}
	c.fingerprints = fingerprintsMap
	sort.Strings(rootsName)
	c.roots = rootsName

	if config.Which() == msgs.CONFIGURATION_TRANSITIONINGTO {
		next := config.TransitioningTo()
		nextConfig := next.Configuration()

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

		c.nextConfiguration = &NextConfiguration{
			Configuration:   ConfigurationFromCap(&nextConfig),
			AllHosts:        next.AllHosts().ToArray(),
			NewRMIds:        newRMIds,
			SurvivingRMIds:  survivingRMIds,
			LostRMIds:       lostRMIds,
			RootIndices:     rootIndices,
			InstalledOnNew:  next.InstalledOnNew(),
			BarrierReached1: barrierReached1,
			BarrierReached2: barrierReached2,
			Pending:         ConditionsFromCap(&pending),
		}
	}

	return c
}

func (a *Configuration) Equal(b *Configuration) bool {
	if a == nil || b == nil {
		return a == b
	}
	if !(a.ClusterId == b.ClusterId && a.clusterUUId == b.clusterUUId && a.Version == b.Version && a.F == b.F && a.MaxRMCount == b.MaxRMCount && a.NoSync == b.NoSync && len(a.Hosts) == len(b.Hosts) && len(a.fingerprints) == len(b.fingerprints) && len(a.rms) == len(b.rms) && len(a.rmsRemoved) == len(b.rmsRemoved)) {
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
	for fingerprint, aRoots := range a.fingerprints {
		if bRoots, found := b.fingerprints[fingerprint]; !found || len(aRoots) != len(bRoots) {
			return false
		} else {
			for name, aRootCaps := range aRoots {
				if bRootCaps, found := bRoots[name]; !found || !common.EqualCapabilities(aRootCaps, bRootCaps) {
					return false
				}
			}
		}
	}
	return a.nextConfiguration.Equal(b.nextConfiguration)
}

func (config *Configuration) String() string {
	return fmt.Sprintf("Configuration{ClusterId: %v(%v), Version: %v, Hosts: %v, F: %v, MaxRMCount: %v, NoSync: %v, RMs: %v, Removed: %v, RootNames: %v, %v}",
		config.ClusterId, config.clusterUUId, config.Version, config.Hosts, config.F, config.MaxRMCount, config.NoSync, config.rms, config.rmsRemoved, config.roots, config.nextConfiguration)
}

func (config *Configuration) ClusterUUId() uint64 {
	return config.clusterUUId
}

func (config *Configuration) SetClusterUUId(uuid uint64) {
	if config.clusterUUId == 0 {
		if uuid == 0 {
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			r := uint64(rng.Int63())
			for r == 0 {
				r = uint64(rng.Int63())
			}
			config.clusterUUId = r
		} else {
			config.clusterUUId = uuid
		}
	}
}

func (config *Configuration) Fingerprints() map[[sha256.Size]byte]map[string]*cmsgs.Capabilities {
	return config.fingerprints
}

func (config *Configuration) RootNames() []string {
	return config.roots
}

func (config *Configuration) NextBarrierReached1(rmId common.RMId) bool {
	if config.nextConfiguration != nil {
		for _, r := range config.nextConfiguration.BarrierReached1 {
			if r == rmId {
				return true
			}
		}
	}
	return false
}

func (config *Configuration) NextBarrierReached2(rmId common.RMId) bool {
	if config.nextConfiguration != nil {
		for _, r := range config.nextConfiguration.BarrierReached2 {
			if r == rmId {
				return true
			}
		}
	}
	return false
}

func (config *Configuration) Next() *NextConfiguration {
	return config.nextConfiguration
}

func (config *Configuration) SetNext(next *NextConfiguration) {
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

func (config *Configuration) SetRMsRemoved(removed map[common.RMId]server.EmptyStruct) {
	config.rmsRemoved = removed
}

func (config *Configuration) Clone() *Configuration {
	clone := &Configuration{
		ClusterId:   config.ClusterId,
		clusterUUId: config.clusterUUId,
		Version:     config.Version,
		Hosts:       make([]string, len(config.Hosts)),
		F:           config.F,
		MaxRMCount:  config.MaxRMCount,
		NoSync:      config.NoSync,
		ClientCertificateFingerprints: nil,
		roots:             make([]string, len(config.roots)),
		rms:               make([]common.RMId, len(config.rms)),
		rmsRemoved:        make(map[common.RMId]server.EmptyStruct, len(config.rmsRemoved)),
		fingerprints:      make(map[[sha256.Size]byte]map[string]*cmsgs.Capabilities, len(config.fingerprints)),
		nextConfiguration: config.nextConfiguration.Clone(),
	}

	copy(clone.Hosts, config.Hosts)
	if config.ClientCertificateFingerprints != nil {
		clone.ClientCertificateFingerprints = make(map[string]map[string]*RootCapabilities, len(config.ClientCertificateFingerprints))
		for k, v := range config.ClientCertificateFingerprints {
			clone.ClientCertificateFingerprints[k] = v
		}
	}
	copy(clone.roots, config.roots)
	copy(clone.rms, config.rms)
	for k, v := range config.rmsRemoved {
		clone.rmsRemoved[k] = v
	}
	for k, v := range config.fingerprints {
		clone.fingerprints[k] = v
	}
	return clone
}

func (config *Configuration) AddToSegAutoRoot(seg *capn.Segment) msgs.Configuration {
	cap := msgs.AutoNewConfiguration(seg)
	cap.SetClusterId(config.ClusterId)
	cap.SetClusterUUId(config.clusterUUId)
	cap.SetVersion(config.Version)

	hosts := seg.NewTextList(len(config.Hosts))
	cap.SetHosts(hosts)
	for idx, host := range config.Hosts {
		hosts.Set(idx, host)
	}

	cap.SetF(config.F)
	cap.SetMaxRMCount(config.MaxRMCount)
	cap.SetNoSync(config.NoSync)

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
	fingerprintsCap := msgs.NewFingerprintList(seg, len(fingerprintsMap))
	idx = 0
	for fingerprint, roots := range fingerprintsMap {
		fingerprintCap := msgs.NewFingerprint(seg)
		fingerprintCap.SetSha256(fingerprint[:])
		rootsCap := msgs.NewRootList(seg, len(roots))
		idy := 0
		for name, capabilities := range roots {
			rootCap := msgs.NewRoot(seg)
			rootCap.SetName(name)
			rootCap.SetCapabilities(*capabilities)
			rootsCap.Set(idy, rootCap)
			idy++
		}
		fingerprintCap.SetRoots(rootsCap)
		fingerprintsCap.Set(idx, fingerprintCap)
		idx++
	}
	cap.SetFingerprints(fingerprintsCap)

	if config.nextConfiguration == nil {
		cap.SetStable()
	} else {
		nextConfig := config.nextConfiguration
		cap.SetTransitioningTo()
		next := cap.TransitioningTo()
		next.SetConfiguration(nextConfig.Configuration.AddToSegAutoRoot(seg))

		allHostsCap := seg.NewTextList(len(nextConfig.AllHosts))
		for idx, host := range nextConfig.AllHosts {
			allHostsCap.Set(idx, host)
		}
		next.SetAllHosts(allHostsCap)

		newRMIdsCap := seg.NewUInt32List(len(nextConfig.NewRMIds))
		for idx, rmId := range nextConfig.NewRMIds {
			newRMIdsCap.Set(idx, uint32(rmId))
		}
		next.SetNewRMIds(newRMIdsCap)

		survivingRMIdsCap := seg.NewUInt32List(len(nextConfig.SurvivingRMIds))
		for idx, rmId := range nextConfig.SurvivingRMIds {
			survivingRMIdsCap.Set(idx, uint32(rmId))
		}
		next.SetSurvivingRMIds(survivingRMIdsCap)

		lostRMIdsCap := seg.NewUInt32List(len(nextConfig.LostRMIds))
		for idx, rmId := range nextConfig.LostRMIds {
			lostRMIdsCap.Set(idx, uint32(rmId))
		}
		next.SetLostRMIds(lostRMIdsCap)

		rootIndicesCap := seg.NewUInt32List(len(nextConfig.RootIndices))
		for idx, index := range nextConfig.RootIndices {
			rootIndicesCap.Set(idx, index)
		}
		next.SetRootIndices(rootIndicesCap)

		barrierReached1Cap := seg.NewUInt32List(len(nextConfig.BarrierReached1))
		for idx, rmId := range nextConfig.BarrierReached1 {
			barrierReached1Cap.Set(idx, uint32(rmId))
		}
		next.SetBarrierReached1(barrierReached1Cap)

		barrierReached2Cap := seg.NewUInt32List(len(nextConfig.BarrierReached2))
		for idx, rmId := range nextConfig.BarrierReached2 {
			barrierReached2Cap.Set(idx, uint32(rmId))
		}
		next.SetBarrierReached2(barrierReached2Cap)

		next.SetInstalledOnNew(nextConfig.InstalledOnNew)

		next.SetPending(nextConfig.Pending.AddToSeg(seg))
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
	witness() Cond
	SatisfiedBy(topology *Topology, positions *common.Positions) (bool, error)
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

func (c *Conjunction) witness() Cond  { return c }
func (c *Conjunction) String() string { return fmt.Sprintf("(%v ∧ %v)", c.Left, c.Right) }

func (c *Conjunction) SatisfiedBy(topology *Topology, positions *common.Positions) (bool, error) {
	if b, err := c.Left.SatisfiedBy(topology, positions); err != nil || !b {
		return b, err
	}
	return c.Right.SatisfiedBy(topology, positions)
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

func (d *Disjunction) witness() Cond  { return d }
func (d *Disjunction) String() string { return fmt.Sprintf("(%v ∨ %v)", d.Left, d.Right) }

func (d *Disjunction) SatisfiedBy(topology *Topology, positions *common.Positions) (bool, error) {
	if b, err := d.Left.SatisfiedBy(topology, positions); err != nil || b {
		return b, err
	}
	return d.Right.SatisfiedBy(topology, positions)
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

func (g *Generator) witness() Cond { return g }
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

func (g *Generator) SatisfiedBy(topology *Topology, positions *common.Positions) (bool, error) {
	rms := topology.RMs()
	twoFInc := topology.TwoFInc
	if g.UseNext {
		next := topology.Next()
		rms = next.RMs()
		twoFInc = (uint16(next.F) * 2) + 1
	}
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

type SortUInt32 []uint32

func (nums SortUInt32) Sort()              { sort.Sort(nums) }
func (nums SortUInt32) Len() int           { return len(nums) }
func (nums SortUInt32) Less(i, j int) bool { return nums[i] < nums[j] }
func (nums SortUInt32) Swap(i, j int)      { nums[i], nums[j] = nums[j], nums[i] }
