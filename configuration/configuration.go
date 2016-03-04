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
	nextConfiguration             *NextConfiguration
}

type NextConfiguration struct {
	*Configuration
	InstalledOnAll bool
	Pending        Conds
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
		next := config.TransitioningTo()
		nextConfig := next.Configuration()
		pending := next.Pending()
		c.nextConfiguration = &NextConfiguration{
			Configuration:  ConfigurationFromCap(&nextConfig),
			InstalledOnAll: next.InstalledOnAll(),
			Pending:        ConditionsFromCap(&pending),
		}
	}

	return c
}

func (a *Configuration) Equal(b *Configuration) bool {
	if a == nil || b == nil {
		return a == b
	}
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
	return a.nextConfiguration.InstalledOnAll == b.nextConfiguration.InstalledOnAll &&
		a.nextConfiguration.Pending.Equal(b.nextConfiguration.Pending) &&
		a.nextConfiguration.Configuration.Equal(b.nextConfiguration.Configuration)
}

func (config *Configuration) String() string {
	return fmt.Sprintf("Configuration{ClusterId: %v, Version: %v, Hosts: %v, F: %v, MaxRMCount: %v, AsyncFlush: %v, RMs: %v}",
		config.ClusterId, config.Version, config.Hosts, config.F, config.MaxRMCount, config.AsyncFlush, config.rms)
}

func (config *Configuration) Fingerprints() map[[sha256.Size]byte]server.EmptyStruct {
	return config.fingerprints
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
		ClusterId:                     config.ClusterId,
		Version:                       config.Version,
		Hosts:                         make([]string, len(config.Hosts)),
		F:                             config.F,
		MaxRMCount:                    config.MaxRMCount,
		AsyncFlush:                    config.AsyncFlush,
		ClientCertificateFingerprints: make([]string, len(config.ClientCertificateFingerprints)),
		rms:          make([]common.RMId, len(config.rms)),
		rmsRemoved:   make(map[common.RMId]server.EmptyStruct, len(config.rmsRemoved)),
		fingerprints: make(map[[sha256.Size]byte]server.EmptyStruct, len(config.fingerprints)),
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
	if config.nextConfiguration != nil {
		// Assumption is that conditions are immutable. So the only
		// thing we'll want to do is shrink the list, so we do a copy of
		// the list, not a deep copy of the conditions.
		pending := make([]Cond, len(config.nextConfiguration.Pending))
		copy(pending, config.nextConfiguration.Pending)
		clone.nextConfiguration = &NextConfiguration{
			Configuration:  config.nextConfiguration.Configuration.Clone(),
			InstalledOnAll: config.nextConfiguration.InstalledOnAll,
			Pending:        pending,
		}
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
		cap.SetTransitioningTo()
		next := cap.TransitioningTo()
		next.SetConfiguration(config.nextConfiguration.Configuration.AddToSegAutoRoot(seg))
		next.SetInstalledOnAll(config.nextConfiguration.InstalledOnAll)
		next.SetPending(config.nextConfiguration.Pending.AddToSeg(seg))
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

type Conds []Cond

func (a Conds) Equal(b Conds) bool {
	if len(a) != len(b) {
		return false
	}
	for idx, aCond := range a {
		bCond := b[idx]
		if !aCond.Equal(bCond) {
			return false
		}
	}
	return true
}
func (c Conds) AddToSeg(seg *capn.Segment) msgs.Condition_List {
	cap := msgs.NewConditionList(seg, len(c))
	for idx, cond := range c {
		cap.Set(idx, cond.AddToSeg(seg))
	}
	return cap
}

func (c Conds) String() string {
	str := ""
	for k, v := range c {
		str += fmt.Sprintf("\n  %v: %v", k, v)
	}
	if str == "" {
		return ""
	}
	return str[1:]
}

type Cond interface {
	Equal(Cond) bool
	AddToSeg(seg *capn.Segment) msgs.Condition
	condWitness()
}

func ConditionsFromCap(condsCap *msgs.Condition_List) Conds {
	conds := make([]Cond, condsCap.Len())
	for idx := range conds {
		condCap := condsCap.At(idx)
		conds[idx] = conditionFromCap(&condCap)
	}
	return conds
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
		gen := &Generator{
			RMId:     common.RMId(condGen.RmId()),
			PermLen:  condGen.PermLen(),
			Start:    condGen.Start(),
			Includes: condGen.Includes(),
		}
		if condGen.Which() == msgs.GENERATOR_LENSIMPLE {
			gen.Len = condGen.LenSimple()
		} else {
			lai := condGen.LenAdjustIntersect()
			gen.LenAdjustIntersect = make([]common.RMId, lai.Len())
			for idx := range gen.LenAdjustIntersect {
				gen.LenAdjustIntersect[idx] = common.RMId(lai.At(idx))
			}
		}
		return gen
	default:
		panic(fmt.Sprintf("Unexpected Condition type (%v)", condCap.Which()))
		return nil
	}
}

type Conjunction struct {
	Left  Cond
	Right Cond
}

func (c *Conjunction) condWitness()   {}
func (c *Conjunction) String() string { return fmt.Sprintf("(%v ∧ %v)", c.Left, c.Right) }

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

func (d *Disjunction) condWitness()   {}
func (d *Disjunction) String() string { return fmt.Sprintf("(%v ∨ %v)", d.Left, d.Right) }

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
	RMId               common.RMId
	PermLen            uint8
	Start              uint8
	Len                uint8
	LenAdjustIntersect common.RMIds
	Includes           bool
}

func (g *Generator) condWitness() {}
func (g *Generator) String() string {
	op := "∈"
	if !g.Includes {
		op = "∉"
	}
	start := ""
	if g.Start > 0 {
		start = fmt.Sprintf("%v", g.Start)
	}
	end := fmt.Sprintf("%v", g.Start+g.Len)
	if len(g.LenAdjustIntersect) > 0 {
		set := ""
		for _, rmId := range g.LenAdjustIntersect {
			set += fmt.Sprintf(",%s", rmId)
		}
		end = fmt.Sprintf("%v+|(p,%v)[:%v] ∩ {%v}|", g.Start, g.PermLen, g.Start+g.Len, set[1:])
	}
	return fmt.Sprintf("%v %v (p,%v)[%s:%v]", g.RMId, op, g.PermLen, start, end)
}

func (a *Generator) Equal(b Cond) bool {
	bGen, ok := b.(*Generator)
	if !ok {
		return false
	}
	if a == nil || b == nil || bGen == nil {
		return a == b || a == bGen
	}
	return a.RMId == bGen.RMId && a.PermLen == bGen.PermLen && a.Start == bGen.Start && a.Len == bGen.Len &&
		a.Includes == bGen.Includes && a.LenAdjustIntersect.Equal(bGen.LenAdjustIntersect)
}

func (g *Generator) AddToSeg(seg *capn.Segment) msgs.Condition {
	genCap := msgs.NewGenerator(seg)
	genCap.SetRmId(uint32(g.RMId))
	genCap.SetPermLen(g.PermLen)
	genCap.SetStart(g.Start)
	genCap.SetIncludes(g.Includes)
	if len(g.LenAdjustIntersect) > 0 {
		rmIds := seg.NewUInt32List(len(g.LenAdjustIntersect))
		for idx, rmId := range g.LenAdjustIntersect {
			rmIds.Set(idx, uint32(rmId))
		}
		genCap.SetLenAdjustIntersect(rmIds)
	} else {
		genCap.SetLenSimple(g.Len)
	}
	condCap := msgs.NewCondition(seg)
	condCap.SetGenerator(genCap)
	return condCap
}
