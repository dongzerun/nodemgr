package nodemgr

import (
	"encoding/json"
	"io/ioutil"
	//    "fmt"
	"errors"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	HEALTHY = iota
	UNHEALTHY
)

type ServiceConf struct {
	Hosts            []string `json:"hosts"`
	HealthyThreshold int64    `json:"healthyThreshold"`
	MaxCooldownTime  int64    `json:"maxCooldownTime"`
	MinHealthyRatio  float64  `json:"minHealthyRatio"`
}

func NewServiceConf() *ServiceConf {
	return &ServiceConf{
		Hosts:            make([]string, 0),
		HealthyThreshold: 10,
		MaxCooldownTime:  30,
		MinHealthyRatio:  0.67,
	}
}

func (sc *ServiceConf) AddHosts(hosts ...string) {
	for _, h := range hosts {
		sc.Hosts = append(sc.Hosts, h)
	}
}

type ConfigJson struct {
	WorkerCycle int                     `json:"workerCycle"`
	Services    map[string]*ServiceConf `json:"services"`
}

func NewConfigJson() *ConfigJson {
	return &ConfigJson{
		WorkerCycle: 1,
		Services:    make(map[string]*ServiceConf),
	}
}

func (cj *ConfigJson) AddService(name string, c *ServiceConf) {
	if _, exists := cj.Services[name]; exists {
		panic("nodemgr " + name + " service already exists")
	}

	cj.Services[name] = c
}

type NodeInfo struct {
	Host              string
	Status            int
	Vote              int64
	startCooldownTime int64        // 开始被冷冻的起始时间
	rwlock            sync.RWMutex // 读写锁，用于保护status和vote的更新
}

type AllNodesInfo struct {
	AllNodesCount       int
	HealthyNodesCount   int
	UnhealthyNodesCount int
	AllNodes            []NodeInfo
}

type SafeRand struct {
	mutex   sync.Mutex
	randGen *rand.Rand
}

type ServiceInfo struct {
	allNodesMap     map[string]*NodeInfo
	allNodesSlice   []string
	healthyNodesOne []string
	healthyNodesTwo []string
	curHealthyNodes int          // 值为１或者２，指向当前有效的健康列表
	rwlock          sync.RWMutex // 读写锁，用于保护健康列表的更新
	safeRand        SafeRand
}

type Nodemgr struct {
	cfgJson     *ConfigJson
	allServices map[string]*ServiceInfo
	workerRun   bool
}

func (sr *SafeRand) intn(n int) (res int) {
	sr.mutex.Lock()
	res = sr.randGen.Intn(n)
	sr.mutex.Unlock()
	return res
}

func (nm *Nodemgr) Init(conf string) (err error) {
	c := NewConfigJson()
	data, err := ioutil.ReadFile(conf)
	if err != nil {
		return err
	}

	if err = json.Unmarshal(data, c); err != nil {
		return
	}

	return nm.InitWithConfig(c)
}

func (nm *Nodemgr) InitWithConfig(conf *ConfigJson) (err error) {

	nm.cfgJson = conf

	if nm.cfgJson.WorkerCycle <= 0 {
		return errors.New("conf WorkerCycle error")
	}

	servicesCnt := 0
	for sn, snConf := range nm.cfgJson.Services {
		if snConf.HealthyThreshold < 3 {
			return errors.New("conf HealthyThreshold error")
		}
		if snConf.MinHealthyRatio < 0.0 || snConf.MinHealthyRatio > 1.0 {
			return errors.New("conf MinHealthyRatio error")
		}
		if snConf.MaxCooldownTime <= 0 {
			return errors.New("conf MaxCooldownTime error")
		}
		if len(snConf.Hosts) <= 0 {
			return errors.New("conf Hosts error")
		}

		var sinfo ServiceInfo
		sinfo.allNodesMap = make(map[string]*NodeInfo)
		sinfo.allNodesSlice = make([]string, 0, len(snConf.Hosts))
		sinfo.curHealthyNodes = 0
		sinfo.safeRand = SafeRand{randGen: rand.New(rand.NewSource(time.Now().UnixNano()))}

		for _, host := range snConf.Hosts {
			var ninfo NodeInfo
			ninfo.Host = host
			ninfo.Status = HEALTHY
			ninfo.Vote = 0
			ninfo.startCooldownTime = 0

			sinfo.allNodesMap[host] = &ninfo
			sinfo.healthyNodesOne = append(sinfo.healthyNodesOne, host)
			sinfo.allNodesSlice = append(sinfo.allNodesSlice, host)
		}
		shuffle(sinfo.allNodesSlice)
		nm.allServices[sn] = &sinfo
		servicesCnt++
	}
	//fmt.Println(nm.allServices)

	if servicesCnt <= 0 {
		return errors.New("conf Services error")
	}

	nm.workerRun = true
	go nodemgrWorker(nm)

	return
}

func (nm *Nodemgr) GetAllNodes(serviceName string) (AllNodesInfo, error) {
	var nodes AllNodesInfo
	if _, ok := nm.allServices[serviceName]; !ok {
		return nodes, errors.New("servicename not exist")
	}
	nodes.AllNodesCount = 0
	nodes.HealthyNodesCount = 0
	nodes.UnhealthyNodesCount = 0
	for _, ninfo := range nm.allServices[serviceName].allNodesMap {
		nodes.AllNodesCount++
		if ninfo.Status == HEALTHY {
			nodes.HealthyNodesCount++
		} else {
			nodes.UnhealthyNodesCount++
		}
		var n NodeInfo
		n.Host = ninfo.Host
		n.Status = ninfo.Status
		n.Vote = ninfo.Vote
		nodes.AllNodes = append(nodes.AllNodes, n)
	}
	return nodes, nil
}

func (nm *Nodemgr) GetNode(serviceName string, blacknode string) (string, error) {
	if _, ok := nm.allServices[serviceName]; !ok {
		return "", errors.New("servicename not exist")
	}

	var blackIp string
	if len(blacknode) != 0 {
		blackIp = (strings.SplitAfterN(blacknode, ":", 2))[0]
		if strings.HasSuffix(blackIp, ":") == false {
			blackIp += ":"
		}
	}
	//fmt.Println(blackIp)

	var h string
	nm.allServices[serviceName].rwlock.RLock()
	if nm.allServices[serviceName].curHealthyNodes == 0 {
		cnt := len(nm.allServices[serviceName].healthyNodesOne)
		r := nm.allServices[serviceName].safeRand.intn(cnt)
		for i := cnt; i > 0; i-- {
			if strings.HasPrefix(nm.allServices[serviceName].healthyNodesOne[r], blackIp) == false || i == 1 {
				h = nm.allServices[serviceName].healthyNodesOne[r]
				break
			}
			r = (r + 1) % cnt
		}
	} else if nm.allServices[serviceName].curHealthyNodes == 1 {
		cnt := len(nm.allServices[serviceName].healthyNodesTwo)
		r := nm.allServices[serviceName].safeRand.intn(cnt)
		for i := cnt; i > 0; i-- {
			if strings.HasPrefix(nm.allServices[serviceName].healthyNodesTwo[r], blackIp) == false || i == 1 {
				h = nm.allServices[serviceName].healthyNodesTwo[r]
				break
			}
			r = (r + 1) % cnt
		}
	}
	nm.allServices[serviceName].rwlock.RUnlock()

	if len(h) != 0 {
		return h, nil
	}

	return "", errors.New("node not found")
}

func (nm *Nodemgr) Vote(serviceName string, node string, voteType int) error {
	if _, ok := nm.allServices[serviceName]; !ok {
		return errors.New("servicename not exist")
	}

	if _, ok := nm.allServices[serviceName].allNodesMap[node]; !ok {
		return errors.New("node not exist")
	}

	if voteType != HEALTHY && voteType != UNHEALTHY {
		return errors.New("notetype error")
	}

	nm.allServices[serviceName].allNodesMap[node].rwlock.Lock()
	defer nm.allServices[serviceName].allNodesMap[node].rwlock.Unlock()

	if voteType == HEALTHY {
		if nm.allServices[serviceName].allNodesMap[node].Vote > 0 {
			nm.allServices[serviceName].allNodesMap[node].Vote -= 1
		}
	} else if voteType == UNHEALTHY {
		nm.allServices[serviceName].allNodesMap[node].Vote += 1
	}

	if nm.allServices[serviceName].allNodesMap[node].Vote < nm.cfgJson.Services[serviceName].HealthyThreshold {
		nm.allServices[serviceName].allNodesMap[node].Status = HEALTHY
	} else {
		nm.allServices[serviceName].allNodesMap[node].Status = UNHEALTHY
	}

	return nil
}

func (nm *Nodemgr) Uninit() {
	nm.workerRun = false
	return
}

type voteNode struct {
	host string
	vote int64
}
type byVote []voteNode

func (n byVote) Len() int           { return len(n) }
func (n byVote) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n byVote) Less(i, j int) bool { return n[i].vote < n[j].vote }

func nodemgrWorker(nm *Nodemgr) {
	if nm == nil {
		panic("nodemgr is nil")
	}

	for {
		if nm.workerRun == false {
			break
		}

		//for sn, sinfo := range(nm.allServices) {
		for sn := range nm.allServices {
			//fmt.Println("\n", "#############", sn, "#############")
			var nodelist []string
			var backup []voteNode
			allcnt := 0
			healthycnt := 0
			backupcnt := 0

			//挑选可用节点
			for _, node := range nm.allServices[sn].allNodesSlice {
				allcnt++

				nm.allServices[sn].allNodesMap[node].rwlock.Lock()
				if nm.allServices[sn].allNodesMap[node].Status == HEALTHY {
					nodelist = append(nodelist, node)
					nm.allServices[sn].allNodesMap[node].startCooldownTime = 0
					healthycnt++
				} else {
					if nm.allServices[sn].allNodesMap[node].startCooldownTime == 0 {
						//第一次被封禁
						//fmt.Println(sn, node, "first be cooldown")
						nm.allServices[sn].allNodesMap[node].startCooldownTime = time.Now().Unix()
						var b voteNode
						b.host = node
						b.vote = nm.allServices[sn].allNodesMap[node].Vote
						backup = append(backup, b)
						backupcnt++
					} else {
						if time.Now().Unix()-nm.allServices[sn].allNodesMap[node].startCooldownTime >
							nm.cfgJson.Services[sn].MaxCooldownTime {
							//封禁恢复，给于３次机会
							//fmt.Println(sn, node, "restore from cooldown")
							nm.allServices[sn].allNodesMap[node].Vote = nm.cfgJson.Services[sn].HealthyThreshold - 3
							nm.allServices[sn].allNodesMap[node].Status = HEALTHY
							nm.allServices[sn].allNodesMap[node].startCooldownTime = 0
							nodelist = append(nodelist, node)
							healthycnt++
						} else {
							//还未到恢复的时间
							//fmt.Println(sn, node, "still be cooldown")
							var b voteNode
							b.host = node
							b.vote = nm.allServices[sn].allNodesMap[node].Vote
							backup = append(backup, b)
							backupcnt++
						}
					}
				}
				nm.allServices[sn].allNodesMap[node].rwlock.Unlock()
			}
			//fmt.Println(sn, "nodelist", nodelist)

			//可用节点不够，需要补充
			needmore := int(math.Ceil(float64(allcnt)*nm.cfgJson.Services[sn].MinHealthyRatio)) - healthycnt
			if needmore > backupcnt {
				needmore = backupcnt
			}
			//fmt.Println(sn, "needmore", needmore)

			if needmore > 0 {
				//挑选投票数最少的节点
				sort.Sort(byVote(backup))
				//fmt.Println(sn, "sort", backup)
				for idx := 0; idx < needmore; idx++ {
					host := backup[idx].host

					nm.allServices[sn].allNodesMap[host].rwlock.Lock()
					nm.allServices[sn].allNodesMap[host].Vote = nm.cfgJson.Services[sn].HealthyThreshold - 3
					//nm.allServices[sn].allNodesMap[host].Vote = 0
					nm.allServices[sn].allNodesMap[host].Status = HEALTHY
					nm.allServices[sn].allNodesMap[host].startCooldownTime = 0
					nm.allServices[sn].allNodesMap[host].rwlock.Unlock()

					nodelist = append(nodelist, host)

					healthycnt++
				}
			}

			//双buffer切换
			if len(nodelist) >= int(math.Ceil(float64(allcnt)*nm.cfgJson.Services[sn].MinHealthyRatio)) {
				nm.allServices[sn].rwlock.Lock()
				if nm.allServices[sn].curHealthyNodes == 0 {
					nm.allServices[sn].healthyNodesTwo = nodelist
					nm.allServices[sn].curHealthyNodes = 1
				} else if nm.allServices[sn].curHealthyNodes == 1 {
					nm.allServices[sn].healthyNodesOne = nodelist
					nm.allServices[sn].curHealthyNodes = 0
				}
				nm.allServices[sn].rwlock.Unlock()
			}

			//调试信息
			/*
				            fmt.Println(sn, "nodelist", nodelist)
							            fmt.Println(sn, "sinfo", sinfo)
										            nodes, err := GetAllNodes(sn)
													            if err != nil {
																		                panic(err)
																						            }else{
																											                fmt.Println(sn, "allnodes", nodes)
																															            }
			*/
		}

		time.Sleep(time.Second * time.Duration(nm.cfgJson.WorkerCycle))
	}
}

// default
var (
	nodemgr_default *Nodemgr
	takeup          = false
)

func Init(conf string) error {
	return nodemgr_default.Init(conf)
}

func InitWithConfig(conf *ConfigJson) (err error) {
	return nodemgr_default.InitWithConfig(conf)
}

func Uninit() {
	nodemgr_default.Uninit()
}

func GetAllNodes(serviceName string) (AllNodesInfo, error) {
	return nodemgr_default.GetAllNodes(serviceName)
}

func GetNode(serviceName string, blacknode string) (string, error) {
	return nodemgr_default.GetNode(serviceName, blacknode)
}

func Vote(serviceName string, node string, voteType int) error {
	return nodemgr_default.Vote(serviceName, node, voteType)
}

func NewNodemgr() *Nodemgr {
	if nodemgr_default != nil && takeup == false {
		takeup = true
		return nodemgr_default
	}

	nm := &Nodemgr{
		allServices: make(map[string]*ServiceInfo),
	}

	return nm
}

func init() {
	nodemgr_default = NewNodemgr()
}

func shuffle(ssl []string) {
	for i, v := range rand.New(rand.NewSource(time.Now().UnixNano())).Perm(len(ssl)) {
		ssl[i], ssl[v] = ssl[v], ssl[i]
	}
}
