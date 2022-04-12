package commands

import (
	"fmt"
	"os"
	"bufio"
	"sort"
	"context"
	"strings"
	"crypto/sha1"
    "encoding/hex"
    "strconv"

	rocks "github.com/go-redis/redis/v8"
	"github.com/urfave/cli"
)

var ClusterInitCommand = cli.Command{
	Name:   "cluster.init",
	Usage:  "kvrocks cluster init",
	Action: clusterInitAction,
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "s,shard", 
			Value: 1, 
			Usage: "shard number"},
		cli.StringFlag{
			Name:  "c,config",
			Value: "./",
			Usage: "config path, kvrocks nodes address"},
		cli.BoolFlag{
			Name:  "d,do",
			Usage: "flag do init cluster"},
	},
	Description: `
    make topu info and set to kvrocks nodes
    `,
}

var (
	SLOT_NUM      = 16384
	RESERVE_PORT  = 10000

	clientMap = make(map[string]*rocks.Client)
	nodeIdMap = make(map[string]string)

	ctx       = context.Background()
)

type Replication struct {
	Master string
	Slaves []string
	Slot   []int
}

func clusterInitAction(c *cli.Context) {
	shard := c.Int("s")
	conf  := c.String("c")
	do 	  := c.Bool("d")

	// necessary for cluster init
	if SLOT_NUM % shard != 0 {
		fmt.Println("SLOT_NUM(16384) can't divide shard")
		os.Exit(1)
	}

	// accquire nodes address
	file, err := os.Open(conf)
	if err != nil {
		fmt.Println("open config file err: ", err)
		os.Exit(1)
	}
	defer file.Close()
	var nodes []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
        line := scanner.Text()
   		nodes = append(nodes, line)
    }
    if err := scanner.Err(); err != nil {
    	fmt.Println("scan config file err: ", err)
		os.Exit(1)
    }
    nodeSize := len(nodes)
    if nodeSize == 0 {
    	fmt.Println("config nodes is empty")
		os.Exit(1)
    }
    if nodeSize < shard {
    	fmt.Println("config nodes less shard")
		os.Exit(1)
    }
    if nodeSize % shard != 0 {
    	fmt.Println("config nodes can't divide shard")
		os.Exit(1)
    }
    sort.Strings(nodes)

    // check nodes init state and cache node client
    for _, node := range nodes {
    	client := rocks.NewClient(&rocks.Options{
	        Addr: node,
	    })
	    ver, err := client.Do(ctx, "clusterx", "version").Result()
	    if err != nil {
	    	fmt.Println("node: ", node, " clusterx version err: ", err)
	    	os.Exit(1)
	    }
	    if ver.(string) != "-1" {
	    	fmt.Println("node: ", node, " clusterx version : ", ver, " not -1")
	    	os.Exit(1)
	    }
	    clientMap[node] = client

	    c := sha1.New()
		c.Write([]byte(node))
		bytes := c.Sum(nil)
		nodeIdMap[node] = hex.EncodeToString(bytes)
    }
    fmt.Println("nodes and shard check ok")

    replicates := MakeReplicates(nodes, shard)
    cmd := NodesInfo(replicates)
    fmt.Println(cmd)

    if do {
    	for node, cli := range clientMap {
    		if _, err := cli.Do(ctx, "clusterx", "setnodes", cmd, "0", "force").Result(); err != nil {
    			fmt.Println("clusterx setnodes err: ", err, "node: ", node)
	    		os.Exit(1)
    		}
    	}
    	fmt.Println("cluster init success!")
    }
}

func NodesInfo(replicates []*Replication) string{
	var cmd string
	// nodeid host port role master_id slot_info
	for _, replicate := range replicates {
		cmd = cmd + nodeIdMap[replicate.Master] + " "
		addr := strings.Split(replicate.Master, ":")
		cmd = cmd + addr[0] + " " + addr[1] + " "
		cmd = cmd + "master" + " " + "-" + " "
		cmd = cmd + strconv.Itoa(replicate.Slot[0]) + "-" + strconv.Itoa(replicate.Slot[1]) + "\n"
		for _, slave := range replicate.Slaves {
			cmd = cmd + nodeIdMap[slave] + " "
			addr := strings.Split(slave, ":")
			cmd = cmd + addr[0] + " " + addr[1] + " "
			cmd = cmd + "slave" + " " + nodeIdMap[replicate.Master] + "\n"
		}
	}
	return cmd
}

func PrintPlan(replicates []*Replication) {
	for _, replicate := range replicates {
		fmt.Println("slot: ", replicate.Slot)
		fmt.Print("     master:\n")
		fmt.Print("            ",replicate.Master, " (", nodeIdMap[replicate.Master], ")\n")
		fmt.Print("     slaves:\n")
		for _, slave := range replicate.Slaves {
			fmt.Print("            ",slave, " (", nodeIdMap[slave], ")\n")
		}
	}
}

func MakeReplicates(nodes []string, num int) []*Replication {
	replicates := make([]*Replication, num)

	info := make(map[string][]string)
	for _, node := range nodes {
		addr := strings.Split(node, ":")
		if len(addr) != 2 {
			fmt.Println("node addr format err : ", node)
	    	os.Exit(1)
		}
		if port, _ := strconv.Atoi(addr[1]); port >= (65535 - RESERVE_PORT) {
			fmt.Println("node port format more than (65535 - 10000) : ", node)
	    	os.Exit(1)
		}
		info[addr[0]] = append(info[addr[0]], addr[1])
	}

	var ipInfo []string
	for ip, _ := range info {
		ipInfo = append(ipInfo, ip)
	}
	sort.Strings(ipInfo)


	// for master
	idx := 0
	for idx < num {
		for _, ip := range ipInfo {
			if _, ok := info[ip]; !ok {
				continue
			}
			replicates[idx] = &Replication{
				Master: ip + ":" + info[ip][0],
			}
			if len(info[ip]) == 1 {
				delete(info, ip)
			} else {
				info[ip] = info[ip][1:]
			}
			idx++
			if idx == num {
				break
			}
		}
	}

	// for slaves
	slaves := len(nodes) / num - 1
	for slaves > 0 {
		for _, replicate := range replicates {
			if replicate == nil {
				fmt.Println("assgin master err")
	    		os.Exit(1)
			}
rotate:
			assgin := false
			for _, ip := range ipInfo {
				if _, ok := info[ip]; !ok {
					continue
				}
				masterAddr := strings.Split(replicate.Master, ":")
				if ip == masterAddr[0] && len(info) > 1 {
					continue
				} else {
					replicate.Slaves = append(replicate.Slaves, ip + ":" + info[ip][0])
					if len(info[ip]) == 1 {
						delete(info, ip)
					} else {
						info[ip] = info[ip][1:]
					}
					assgin = true
					break
				}
			}
			if !assgin {
				goto rotate
			}
		}
		slaves--
	}

	// for slot
	slots := SLOT_NUM / num
	cursor := 0
	for _, replicate := range replicates {
		replicate.Slot = append(replicate.Slot, cursor)
		cursor += slots
		replicate.Slot = append(replicate.Slot, cursor - 1)
	}

	return replicates
}