package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"

	raftgo "github.com/summer-boythink/raft_go"
)

var (
	local             string
	peer              []string
	rpcTimeout        int
	heartbeatTimeout  int
	heartbeatInterval int
)

var rootCmd = &cobra.Command{
	Use:   "server",
	Short: "raft server",
	Run: func(cmd *cobra.Command, args []string) {
		nodeAddr := append([]string{local}, peer...)
		sort.Strings(nodeAddr)
		u, err := url.Parse(local)
		if err != nil {
			log.Fatal(err)
		}
		portStr := u.Port()
		port, err := strconv.Atoi(portStr)
		if err != nil {
			log.Fatal(err)
		}

		peers := make(map[int]raftgo.Peer)
		var id int
		for i, addr := range nodeAddr {
			if addr == local {
				id = i
			} else {
				peers[i] = &raftgo.HttpPeer{Addr: addr}
			}
		}

		stateMachine := raftgo.NewMemStateMachine()

		httpServe(
			raftgo.NewRaft(id, raftgo.NewLogs(raftgo.NewMemStorage(), stateMachine), peers, raftgo.Config{
				RPCTimeout:        time.Duration(rpcTimeout),
				HeartbeatTimeout:  time.Duration(heartbeatTimeout),
				HeartbeatInterval: time.Duration(heartbeatInterval),
			}),
			stateMachine,
			port,
		)

	},
}

func httpServe(raft *raftgo.Raft, stateMachine *raftgo.MemStateMachine, port int) {
	r := gin.Default()
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})
	r.Run(fmt.Sprintf(":%d", port))
}

func main() {
	rootCmd.Flags().StringVarP(&local, "local", "l", "", "the raft server local url")
	rootCmd.MarkFlagRequired("local")
	rootCmd.Flags().StringSliceVarP(&peer, "peer", "p", []string{}, "the raft server peer")
	rootCmd.MarkFlagRequired("peer")
	rootCmd.Flags().IntVarP(&rpcTimeout, "rpcTimeout", "r", 100, "the raft server local url")
	rootCmd.Flags().IntVarP(&heartbeatTimeout, "heartbeatTimeout", "", 300, "")
	rootCmd.Flags().IntVarP(&heartbeatInterval, "heartbeatInterval", "", 100, "")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
