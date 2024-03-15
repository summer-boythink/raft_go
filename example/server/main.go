package main

import (
	"fmt"
	"log"
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

type CommandBody struct {
	command string
}

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
	r.Use(func(ctx *gin.Context) {
		if raft.LeaderID == nil {
			ctx.JSON(500, "no leader")
			return
		}

		if !raft.IsLeader() {
			ctx.JSON(400, "it now is no leader")
			return
		}
	})
	r.POST("/append_entries", func(c *gin.Context) {
		var body any
		c.BindJSON(&body)
		c.JSON(200, raft.HandleAppendEntries(body.(raftgo.AppendEntriesArgs)))
	})

	r.POST("/request_vote", func(c *gin.Context) {
		var body any
		c.BindJSON(&body)
		c.JSON(200, raft.HandleRequestVote(body.(raftgo.RequestVoteArgs)))
	})

	r.POST("/append", func(c *gin.Context) {
		var body any
		c.BindJSON(&body)
		commandBase64 := body.(CommandBody).command
		receiveHandleAppend := raft.HandleAppend(commandBase64)
		select {
		case <-receiveHandleAppend:
			c.JSON(200, map[string]bool{"success": true})
		case <-time.After(400 * time.Millisecond):
			c.JSON(200, map[string]bool{"success": false})
		}
	})

	r.GET("/get", func(c *gin.Context) {
		key := c.Query("key")
		c.JSON(200, map[string]interface{}{"value": stateMachine.Get(key)})
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
