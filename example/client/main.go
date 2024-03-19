package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"net/http"
	"time"

	raftgo "github.com/summer-boythink/raft_go"
)

type Option struct {
	Addr string
}

type Response struct {
	Success bool   `json:"success"`
	Value   string `json:"value,omitempty"`
}

var rootCmd = &cobra.Command{
	Use:   "client",
	Short: "A client for raft",
}

var setCmd = &cobra.Command{
	Use:   "set [key] [value]",
	Short: "Set the value of a string key to a string",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		addr, _ := cmd.Flags().GetString("addr")
		key := args[0]
		value := args[1]
		set(Option{Addr: addr}, key, value)
	},
}

var getCmd = &cobra.Command{
	Use:   "get [key]",
	Short: "Get the string value of a given string key",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		addr, _ := cmd.Flags().GetString("addr")
		key := args[0]
		get(Option{Addr: addr}, key)
	},
}

var rmCmd = &cobra.Command{
	Use:   "rm [key]",
	Short: "Remove a given key",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		addr, _ := cmd.Flags().GetString("addr")
		key := args[0]
		rm(Option{Addr: addr}, key)
	},
}

func main() {
	rootCmd.PersistentFlags().StringP("addr", "a", "", "server addr.")
	rootCmd.AddCommand(setCmd, getCmd, rmCmd)
	cobra.CheckErr(rootCmd.Execute())
}

func set(op Option, key string, value string) {
	command := raftgo.Command{Type: "set", Key: key, Value: value}
	send("POST", op.Addr, "/append", command)
}

func get(op Option, key string) {
	send("GET", op.Addr, "/get?key="+key, nil)
}

func rm(op Option, key string) {
	command := raftgo.Command{Type: "rm", Key: key}
	send("POST", op.Addr, "/append", command)
}

func send(method string, addr string, path string, body interface{}) {
	start := time.Now()
	jsonBody, _ := json.Marshal(body)
	if method == "GET" {
		resp, err := http.Get(addr + path)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		var result Response
		json.Unmarshal(data, &result)
		fmt.Println("Response:", result)
		fmt.Println("Latency:", time.Since(start))
	} else {
		resp, err := http.Post(addr+path, "application/json", bytes.NewBuffer(jsonBody))
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		var result Response
		json.Unmarshal(data, &result)
		fmt.Println("Response:", result)
		fmt.Println("Latency:", time.Since(start))
	}

}
