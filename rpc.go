package raftgo

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"
)

type Peer interface {
	AppendEntries(aea AppendEntriesArgs, timeout time.Duration) (AppendEntriesReply, error)
	RequestVote(rv RequestVoteArgs, timeout time.Duration) (RequestVoteReply, error)
}

type HttpPeer struct {
	Addr string
}

func NewHttpPeer(addr string) *HttpPeer {
	return &HttpPeer{Addr: addr}
}

func (p *HttpPeer) AppendEntries(aea AppendEntriesArgs, timeout time.Duration) (AppendEntriesReply, error) {
	res, err := p.post("append_entries", aea, timeout)
	if err != nil {
		return AppendEntriesReply{}, err
	}
	return res.(AppendEntriesReply), nil
}

func (p *HttpPeer) RequestVote(rv RequestVoteArgs, timeout time.Duration) (RequestVoteReply, error) {
	res, err := p.post("request_vote", rv, timeout)
	if err != nil {
		return RequestVoteReply{}, err
	}
	return res.(RequestVoteReply), nil
}

func (p *HttpPeer) post(method string, data interface{}, timeout time.Duration) (interface{}, error) {
	datas, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", p.Addr+"/"+method, bytes.NewBuffer(datas))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result interface{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	return result, nil
}
