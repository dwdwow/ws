package wsclt

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

type MergedClientMsg struct {
	MsgType int
	Data    []byte
	Err     error
	Client  *BaseClient
}

type MergedClient struct {
	mux sync.Mutex

	ctx           context.Context
	ctxCancelFunc context.CancelFunc

	id          string
	url         string
	maxTopicNum int
	clients     []*BaseClient

	needRecon           bool
	reconWaitingTsMilli int64

	conCallback  func(client *BaseClient) error
	topicSuber   func(client *BaseClient, topics []string) error
	topicUnsuber func(client *BaseClient, topics []string) error
	ping         func(ctx context.Context, client *BaseClient)
	pong         func(client *BaseClient)
	msgCallback  func(msg MergedClientMsg)
	msgCh        chan<- MergedClientMsg
	urlFunc      func() (string, error)

	successStatusCode int
	createTsMilli     int64

	logger *slog.Logger
}

func NewMergedClient(url string, repeated bool, maxTopicNum int, logger *slog.Logger) *MergedClient {
	id := createMergedCltId()
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stdout, nil))
	}
	logger = logger.With("ws_merged_clt_id", id)
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &MergedClient{
		id:                  id,
		ctx:                 ctx,
		ctxCancelFunc:       cancelFunc,
		url:                 url,
		maxTopicNum:         maxTopicNum,
		needRecon:           repeated,
		reconWaitingTsMilli: 1000,
		successStatusCode:   101,
		createTsMilli:       time.Now().UnixMilli(),
		logger:              logger,
	}
}

func (mc *MergedClient) SetConCallback(cb func(client *BaseClient) error) *MergedClient {
	mc.conCallback = cb
	return mc
}

func (mc *MergedClient) SetTopicSuber(suber func(client *BaseClient, topics []string) error) *MergedClient {
	mc.topicSuber = suber
	return mc
}

func (mc *MergedClient) SetTopicUnsuber(unsuber func(client *BaseClient, topics []string) error) *MergedClient {
	mc.topicUnsuber = unsuber
	return mc
}

func (mc *MergedClient) SetPing(ping func(ctx context.Context, client *BaseClient)) *MergedClient {
	mc.ping = ping
	return mc
}

func (mc *MergedClient) SetPong(pong func(client *BaseClient)) *MergedClient {
	mc.pong = pong
	return mc
}

func (mc *MergedClient) SetUrlFunc(urlFunc func() (string, error)) *MergedClient {
	mc.urlFunc = urlFunc
	return mc
}

func (mc *MergedClient) SetMsgCallback(callback func(msg MergedClientMsg)) *MergedClient {
	mc.msgCallback = callback
	return mc
}

func (mc *MergedClient) SetMsgCh(msgCh chan<- MergedClientMsg) *MergedClient {
	mc.msgCh = msgCh
	return mc
}

func (mc *MergedClient) SetSuccessStatusCode(code int) *MergedClient {
	mc.successStatusCode = code
	return mc
}

func (mc *MergedClient) SetReconWaitingTsMilli(tsMilli int64) *MergedClient {
	mc.reconWaitingTsMilli = tsMilli
	return mc
}

func (mc *MergedClient) AddClient(topics []string) (remainTopics []string, err error) {
	mc.mux.Lock()
	defer mc.mux.Unlock()
	newClient := mc.newBaseClient()
	err = newClient.Start()
	if err != nil {
		return
	}
	mc.clients = append(mc.clients, newClient)
	go mc.waitMsg(mc.ctx, newClient)
	if len(topics) <= 0 {
		return
	}
	return newClient.Sub(topics)
}

func (mc *MergedClient) Sub(topics []string) error {
	mc.mux.Lock()
	defer mc.mux.Unlock()
	topics = mc.filterTopics(topics)
	if len(topics) == 0 {
		return nil
	}
	var err error
	remainTopics := topics
	for _, client := range mc.clients {
		remainTopics, err = client.Sub(remainTopics)
		if err != nil {
			return err
		}
		if len(remainTopics) <= 0 {
			return nil
		}
	}
	for {
		newClient := mc.newBaseClient()
		err = newClient.Start()
		if err != nil {
			return err
		}
		mc.clients = append(mc.clients, newClient)
		go mc.waitMsg(mc.ctx, newClient)
		remainTopics, err = newClient.Sub(remainTopics)
		if err != nil {
			return err
		}
		if len(remainTopics) <= 0 {
			return nil
		}
	}
}

func (mc *MergedClient) Unsub(topics []string) error {
	mc.mux.Lock()
	defer mc.mux.Unlock()
	for _, topic := range topics {
		for _, client := range mc.clients {
			for _, oldTopic := range client.topics {
				if oldTopic != topic {
					continue
				}
				_, err := client.Unsub([]string{topic})
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (mc *MergedClient) WriteJSON(v any) error {
	for _, client := range mc.clients {
		err := client.WriteJSON(v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (mc *MergedClient) filterTopics(newTopics []string) []string {
	var totalOldTopics []string
	for _, client := range mc.clients {
		totalOldTopics = append(totalOldTopics, client.topics...)
	}
	return filterTopics(totalOldTopics, newTopics)
}

func (mc *MergedClient) newBaseClient() *BaseClient {
	return NewBaseClient(mc.url, mc.needRecon, mc.maxTopicNum, mc.logger).
		SetConCallback(mc.conCallback).
		SetTopicSuber(mc.topicSuber).
		SetTopicUnSuber(mc.topicUnsuber).
		SetPing(mc.ping).
		SetPong(mc.pong).
		SetUrlFunc(mc.urlFunc).
		SetSuccessStatusCode(mc.successStatusCode).
		SetReconWaitingTsMilli(mc.reconWaitingTsMilli)
}

func (mc *MergedClient) waitMsg(ctx context.Context, client *BaseClient) {
	for {
		msgType, data, err := client.Read()
		newMsg := MergedClientMsg{
			MsgType: msgType,
			Data:    data,
			Err:     err,
			Client:  client,
		}
		if mc.msgCallback != nil {
			mc.msgCallback(newMsg)
		}
		if mc.msgCh != nil {
			mc.msgCh <- newMsg
		}
		if err != nil {
			if !client.needRecon {
				return
			}
			select {
			case <-time.After(time.Millisecond * 100):
			case <-mc.ctx.Done():
				_ = client.CloseConn()
				return
			}
		}
	}
}

var muxMergedCltCreateTimes sync.Mutex
var mergedCltCreateTimes int

func createMergedCltId() string {
	muxMergedCltCreateTimes.Lock()
	mergedCltCreateTimes++
	muxMergedCltCreateTimes.Unlock()
	return fmt.Sprintf("merged_clt_%v_%v", time.Now().UnixNano(), mergedCltCreateTimes)
}
