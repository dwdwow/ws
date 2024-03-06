package wsclt

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type ClientStatus int

const (
	ClientStatusNew ClientStatus = iota
	ClientStatusDialing
	ClientStatusConnected
	ClientStatusRedial
)

type BaseClient struct {
	mux      sync.Mutex
	readMux  sync.Mutex
	writeMux sync.Mutex

	ctx           context.Context
	ctxCancelFunc context.CancelFunc

	id string

	url     string
	urlFunc func() (string, error)

	dialer              websocket.Dialer
	connId              string
	conn                *websocket.Conn
	connTsMilliList     []int64
	connFailTimes       int
	needRecon           bool
	reconWaitingTsMilli int64

	maxTopicNum int
	topics      []string

	conCallback  func(bc *BaseClient) error
	topicSuber   func(bc *BaseClient, topics []string) error
	topicUnsuber func(bc *BaseClient, topics []string) error
	ping         func(ctx context.Context, bc *BaseClient)
	pong         func(bc *BaseClient)

	successStatusCode int

	muxStatus sync.RWMutex
	status    ClientStatus

	createTsMilli           int64
	latestServerPingTsMilli int64
	latestServerPongTsMilli int64

	logger *slog.Logger
}

func NewBaseClient(url string, repeated bool, maxTopicNum int, logger *slog.Logger) *BaseClient {
	id := createBaseClientId()
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stdout, nil))
	}
	logger = logger.With("ws_base_clt_id", id)
	return &BaseClient{
		mux:                 sync.Mutex{},
		id:                  id,
		url:                 url,
		dialer:              websocket.Dialer{},
		maxTopicNum:         maxTopicNum,
		topics:              []string{},
		needRecon:           repeated,
		reconWaitingTsMilli: 1000,
		successStatusCode:   101,
		createTsMilli:       time.Now().UnixMilli(),
		logger:              logger,
	}
}

func (bc *BaseClient) SetConCallback(cb func(bc *BaseClient) error) *BaseClient {
	bc.conCallback = cb
	return bc
}

func (bc *BaseClient) SetTopicSuber(suber func(bc *BaseClient, topics []string) error) *BaseClient {
	bc.topicSuber = suber
	return bc
}

func (bc *BaseClient) SetTopicUnSuber(unsuber func(bc *BaseClient, topics []string) error) *BaseClient {
	bc.topicUnsuber = unsuber
	return bc
}

func (bc *BaseClient) SetPing(ping func(ctx context.Context, bc *BaseClient)) *BaseClient {
	bc.ping = ping
	return bc
}

func (bc *BaseClient) SetPong(pong func(bc *BaseClient)) *BaseClient {
	bc.pong = pong
	return bc
}

func (bc *BaseClient) SetUrlFunc(urlFunc func() (string, error)) *BaseClient {
	bc.urlFunc = urlFunc
	return bc
}

func (bc *BaseClient) SetSuccessStatusCode(code int) *BaseClient {
	bc.successStatusCode = code
	return bc
}

func (bc *BaseClient) SetReconWaitingTsMilli(tsMilli int64) *BaseClient {
	bc.reconWaitingTsMilli = tsMilli
	return bc
}

func (bc *BaseClient) Status() ClientStatus {
	bc.muxStatus.RLock()
	defer bc.muxStatus.RUnlock()
	return bc.status
}

func (bc *BaseClient) setStatus(s ClientStatus) {
	bc.muxStatus.Lock()
	defer bc.muxStatus.Unlock()
	bc.status = s
}

func (bc *BaseClient) Topics() []string {
	return bc.topics
}

func (bc *BaseClient) NeedRecon() bool {
	return bc.needRecon
}

func (bc *BaseClient) RunTsMilli() int64 {
	return time.Now().UnixMilli() - bc.createTsMilli
}

func (bc *BaseClient) ConnTsMilliList() []int64 {
	return bc.connTsMilliList
}

func (bc *BaseClient) SetLatestServerPingTsMilli(ts int64) {
	bc.latestServerPingTsMilli = ts
}

func (bc *BaseClient) LatestServerPingTsMilli() int64 {
	return bc.latestServerPingTsMilli
}

func (bc *BaseClient) SetLatestServerPongTsMilli(ts int64) {
	bc.latestServerPongTsMilli = ts
}

func (bc *BaseClient) LatestServerPongTsMilli() int64 {
	return bc.latestServerPongTsMilli
}

func (bc *BaseClient) Start() error {
	bc.mux.Lock()
	defer func() {
		if bc.Status() == ClientStatusDialing {
			bc.setStatus(ClientStatusNew)
		}
		bc.mux.Unlock()
	}()
	if bc.Status() != ClientStatusNew {
		bc.connFailTimes++
		return errors.New("wsclt: " + bc.id + " is not new")
	}
	bc.setStatus(ClientStatusDialing)
	url := bc.url
	var err error
	if bc.urlFunc != nil {
		url, err = bc.urlFunc()
		if err != nil {
			return errors.New("wsclt: " + err.Error())
		}
	}
	conn, httpResp, err := bc.dialer.Dial(url, nil)
	if err != nil {
		bc.connFailTimes++
		return fmt.Errorf("wsclt: dial %v, %w, fail times: %v", url, err, bc.connFailTimes)
	}
	if httpResp.StatusCode != bc.successStatusCode {
		bc.connFailTimes++
		return fmt.Errorf("wsclt: response code %v, require code %v", httpResp.StatusCode, bc.successStatusCode)
	}
	bc.conn = conn
	if bc.conCallback != nil {
		err := bc.conCallback(bc)
		if err != nil {
			_ = bc.conn.Close()
			bc.conn = nil
			return fmt.Errorf("wsclt: %w", err)
		}
	}
	bc.setStatus(ClientStatusConnected)
	bc.connTsMilliList = append(bc.connTsMilliList, time.Now().UnixMilli())
	bc.connId = createConnId(bc.id)
	bc.ctx, bc.ctxCancelFunc = context.WithCancel(context.Background())
	if bc.ping != nil {
		go bc.ping(bc.ctx, bc)
	}
	return nil
}

func (bc *BaseClient) Read() (msgType int, msgData []byte, err error) {
	// websocket can not support concurrent read
	bc.readMux.Lock()
	defer bc.readMux.Unlock()
	err = bc.checkBeforeReading()
	if err != nil {
		err = fmt.Errorf("wsclt: read msg, %w", err)
		return
	}
	msgType, msgData, err = bc.conn.ReadMessage()
	if err != nil {
		bc.handleRedialWork()
		err = fmt.Errorf("wsclt: read msg, %w", err)
		return
	}
	switch msgType {
	case websocket.CloseMessage:
		bc.handleRedialWork()
		err = fmt.Errorf("wsclt: closed")
	case websocket.PingMessage:
		bc.latestServerPingTsMilli = time.Now().UnixMilli()
		bc.logger.Info("Server Ping", "msg", string(msgData))
		if bc.pong != nil {
			go bc.pong(bc)
		}
	case websocket.PongMessage:
		bc.latestServerPongTsMilli = time.Now().UnixMilli()
		bc.logger.Info("Server Pong", "msg", string(msgData))
	case websocket.BinaryMessage, websocket.TextMessage:
	default:
		err = fmt.Errorf("wsclt: unknown message type %v, msg: %v", msgType, string(msgData))
	}
	return
}

func (bc *BaseClient) WriteJSON(v any) error {
	// websocket do not support concurrent write
	bc.writeMux.Lock()
	defer bc.writeMux.Unlock()
	if bc.conn == nil {
		return errors.New("wsclt: write JSON, conn is nil")
	}
	err := bc.conn.WriteJSON(v)
	if err != nil {
		return fmt.Errorf("wsclt: write JSON, %w", err)
	}
	return nil
}

func (bc *BaseClient) WriteText(v []byte) error {
	// websocket do not support concurrent write
	bc.writeMux.Lock()
	defer bc.writeMux.Unlock()
	if bc.conn == nil {
		return errors.New("wsclt: write JSON, conn is nil")
	}
	err := bc.conn.WriteMessage(websocket.TextMessage, v)
	if err != nil {
		return fmt.Errorf("wsclt: write text, %w", err)
	}
	return nil
}

func (bc *BaseClient) Pong(v []byte) error {
	// websocket do not support concurrent write
	bc.writeMux.Lock()
	defer bc.writeMux.Unlock()
	if bc.conn == nil {
		return errors.New("wsclt: conn is nil")
	}
	err := bc.conn.WriteMessage(websocket.PongMessage, v)
	if err != nil {
		return fmt.Errorf("wsclt: pong, %w", err)
	}
	return nil
}

func (bc *BaseClient) Sub(newTopics []string) (remainTopics []string, err error) {
	bc.mux.Lock()
	defer bc.mux.Unlock()
	newTopics = bc.filterTopics(newTopics)
	if bc.topicSuber == nil {
		err = errors.New("wsclt: topic suber is nil")
		return
	}
	if len(newTopics) == 0 {
		return []string{}, nil
	}
	oldTopicsLen := len(bc.topics)
	newTopicsLen := len(newTopics)
	topicsCap := bc.maxTopicNum - oldTopicsLen
	canSubTopics := newTopics
	if topicsCap < newTopicsLen {
		canSubTopics = newTopics[:topicsCap]
		remainTopics = newTopics[topicsCap:]
	}
	err = bc.topicSuber(bc, canSubTopics)
	if err != nil {
		err = fmt.Errorf("wsclt: sub topics, %w", err)
	} else {
		bc.topics = append(bc.topics, canSubTopics...)
	}
	return
}

func (bc *BaseClient) Unsub(topics []string) (currentTopics []string, err error) {
	bc.mux.Lock()
	defer bc.mux.Unlock()
	if len(bc.topics) <= 0 {
		return
	}
	if bc.topicUnsuber == nil {
		err = errors.New("wsclt: topic unsuber is nil")
		return
	}
	currentTopics = bc.topics
	err = bc.topicUnsuber(bc, topics)
	if err != nil {
		return
	}
	for _, topic := range topics {
		for i, oldTopic := range currentTopics {
			if oldTopic == topic {
				oldLen := len(currentTopics)
				currentTopics[i] = currentTopics[oldLen-1]
				currentTopics = currentTopics[:oldLen-1]
				break
			}
		}
	}
	return
}

func (bc *BaseClient) filterTopics(newTopics []string) []string {
	return filterTopics(bc.topics, newTopics)
}

func (bc *BaseClient) checkBeforeReading() error {
	if bc.conn == nil {
		return errors.New("wsclt: conn is nil")
	}
	if bc.Status() != ClientStatusConnected {
		return fmt.Errorf("wsclt: status is %v, not connected", bc.status)
	}
	return nil
}

func (bc *BaseClient) handleRedialWork() {
	bc.setStatus(ClientStatusRedial)
	err := bc.CloseConn()
	if err != nil {
		bc.logger.Error("Cannot Close Conn Before Redialing", "err", err)
	}
	if !bc.needRecon {
		return
	}
	go bc.reconnect()
}

func (bc *BaseClient) reconnect() {
	for {
		bc.logger.Info("Reconnecting")
		err := bc.Start()
		if bc.Status() == ClientStatusConnected {
			break
		}
		if err != nil {
			bc.logger.Error("Cannot Reconnect")
			time.Sleep(time.Millisecond * time.Duration(bc.reconWaitingTsMilli))
		}
	}
	if bc.topicSuber != nil {
		// must sub by topicSuber
		err := bc.topicSuber(bc, bc.topics)
		if err != nil {
			bc.logger.Error("Cannot Sub Topics After Reconnecting", "err", err)
			bc.handleRedialWork()
			return
		}
	}
	bc.logger.Info("Reconnected")
}

func (bc *BaseClient) CloseConn() error {
	bc.mux.Lock()
	if bc.ctxCancelFunc != nil {
		bc.ctxCancelFunc()
	}
	defer func() {
		bc.setStatus(ClientStatusNew)
		bc.mux.Unlock()
		// do not set conn nil here
		// bc.conn = nil
	}()
	if bc.conn == nil {
		return nil
	}
	err := bc.conn.Close()
	if err != nil {
		return fmt.Errorf("wsclt: cannot close, %w", err)
	}
	return nil
}

func filterTopics(oldTopics, newTopics []string) []string {
	mapOldTopics := map[string]bool{}
	for _, topic := range oldTopics {
		mapOldTopics[topic] = true
	}
	var validTopics []string
	for _, topic := range newTopics {
		if mapOldTopics[topic] {
			continue
		}
		validTopics = append(validTopics, topic)
	}
	return validTopics
}

var muxConnCreateTimes = sync.Mutex{}
var wsConnCreateTimes int

func createConnId(baseClientId string) string {
	muxConnCreateTimes.Lock()
	wsConnCreateTimes++
	muxConnCreateTimes.Unlock()
	return fmt.Sprintf("ws_conn_id-%v-%v-%v", time.Now().UnixNano(), wsConnCreateTimes, baseClientId)
}

var muxWsCreateTimes = sync.Mutex{}
var wsCreateTimes int

func createBaseClientId() string {
	muxWsCreateTimes.Lock()
	wsCreateTimes++
	muxWsCreateTimes.Unlock()
	return fmt.Sprintf("ws_clt_id-%v-%v", time.Now().UnixNano(), wsCreateTimes)
}
