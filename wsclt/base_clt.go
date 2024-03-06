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

	conCallback  func(ws *BaseClient) error
	topicSuber   func(ws *BaseClient, topics []string) error
	topicUnsuber func(ws *BaseClient, topics []string) error
	ping         func(ctx context.Context, ws *BaseClient)
	pong         func(ws *BaseClient)

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
	logger = logger.With("id", id)
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

func (ws *BaseClient) SetConCallback(cb func(ws *BaseClient) error) *BaseClient {
	ws.conCallback = cb
	return ws
}

func (ws *BaseClient) SetTopicSuber(suber func(ws *BaseClient, topics []string) error) *BaseClient {
	ws.topicSuber = suber
	return ws
}

func (ws *BaseClient) SetTopicUnSuber(unsuber func(ws *BaseClient, topics []string) error) *BaseClient {
	ws.topicUnsuber = unsuber
	return ws
}

func (ws *BaseClient) SetPing(ping func(ctx context.Context, ws *BaseClient)) *BaseClient {
	ws.ping = ping
	return ws
}

func (ws *BaseClient) SetPong(pong func(ws *BaseClient)) *BaseClient {
	ws.pong = pong
	return ws
}

func (ws *BaseClient) SetUrlFunc(urlFunc func() (string, error)) *BaseClient {
	ws.urlFunc = urlFunc
	return ws
}

func (ws *BaseClient) SetSuccessStatusCode(code int) *BaseClient {
	ws.successStatusCode = code
	return ws
}

func (ws *BaseClient) SetReconWaitingTsMilli(tsMilli int64) *BaseClient {
	ws.reconWaitingTsMilli = tsMilli
	return ws
}

func (ws *BaseClient) Status() ClientStatus {
	ws.muxStatus.RLock()
	defer ws.muxStatus.RUnlock()
	return ws.status
}

func (ws *BaseClient) setStatus(s ClientStatus) {
	ws.muxStatus.Lock()
	defer ws.muxStatus.Unlock()
	ws.status = s
}

func (ws *BaseClient) Topics() []string {
	return ws.topics
}

func (ws *BaseClient) NeedRecon() bool {
	return ws.needRecon
}

func (ws *BaseClient) RunTsMilli() int64 {
	return time.Now().UnixMilli() - ws.createTsMilli
}

func (ws *BaseClient) ConnTsMilliList() []int64 {
	return ws.connTsMilliList
}

func (ws *BaseClient) SetLatestServerPingTsMilli(ts int64) {
	ws.latestServerPingTsMilli = ts
}

func (ws *BaseClient) LatestServerPingTsMilli() int64 {
	return ws.latestServerPingTsMilli
}

func (ws *BaseClient) SetLatestServerPongTsMilli(ts int64) {
	ws.latestServerPongTsMilli = ts
}

func (ws *BaseClient) LatestServerPongTsMilli() int64 {
	return ws.latestServerPongTsMilli
}

func (ws *BaseClient) Start() error {
	ws.mux.Lock()
	defer func() {
		if ws.Status() == ClientStatusDialing {
			ws.setStatus(ClientStatusNew)
		}
		ws.mux.Unlock()
	}()
	if ws.Status() != ClientStatusNew {
		ws.connFailTimes++
		return errors.New("wsclt: " + ws.id + " is not new")
	}
	ws.setStatus(ClientStatusDialing)
	url := ws.url
	var err error
	if ws.urlFunc != nil {
		url, err = ws.urlFunc()
		if err != nil {
			return errors.New("wsclt: " + err.Error())
		}
	}
	conn, httpResp, err := ws.dialer.Dial(url, nil)
	if err != nil {
		ws.connFailTimes++
		return fmt.Errorf("wsclt: dial %v, %w, fail times: %v", url, err, ws.connFailTimes)
	}
	if httpResp.StatusCode != ws.successStatusCode {
		ws.connFailTimes++
		return fmt.Errorf("wsclt: response code %v, require code %v", httpResp.StatusCode, ws.successStatusCode)
	}
	ws.conn = conn
	if ws.conCallback != nil {
		err := ws.conCallback(ws)
		if err != nil {
			_ = ws.conn.Close()
			ws.conn = nil
			return fmt.Errorf("wsclt: %w", err)
		}
	}
	ws.setStatus(ClientStatusConnected)
	ws.connTsMilliList = append(ws.connTsMilliList, time.Now().UnixMilli())
	ws.connId = createConnId(ws.id)
	ctx, cancelFunc := context.WithCancel(context.Background())
	ws.ctxCancelFunc = cancelFunc
	if ws.ping != nil {
		go ws.ping(ctx, ws)
	}
	return nil
}

func (ws *BaseClient) Read() (msgType int, msgData []byte, err error) {
	// websocket can not support concurrent read
	ws.readMux.Lock()
	defer ws.readMux.Unlock()
	err = ws.checkBeforeReading()
	if err != nil {
		err = fmt.Errorf("wsclt: read msg, %w", err)
		return
	}
	msgType, msgData, err = ws.conn.ReadMessage()
	if err != nil {
		ws.handleRedialWork()
		err = fmt.Errorf("wsclt: read msg, %w", err)
		return
	}
	switch msgType {
	case websocket.CloseMessage:
		ws.handleRedialWork()
		err = fmt.Errorf("wsclt: closed")
	case websocket.PingMessage:
		ws.latestServerPingTsMilli = time.Now().UnixMilli()
		ws.logger.Info("Server Ping", "msg", string(msgData))
		if ws.pong != nil {
			go ws.pong(ws)
		}
	case websocket.PongMessage:
		ws.latestServerPongTsMilli = time.Now().UnixMilli()
		ws.logger.Info("Server Pong", "msg", string(msgData))
	case websocket.BinaryMessage, websocket.TextMessage:
	default:
		err = fmt.Errorf("wsclt: unknown message type %v, msg: %v", msgType, string(msgData))
	}
	return
}

func (ws *BaseClient) WriteJSON(v any) error {
	// websocket do not support concurrent write
	ws.writeMux.Lock()
	defer ws.writeMux.Unlock()
	if ws.conn == nil {
		return errors.New("wsclt: write JSON, conn is nil")
	}
	err := ws.conn.WriteJSON(v)
	if err != nil {
		return fmt.Errorf("wsclt: write JSON, %w", err)
	}
	return nil
}

func (ws *BaseClient) WriteText(v []byte) error {
	// websocket do not support concurrent write
	ws.writeMux.Lock()
	defer ws.writeMux.Unlock()
	if ws.conn == nil {
		return errors.New("wsclt: write JSON, conn is nil")
	}
	err := ws.conn.WriteMessage(websocket.TextMessage, v)
	if err != nil {
		return fmt.Errorf("wsclt: write text, %w", err)
	}
	return nil
}

func (ws *BaseClient) Pong(v []byte) error {
	// websocket do not support concurrent write
	ws.writeMux.Lock()
	defer ws.writeMux.Unlock()
	if ws.conn == nil {
		return errors.New("wsclt: conn is nil")
	}
	err := ws.conn.WriteMessage(websocket.PongMessage, v)
	if err != nil {
		return fmt.Errorf("wsclt: pong, %w", err)
	}
	return nil
}

func (ws *BaseClient) Sub(newTopics []string) (remainTopics []string, err error) {
	ws.mux.Lock()
	defer ws.mux.Unlock()
	newTopics = ws.filterTopics(newTopics)
	if ws.topicSuber == nil {
		err = errors.New("wsclt: topic suber is nil")
		return
	}
	if len(newTopics) == 0 {
		return []string{}, nil
	}
	oldTopicsLen := len(ws.topics)
	newTopicsLen := len(newTopics)
	topicsCap := ws.maxTopicNum - oldTopicsLen
	canSubTopics := newTopics
	if topicsCap < newTopicsLen {
		canSubTopics = newTopics[:topicsCap]
		remainTopics = newTopics[topicsCap:]
	}
	err = ws.topicSuber(ws, canSubTopics)
	if err != nil {
		err = fmt.Errorf("wsclt: sub topics, %w", err)
	} else {
		ws.topics = append(ws.topics, canSubTopics...)
	}
	return
}

func (ws *BaseClient) Unsub(topics []string) (currentTopics []string, err error) {
	ws.mux.Lock()
	defer ws.mux.Unlock()
	if len(ws.topics) <= 0 {
		return
	}
	if ws.topicUnsuber == nil {
		err = errors.New("wsclt: topic unsuber is nil")
		return
	}
	currentTopics = ws.topics
	err = ws.topicUnsuber(ws, topics)
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

func (ws *BaseClient) filterTopics(newTopics []string) []string {
	return filterTopics(ws.topics, newTopics)
}

func (ws *BaseClient) checkBeforeReading() error {
	if ws.conn == nil {
		return errors.New("wsclt: conn is nil")
	}
	if ws.Status() != ClientStatusConnected {
		return fmt.Errorf("wsclt: status is %v, not connected", ws.status)
	}
	return nil
}

func (ws *BaseClient) handleRedialWork() {
	ws.setStatus(ClientStatusRedial)
	err := ws.CloseConn()
	if err != nil {
		ws.logger.Error("Cannot Close Conn Before Redialing", "err", err)
	}
	if !ws.needRecon {
		return
	}
	go ws.reconnect()
}

func (ws *BaseClient) reconnect() {
	for {
		ws.logger.Info("Reconnecting")
		err := ws.Start()
		if ws.Status() == ClientStatusConnected {
			break
		}
		if err != nil {
			ws.logger.Error("Cannot Reconnect")
			time.Sleep(time.Millisecond * time.Duration(ws.reconWaitingTsMilli))
		}
	}
	if ws.topicSuber != nil {
		// must sub by topicSuber
		err := ws.topicSuber(ws, ws.topics)
		if err != nil {
			ws.logger.Error("Cannot Sub Topics After Reconnecting", "err", err)
			ws.handleRedialWork()
			return
		}
	}
	ws.logger.Info("Reconnected")
}

func (ws *BaseClient) CloseConn() error {
	ws.mux.Lock()
	if ws.ctxCancelFunc != nil {
		ws.ctxCancelFunc()
	}
	defer func() {
		ws.setStatus(ClientStatusNew)
		ws.mux.Unlock()
		// do not set conn nil here
		// ws.conn = nil
	}()
	if ws.conn == nil {
		return nil
	}
	err := ws.conn.Close()
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
	validTopics := []string{}
	for _, topic := range newTopics {
		if mapOldTopics[topic] {
			continue
		}
		validTopics = append(validTopics, topic)
	}
	return validTopics
}

var muxConnCreateTimes = sync.Mutex{}
var wsConnCreateTimes = 0

func createConnId(baseClientId string) string {
	muxConnCreateTimes.Lock()
	wsConnCreateTimes++
	muxConnCreateTimes.Unlock()
	return fmt.Sprintf("ws_conn_id-%v-%v-%v", time.Now().UnixNano(), wsConnCreateTimes, baseClientId)
}

var muxWsCreateTimes = sync.Mutex{}
var wsCreateTimes = 0

func createBaseClientId() string {
	muxWsCreateTimes.Lock()
	wsCreateTimes++
	muxWsCreateTimes.Unlock()
	return fmt.Sprintf("ws_clt_id-%v-%v", time.Now().UnixNano(), wsCreateTimes)
}
