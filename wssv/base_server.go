package wssv

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Msg[Event any, Topic any, Data any] struct {
	Event Event  `json:"event" bson:"event"`
	Topic Topic  `json:"topic" bson:"topic"`
	Data  Data   `json:"data" bson:"data"`
	Time  int64  `json:"time" bson:"time"`
	Code  int64  `json:"code" bson:"code"`
	Msg   string `json:"msg" bson:"msg"`
}

type Client struct {
	writeMux     sync.Mutex
	statusMux    sync.Mutex
	ServerPort   uint64
	ID           string
	Request      *http.Request
	Conn         *websocket.Conn
	StartTsMilli int64
	EndTsMilli   int64
}

func NewClient(serverId string, serverPort uint64, req *http.Request, conn *websocket.Conn) *Client {
	return &Client{
		ServerPort:   serverPort,
		ID:           newCltId(serverId),
		Request:      req,
		Conn:         conn,
		StartTsMilli: time.Now().UnixNano(),
		EndTsMilli:   0,
	}
}

func (c *Client) WriteJSON(v any) error {
	c.writeMux.Lock()
	defer c.writeMux.Unlock()
	err := c.Conn.WriteJSON(v)
	if err != nil {
		c.close(err)
	}
	return err
}

func (c *Client) WriteText(data []byte) error {
	c.writeMux.Lock()
	defer c.writeMux.Unlock()
	err := c.Conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		c.close(err)
	}
	return err
}

func (c *Client) close(err error) {
	c.statusMux.Lock()
	defer c.statusMux.Unlock()
	if c.EndTsMilli > 0 {
		return
	}
	closeErr := c.Conn.Close()
	if closeErr != nil {
		slog.Error("Cannot Close", "cltId", c.ID)
	}
	c.EndTsMilli = time.Now().UnixMilli()
}

func (c *Client) Closed() bool {
	c.statusMux.Lock()
	defer c.statusMux.Unlock()
	return c.EndTsMilli > 0
}

type ConHandler func(server *Server, client *Client)
type MsgHandler func(server *Server, client *Client, messageType int, msg []byte, err error)

type Server struct {
	id         string
	port       uint64
	conHandler ConHandler
	msgHandler MsgHandler

	logger *slog.Logger
}

func NewServer(port uint64, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stdout, nil))
	}
	id := fmt.Sprintf("server_id_%v", time.Now().UnixNano())
	logger = logger.With("server_id", id)
	return &Server{
		id:     id,
		port:   port,
		logger: logger,
	}
}

func (s *Server) Start(conHandler ConHandler, msgHandler MsgHandler) error {
	s.conHandler = conHandler
	s.msgHandler = msgHandler
	http.HandleFunc("/", s.serverEcho)
	s.logger.Info("Server Running", "port", s.port)
	err := http.ListenAndServe(fmt.Sprintf(":%v", s.port), nil)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) serverEcho(w http.ResponseWriter, r *http.Request) {
	logger := s.logger.With("addr", r.RemoteAddr)
	logger.Info("New Connection")
	upgrader := websocket.Upgrader{}
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Error("Cannot Upgrade", "err", err)
		w.WriteHeader(http.StatusBadRequest)
		_, err = w.Write([]byte("Cannot Upgrade"))
		if err != nil {
			logger.Error("Cannot Write", "err", err)
		}
		return
	}
	client := NewClient(s.id, s.port, r, c)
	if s.conHandler != nil {
		s.conHandler(s, client)
	}
	for {
		mt, msg, err := c.ReadMessage()
		s.msgHandler(s, client, mt, msg, err)
		if err != nil {
			client.close(err)
			break
		}
	}
}

var muxCltCreateNum sync.Mutex
var cltCreateNum int

func newCltId(serverId string) string {
	muxCltCreateNum.Lock()
	cltCreateNum++
	muxCltCreateNum.Unlock()
	return fmt.Sprintf("ws_server_client-%v-%v-%v", time.Now().UnixNano(), cltCreateNum, serverId)
}
