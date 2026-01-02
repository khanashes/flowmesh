package handlers

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/flowmesh/engine/internal/logger"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	// Time allowed to write a message to the peer
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer
	pongWait = 60 * time.Second

	// Send pings to peer with this period (must be less than pongWait)
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer
	maxMessageSize = 512 * 1024 // 512KB
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// Allow all origins for development
		// In production, validate origin
		return true
	},
}

// Client represents a WebSocket client connection
type Client struct {
	hub    *Hub
	conn   *websocket.Conn
	send   chan []byte
	topics map[string]bool // Subscribed topics
	mu     sync.RWMutex
	log    zerolog.Logger
}

// Hub maintains the set of active clients and broadcasts messages to clients
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan *WSMessage
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
	log        zerolog.Logger
}

// WSMessage represents a WebSocket message
type WSMessage struct {
	Type    string          `json:"type"`
	Topic   string          `json:"topic,omitempty"`
	Payload json.RawMessage `json:"payload"`
}

// NewHub creates a new WebSocket hub
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan *WSMessage, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		log:        logger.WithComponent("websocket.hub"),
	}
}

// Run starts the hub's main loop
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			h.log.Debug().Int("clients", len(h.clients)).Msg("Client registered")

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()
			h.log.Debug().Int("clients", len(h.clients)).Msg("Client unregistered")

		case message := <-h.broadcast:
			// Collect clients that need to be removed (those with full send buffers)
			var clientsToRemove []*Client

			h.mu.RLock()
			for client := range h.clients {
				// Check if client is subscribed to this topic
				client.mu.RLock()
				subscribed := len(client.topics) == 0 || client.topics[message.Topic]
				client.mu.RUnlock()

				if subscribed {
					select {
					case client.send <- h.messageToBytes(message):
					default:
						// Client's send buffer is full, mark for removal
						clientsToRemove = append(clientsToRemove, client)
					}
				}
			}
			h.mu.RUnlock()

			// Remove clients that had full send buffers (requires write lock)
			if len(clientsToRemove) > 0 {
				h.mu.Lock()
				for _, client := range clientsToRemove {
					if _, ok := h.clients[client]; ok {
						delete(h.clients, client)
						close(client.send)
					}
				}
				h.mu.Unlock()
			}
		}
	}
}

// Broadcast sends a message to all subscribed clients
func (h *Hub) Broadcast(msg *WSMessage) {
	select {
	case h.broadcast <- msg:
	default:
		h.log.Warn().Msg("Broadcast channel full, dropping message")
	}
}

// messageToBytes converts a WSMessage to JSON bytes
func (h *Hub) messageToBytes(msg *WSMessage) []byte {
	data, err := json.Marshal(msg)
	if err != nil {
		h.log.Error().Err(err).Msg("Failed to marshal WebSocket message")
		return nil
	}
	return data
}

// readPump pumps messages from the WebSocket connection to the hub
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		if err := c.conn.Close(); err != nil {
			// Ignore close errors in defer
		}
	}()

	if err := c.conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
		c.log.Warn().Err(err).Msg("Failed to set read deadline")
	}
	c.conn.SetPongHandler(func(string) error {
		if err := c.conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
			c.log.Warn().Err(err).Msg("Failed to set read deadline in pong handler")
		}
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.log.Error().Err(err).Msg("WebSocket error")
			}
			break
		}

		// Handle incoming message (e.g., subscription changes)
		var msg WSMessage
		if err := json.Unmarshal(message, &msg); err == nil {
			c.handleMessage(&msg)
		}
	}
}

// writePump pumps messages from the hub to the WebSocket connection
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		if err := c.conn.Close(); err != nil {
			// Ignore close errors in defer
		}
	}()

	for {
		select {
		case message, ok := <-c.send:
			if err := c.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
				return
			}
			if !ok {
				if err := c.conn.WriteMessage(websocket.CloseMessage, []byte{}); err != nil {
					c.log.Warn().Err(err).Msg("Failed to write close message")
				}
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			if _, err := w.Write(message); err != nil {
				_ = w.Close()
				return
			}

			// Add queued messages to the current WebSocket message
			n := len(c.send)
			for i := 0; i < n; i++ {
				if _, err := w.Write([]byte{'\n'}); err != nil {
					_ = w.Close()
					return
				}
				if _, err := w.Write(<-c.send); err != nil {
					_ = w.Close()
					return
				}
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			if err := c.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
				return
			}
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// handleMessage processes incoming client messages
func (c *Client) handleMessage(msg *WSMessage) {
	switch msg.Type {
	case "subscribe":
		// Subscribe to a topic
		var payload struct {
			Topics []string `json:"topics"`
		}
		if err := json.Unmarshal(msg.Payload, &payload); err == nil {
			c.mu.Lock()
			if c.topics == nil {
				c.topics = make(map[string]bool)
			}
			for _, topic := range payload.Topics {
				c.topics[topic] = true
			}
			c.mu.Unlock()
			c.log.Debug().Strs("topics", payload.Topics).Msg("Client subscribed to topics")
		}

	case "unsubscribe":
		// Unsubscribe from a topic
		var payload struct {
			Topics []string `json:"topics"`
		}
		if err := json.Unmarshal(msg.Payload, &payload); err == nil {
			c.mu.Lock()
			for _, topic := range payload.Topics {
				delete(c.topics, topic)
			}
			c.mu.Unlock()
			c.log.Debug().Strs("topics", payload.Topics).Msg("Client unsubscribed from topics")
		}
	}
}

// ServeWebSocket handles WebSocket requests from clients
func ServeWebSocket(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log := logger.WithComponent("websocket")
		log.Error().Err(err).Msg("Failed to upgrade connection")
		return
	}

	client := &Client{
		hub:    hub,
		conn:   conn,
		send:   make(chan []byte, 256),
		topics: make(map[string]bool),
		log:    logger.WithComponent("websocket.client"),
	}

	client.hub.register <- client

	// Start client goroutines
	go client.writePump()
	go client.readPump()
}

// BroadcastStreamStats broadcasts stream stats updates
func (h *Hub) BroadcastStreamStats(tenant, namespace, name string, stats interface{}) {
	if h == nil {
		return
	}
	payload, err := json.Marshal(stats)
	if err != nil {
		h.log.Error().Err(err).Msg("Failed to marshal stream stats")
		return
	}
	h.Broadcast(&WSMessage{
		Type:    "stream.stats",
		Topic:   "stream.stats." + tenant + "/" + namespace + "/" + name,
		Payload: payload,
	})
}

// BroadcastQueueStats broadcasts queue stats updates
func (h *Hub) BroadcastQueueStats(tenant, namespace, name string, stats interface{}) {
	if h == nil {
		return
	}
	payload, err := json.Marshal(stats)
	if err != nil {
		h.log.Error().Err(err).Msg("Failed to marshal queue stats")
		return
	}
	h.Broadcast(&WSMessage{
		Type:    "queue.stats",
		Topic:   "queue.stats." + tenant + "/" + namespace + "/" + name,
		Payload: payload,
	})
}

// BroadcastReplaySession broadcasts replay session updates
func (h *Hub) BroadcastReplaySession(sessionID string, session interface{}) {
	if h == nil {
		return
	}
	payload, err := json.Marshal(session)
	if err != nil {
		h.log.Error().Err(err).Msg("Failed to marshal replay session")
		return
	}
	h.Broadcast(&WSMessage{
		Type:    "replay.session",
		Topic:   "replay.session." + sessionID,
		Payload: payload,
	})
}

// BroadcastKVUpdate broadcasts KV store updates
func (h *Hub) BroadcastKVUpdate(tenant, namespace, name string, update interface{}) {
	if h == nil {
		return
	}
	payload, err := json.Marshal(update)
	if err != nil {
		h.log.Error().Err(err).Msg("Failed to marshal KV update")
		return
	}
	h.Broadcast(&WSMessage{
		Type:    "kv.update",
		Topic:   "kv.update." + tenant + "/" + namespace + "/" + name,
		Payload: payload,
	})
}
