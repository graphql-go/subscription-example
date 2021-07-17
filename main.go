package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/handler"
)

var RootQuery = graphql.NewObject(graphql.ObjectConfig{
	Name: "RootQuery",
	Fields: graphql.Fields{
		"ping": &graphql.Field{
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return "ok", nil
			},
		},
	},
})

type Feed struct {
	ID string `graphql:"id"`
}

var FeedType = graphql.NewObject(graphql.ObjectConfig{
	Name: "FeedType",
	Fields: graphql.Fields{
		"id": &graphql.Field{
			Type: graphql.ID,
		},
	},
})

var RootSubscription = graphql.NewObject(graphql.ObjectConfig{
	Name: "RootSubscription",
	Fields: graphql.Fields{
		"feed": &graphql.Field{
			Type: FeedType,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				log.Printf("[RootSubscription.feed.Resolve] >>>>>>>>>>>>>>> p.Source: %+v", p.Source)
				go func() {
					for {
						select {
						case <-p.Context.Done():
							log.Printf("[Subscription] done-2!")
							return
						}
					}
				}()
				return p.Source, nil
			},
			Subscribe: func(p graphql.ResolveParams) (interface{}, error) {
				c := make(chan interface{})

				go func() {
					var i int

					for {
						feed := Feed{ID: fmt.Sprintf("%d", i)}
						i++
						select {
						case <-p.Context.Done():
							log.Println("[Subscribe Resolver] subscribe channel closed")
							close(c)
							return
						case c <- feed:
						}
						time.Sleep(300 * time.Millisecond)
						if i == 10 {
							close(c)
							return
						}
					}
				}()

				return c, nil
			},
		},
	},
})

var schema graphql.Schema

func main() {
	schemaConfig := graphql.SchemaConfig{
		Query:        RootQuery,
		Subscription: RootSubscription,
	}
	newSchema, err := graphql.NewSchema(schemaConfig)
	if err != nil {
		log.Fatal(err)
	}
	schema = newSchema

	h := handler.New(&handler.Config{
		Schema:     &schema,
		Pretty:     true,
		GraphiQL:   false,
		Playground: true,
	})

	http.Handle("/graphql", h)
	http.HandleFunc("/subscriptions", SubscriptionsHandler)

	log.Println("GraphQL Server running on [POST]: localhost:8081/graphql")

	log.Println("GraphQL Playground running on [GET]: localhost:8081/graphql")

	log.Fatal(http.ListenAndServe(":8081", nil))
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
	Subprotocols: []string{"graphql-ws"},
}

type ConnectionACKMessage struct {
	OperationID string `json:"id,omitempty"`
	Type        string `json:"type"`
	Payload     struct {
		Query string `json:"query"`
	} `json:"payload,omitempty"`
}

type Subscriber struct {
	ID            int
	Conn          *websocket.Conn
	RequestString string
	OperationID   string
}

var subscribers sync.Map

func SubscriptionsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("failed to do websocket upgrade: %v", err)
		return
	}

	connectionACK, err := json.Marshal(map[string]string{
		"type": "connection_ack",
	})
	if err != nil {
		log.Printf("failed to marshal ws connection ack: %v", err)
		return
	}

	if err := conn.WriteMessage(websocket.TextMessage, connectionACK); err != nil {
		log.Printf("failed to write to ws connection: %v", err)
		return
	}

	var startCtxCancel context.CancelFunc

	go func() {
		for {
			_, p, err := conn.ReadMessage()
			if websocket.IsCloseError(err, websocket.CloseGoingAway) {
				log.Println("[Subscription] subscriber closed connection")
				return
			}

			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
				log.Println("[Subscription] subscriber closed connection")
				return
			}

			if err != nil {
				log.Printf("failed to read websocket message: %v", err)
				return
			}

			var msg ConnectionACKMessage
			if err := json.Unmarshal(p, &msg); err != nil {
				log.Printf("failed to unmarshal: %v", err)
				return
			}

			log.Printf("[Subscription] message received: %+v", msg)

			ctx, cancel := context.WithCancel(context.Background())

			go func() {
				for {
					select {
					case <-ctx.Done():
						log.Printf("[Subscription] done-0!")
						return
					}
				}
			}()

			go func() {
				for {
					select {
					case <-ctx.Done():
						log.Printf("[Subscription] done-1!")
						return
					}
				}
			}()

			if msg.Type == "stop" {
				log.Printf("[Subscription] subscriber stopped connection")
				startCtxCancel()
				return
			}

			if msg.Type == "start" {
				length := 0

				subscribers.Range(func(key, value interface{}) bool {
					length++
					return true
				})

				var subscriber = Subscriber{
					ID:            length + 1,
					Conn:          conn,
					RequestString: msg.Payload.Query,
					OperationID:   msg.OperationID,
				}

				subscribers.Store(subscriber.ID, &subscriber)

				log.Printf("[Subscription] subscriber registered, query: %v", msg.Payload.Query)

				sendMessage := func(r *graphql.Result) {
					message, err := json.Marshal(map[string]interface{}{
						"type":    "data",
						"id":      subscriber.OperationID,
						"payload": r.Data,
					})
					if err != nil {
						log.Printf("failed to marshal message: %v", err)
						return
					}

					if err := subscriber.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
						if err == websocket.ErrCloseSent {
							subscribers.Delete(subscriber.ID)
							return
						}

						log.Printf("failed to write to ws connection: %v", err)
						return
					}
				}

				go func() {
					startCtxCancel = cancel

					subscribeParams := graphql.Params{
						Context:       ctx,
						RequestString: msg.Payload.Query,
						Schema:        schema,
					}

					subscribeChannel := graphql.Subscribe(subscribeParams)
					// defer close(subscribeChannel)

					for {
						select {
						case <-ctx.Done():
							log.Println("here 888")
							return
						case r, isOpen := <-subscribeChannel:
							if !isOpen {
								log.Println("subscribeChannel closed")
								return
							}
							sendMessage(r)
						}
					}
				}()
			}
		}
	}()

}
