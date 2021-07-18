package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/handler"
)

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
							log.Println("[RootSubscription] [Subscribe] subscription canceled")
							close(c)
							return
						default:
							c <- feed
						}
						time.Sleep(300 * time.Millisecond)
						if i == 140 {
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
	UUID          string
	Conn          *websocket.Conn
	RequestString string
	OperationID   string
}

var subscribers sync.Map

func subscribersSize() uint64 {
	var size uint64
	subscribers.Range(func(_, _ interface{}) bool {
		size++
		return true
	})
	return size
}

func SubscriptionsHandler(w http.ResponseWriter, r *http.Request) {

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("failed to do websocket upgrade: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	connectionACK, err := json.Marshal(map[string]string{
		"type": "connection_ack",
	})
	if err != nil {
		log.Printf("failed to marshal ws connection ack: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if err := conn.WriteMessage(websocket.TextMessage, connectionACK); err != nil {
		log.Printf("failed to write to ws connection: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	go func() {
		subscriptionCtx, subscriptionCancelFn := context.WithCancel(context.Background())

		var subscriber *Subscriber

		unsubscribe := func() {
			subscriptionCancelFn()
			if subscriber != nil {
				subscriber.Conn.Close()
				subscribers.Delete(subscriber.UUID)
			}
			log.Printf("[SubscriptionsHandler] subscribers size: %+v", subscribersSize())
		}

		for {
			_, p, err := conn.ReadMessage()
			if websocket.IsCloseError(err, websocket.CloseGoingAway) {
				log.Println("[SubscriptionsHandler] subscriber closed connection")
				unsubscribe()
				return
			}

			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
				log.Println("[SubscriptionsHandler] subscriber closed connection")
				unsubscribe()
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

			log.Printf("[SubscriptionsHandler] msg.Type: %v", msg.Type)

			if msg.Type == "stop" {
				log.Printf("[SubscriptionsHandler] subscriber stopped connection")
				unsubscribe()
				return
			}

			if msg.Type == "start" {
				subscriber = &Subscriber{
					UUID:          uuid.New().String(),
					Conn:          conn,
					RequestString: msg.Payload.Query,
					OperationID:   msg.OperationID,
				}
				subscribers.Store(subscriber.UUID, &subscriber)

				log.Printf("[SubscriptionsHandler] subscribers size: %+v", subscribersSize())

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
							unsubscribe()
							return
						}

						log.Printf("failed to write to ws connection: %v", err)
						return
					}
				}

				go func() {
					subscribeParams := graphql.Params{
						Context:       subscriptionCtx,
						RequestString: msg.Payload.Query,
						Schema:        schema,
					}

					subscribeChannel := graphql.Subscribe(subscribeParams)

					for {
						select {
						case <-subscriptionCtx.Done():
							return
						case r, isOpen := <-subscribeChannel:
							if !isOpen {
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
