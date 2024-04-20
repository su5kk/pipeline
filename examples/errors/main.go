package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/su5kk/pipeline"
)

type Payload struct {
	prices map[string]float64
}

func main() {
	payload := &Payload{
		prices: make(map[string]float64),
	}

	p := pipeline.New[*Payload]("prices",
		pipeline.WithErrorCallback[*Payload](func(ctx context.Context, payload *Payload, err error) (error, bool) {
			fmt.Printf("payload: %+v\nerror: %v\n", payload, err)
			return nil, true
		}),
	)

	// wrong order
	p.AddExecutor("scramble", pipeline.ExecutorFunc[*Payload](func(ctx context.Context, payload *Payload) error {
		if _, ok := payload.prices["eggs"]; !ok {
			return errors.New("eggs not found")
		}
		if _, ok := payload.prices["milk"]; !ok {
			return errors.New("milk not found")
		}

		payload.prices["scramble"] = payload.prices["eggs"] + payload.prices["milk"]
		return nil
	}))

	p.AddExecutor("milk", pipeline.ExecutorFunc[*Payload](func(ctx context.Context, payload *Payload) error {
		payload.prices["milk"] = 1
		return nil
	}))

	p.AddExecutor("eggs", pipeline.ExecutorFunc[*Payload](func(ctx context.Context, payload *Payload) error {
		payload.prices["eggs"] = 1
		return nil
	}))

	ctx := context.Background()
	err := p.Execute(ctx, payload)
	if err != nil {
		panic(err)
	}
	fmt.Println(payload.prices)
}
