package main

import (
	"context"
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

	p := pipeline.New[*Payload]("prices")

	p.AddExecutor("milk", pipeline.ExecutorFunc[*Payload](func(ctx context.Context, payload *Payload) error {
		payload.prices["milk"] = 1
		return nil
	}))

	p.AddExecutor("eggs", pipeline.ExecutorFunc[*Payload](func(ctx context.Context, payload *Payload) error {
		payload.prices["eggs"] = 1
		return nil
	}))

	p.AddExecutor("scramble", pipeline.ExecutorFunc[*Payload](func(ctx context.Context, payload *Payload) error {
		payload.prices["scramble"] = payload.prices["eggs"] + payload.prices["milk"]
		return nil
	}))

	ctx := context.Background()
	err := p.Execute(ctx, payload)
	if err != nil {
		panic(err)
	}
	fmt.Println(payload.prices)
}
