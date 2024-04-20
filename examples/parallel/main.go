package main

import (
	"context"
	"fmt"
	"github.com/su5kk/pipeline"
	"sync"
)

type Payload struct {
	sync.RWMutex
	prices map[string]float64
}

func main() {
	payload := &Payload{
		prices: make(map[string]float64),
	}

	g := map[string][]string{
		"db":        {},
		"eggs":      {"db"},
		"milk":      {"db"},
		"scramble":  {"eggs", "milk"},
		"cereal":    {"milk"},
		"breakfast": {"scramble", "cereal"},
		"carrots":   {"db"},
		"cabbages":  {"db"},
	}

	p := pipeline.New[*Payload]("prices",
		pipeline.WithDependencyGraph[*Payload](g),
		pipeline.WithExecutionStrategy[*Payload](pipeline.StrategyParallel),
	)

	p.AddExecutor("scramble", pipeline.ExecutorFunc[*Payload](func(ctx context.Context, payload *Payload) error {
		payload.Lock()
		defer payload.Unlock()
		payload.prices["scramble"] = payload.prices["eggs"] + payload.prices["milk"]
		return nil
	}))

	p.AddExecutor("milk", pipeline.ExecutorFunc[*Payload](func(ctx context.Context, payload *Payload) error {
		payload.Lock()
		defer payload.Unlock()
		payload.prices["milk"] = 1
		return nil
	}))

	p.AddExecutor("eggs", pipeline.ExecutorFunc[*Payload](func(ctx context.Context, payload *Payload) error {
		payload.Lock()
		defer payload.Unlock()
		payload.prices["eggs"] = 1
		return nil
	}))

	p.AddExecutor("db", pipeline.ExecutorFunc[*Payload](func(ctx context.Context, payload *Payload) error {
		payload.Lock()
		defer payload.Unlock()
		payload.prices = map[string]float64{
			"eggs": 0,
			"milk": 0,
		}
		return nil
	}))

	p.AddExecutor("cereal", pipeline.ExecutorFunc[*Payload](func(ctx context.Context, payload *Payload) error {
		payload.Lock()
		defer payload.Unlock()
		payload.prices["cereal"] = payload.prices["milk"] + 1
		return nil
	}))

	p.AddExecutor("breakfast", pipeline.ExecutorFunc[*Payload](func(ctx context.Context, payload *Payload) error {
		payload.Lock()
		defer payload.Unlock()
		payload.prices["breakfast"] = payload.prices["cereal"] + payload.prices["scramble"]
		return nil
	}))

	p.AddExecutor("carrots", pipeline.ExecutorFunc[*Payload](func(ctx context.Context, payload *Payload) error {
		payload.Lock()
		defer payload.Unlock()
		payload.prices["carrots"] = 1
		return nil
	}))

	p.AddExecutor("cabbages", pipeline.ExecutorFunc[*Payload](func(ctx context.Context, payload *Payload) error {
		payload.Lock()
		defer payload.Unlock()
		payload.prices["cabbages"] = 1
		return nil
	}))

	ctx := context.Background()
	err := p.Execute(ctx, payload)
	if err != nil {
		panic(err)
	}
	fmt.Println(payload.prices)
}
