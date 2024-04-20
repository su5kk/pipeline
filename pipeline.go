package pipeline

import (
	"context"
	"sync"
)

type Executor[T any] interface {
	Execute(ctx context.Context, payload T) error
}

type ExecutorFunc[T any] func(context.Context, T) error

func (f ExecutorFunc[T]) Execute(ctx context.Context, payload T) error {
	return f(ctx, payload)
}

type ErrorHandler[T any] func(ctx context.Context, payload T, err error) (error, bool)

type Strategy int

const (
	StrategySequential Strategy = iota
	StrategyParallel
)

type Pipeline[T any] struct {
	ID string

	executors map[string]Executor[T]
	strategy  Strategy

	q      []string
	groups [][]string

	dependencyGraph map[string][]string

	onError ErrorHandler[T]
}

func New[T any](id string, opts ...Option[T]) *Pipeline[T] {
	p := &Pipeline[T]{
		ID:        id,
		executors: make(map[string]Executor[T]),
		strategy:  StrategySequential,
		groups:    make([][]string, 1),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *Pipeline[T]) AddExecutor(id string, exec Executor[T]) {
	p.executors[id] = exec
	p.groups[0] = append(p.groups[0], id)
	p.sort()
}

// Execute runs the pipeline based on the specified strategy.
func (p *Pipeline[T]) Execute(ctx context.Context, payload T) error {
	switch p.strategy {
	case StrategyParallel:
		return p.parallel(ctx, payload)
	case StrategySequential:
		return p.sequential(ctx, payload)
	}
	return nil
}

func (p *Pipeline[T]) parallel(ctx context.Context, payload T) error {
	var (
		wg   sync.WaitGroup
		errs = make(chan error, 1)
	)

	for _, group := range p.groups {
		for _, execName := range group {
			wg.Add(1)
			go func(e Executor[T]) {
				defer wg.Done()
				err := e.Execute(ctx, payload)
				if err != nil {
					errs <- err
				}
			}(p.executors[execName])
		}
		wg.Wait()
	}

	close(errs)

	for err := range errs {
		if err != nil {
			if p.onError == nil {
				return err
			}

			onErr, cont := p.onError(ctx, payload, err)
			if !cont {
				return onErr
			}
		}
	}

	return nil
}

func (p *Pipeline[T]) sequential(ctx context.Context, payload T) error {
	for _, group := range p.groups {
		for _, execName := range group {
			err := p.executors[execName].Execute(ctx, payload)
			if err != nil {
				if p.onError == nil {
					return err
				}

				onErr, cont := p.onError(ctx, payload, err)
				if !cont {
					return onErr
				}
			}
		}
	}

	return nil
}

func (p *Pipeline[T]) sort() {
	if p.dependencyGraph == nil {
		return
	}

	graph := p.invertGraph()
	indegree := make(map[string]int)

	for node := range graph {
		if _, exists := indegree[node]; !exists {
			indegree[node] = 0
		}
		for _, neighbor := range graph[node] {
			indegree[neighbor]++
		}
	}

	var queue []string
	for node, degree := range indegree {
		if degree == 0 {
			queue = append(queue, node)
		}
	}

	var result [][]string
	for len(queue) > 0 {
		var (
			currentLevel []string
			nextQueue    []string
		)

		for _, node := range queue {
			// current node can be executed since queue contains 0-degree nodes at this point.
			currentLevel = append(currentLevel, node)
			for _, neighbor := range graph[node] {
				// assume that current dependency is checked
				indegree[neighbor]--
				// nextQueue because dependant can't be on the same level
				if indegree[neighbor] == 0 {
					nextQueue = append(nextQueue, neighbor)
				}
			}
		}

		result = append(result, currentLevel)
		queue = nextQueue
	}

	p.groups = result
}

func (p *Pipeline[T]) invertGraph() map[string][]string {
	graph := make(map[string][]string)

	for key := range p.dependencyGraph {
		graph[key] = []string{}
	}

	for node, deps := range p.dependencyGraph {
		for _, dep := range deps {
			graph[dep] = append(graph[dep], node)
		}
	}
	return graph
}

type Option[T any] func(p *Pipeline[T])

// WithDependencyGraph is an option that specifies the executors' dependencies.
// It expects a DAG as an adjacency list.
//
// # Example
//
// a -> b -> d
//
//	-> c -> e
//
// {a: [b, c], b: [d], c: [e]}
func WithDependencyGraph[T any](g map[string][]string) Option[T] {
	return func(p *Pipeline[T]) {
		p.dependencyGraph = g
	}
}

func WithErrorCallback[T any](f ErrorHandler[T]) Option[T] {
	return func(p *Pipeline[T]) {
		p.onError = f
	}
}

func WithExecutionStrategy[T any](strategy Strategy) Option[T] {
	return func(p *Pipeline[T]) {
		p.strategy = strategy
	}
}
