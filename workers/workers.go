package main

// IMPORTANT: This is just an idea im trying out!

// go run workers.go

// Result:
// workers: 2017/01/25 14:51:56.345336 processing queue default with 2 workers.
// workers: 2017/01/25 14:51:56.346417 default JID-e9f9662a5af992cc5f331966 start
// workers: 2017/01/25 14:51:56.346433 default JID-e9f9662a5af992cc5f331966 args: ["argument1","argument2"]
// DOING WORK! FROM github.com/jrallison/go-workers
// &{map[jid:e9f9662a5af992cc5f331966 enqueued_at:1.4853739163427367e+09 at:1.4853739163427365e+09 queue:default class:SomeClass args:[argument1 argument2]]}
// workers: 2017/01/25 14:51:56.347070 default JID-e9f9662a5af992cc5f331966 done: 649.285µs
// DOING WORK! FROM github.com/benmanns/goworker
// [argument1 argument2]
// ^Cworkers: 2017/01/25 14:52:15.626833 quitting queue default (waiting for 0 / 2 workers).
// 1485373935626931489 [Info] MacBook-Pro-2.local:47089-0:myqueue shutdown
// 1485373935626987475 [Info] MacBook-Pro-2.local:47089-1:myqueue shutdown
// 1485373935627045084 [Info] MacBook-Pro-2.local:47089-poller:myqueue shutdown

import (
	"fmt"
	"github.com/jrallison/go-workers"
	"github.com/benmanns/goworker"
)

// BaseWorker interface that all other worker interfaces must implement.
type BaseWorker interface {
	Class() string
	Queue() string
}

// GoWorkersWrapper for "github.com/jrallison/go-workers"
type GoWorkersWrapper interface {
	BaseWorker
	Perform(message *workers.Msg)
	Concurrency() int
	Configuration() map[string]string
}

// Concrete implementation of GoWorkersWrapper
type GoWorkersJob struct {
	concurrency     int
	configuration   map[string]string
	class 		string
	queue           string
}

func (worker GoWorkersJob) Configuration() map[string]string {
	return worker.configuration
}

func (worker GoWorkersJob) Perform(message *workers.Msg) {
	fmt.Println("DOING WORK! FROM github.com/jrallison/go-workers")
	fmt.Println(message.Json)
}

func (worker GoWorkersJob) Class() string {
	return worker.class
}

func (worker GoWorkersJob) Queue() string {
	return worker.queue
}

func (worker GoWorkersJob) Concurrency() int {
	return worker.concurrency
}
// End concrete implementation of GoWorkersWrapper


 //Go worker wrapper for "github.com/benmanns/goworker"
type GoWorkerWrapper interface {
	BaseWorker
	Perform(queue string, args ...interface{}) error
	Configuration() goworker.WorkerSettings
}


// Concrete implementation of GoWorkerWrapper
type GoWorkerJob struct {
	queue string
	class string
}

func (worker GoWorkerJob) Class() string {
	return worker.class
}

func (worker GoWorkerJob) Queue() string {
	return worker.queue
}

func (worker GoWorkerJob) Perform(queue string, args ...interface{}) error {
	fmt.Println("DOING WORK! FROM github.com/benmanns/goworker")
	fmt.Println(args)

	return nil
}

func (worker GoWorkerJob) Configuration() goworker.WorkerSettings {
	return goworker.WorkerSettings{
		URI:            "redis://localhost:6379/",
		Connections:    10,
		Queues:         []string{"myqueue"},
		UseNumber:      true,
		ExitOnComplete: false,
		Concurrency:    2,
		Interval:       5.0,
	}
}
// End concrete implementation of GoWorkerWrapper


// PerformAsync is abstract and will perform a job on our wrapped workers.
func PerformAsync(worker BaseWorker, args ...interface{}) {
	switch worker := worker.(type) {
	case GoWorkersWrapper:
		workers.Enqueue(worker.Queue(), worker.Class(), args)
	case GoWorkerWrapper:
		job := &goworker.Job{Queue: worker.Queue(), Payload: goworker.Payload{Class: worker.Class(), Args: args}}
		goworker.Enqueue(job)
	}
}

// Process is abstract and will start the worker process based on our wrapped type.
func Process(worker BaseWorker) {
	switch worker := worker.(type) {
	case GoWorkersWrapper:
		workers.Process(worker.Queue(), worker.Perform, worker.Concurrency())
	case GoWorkerWrapper:
		goworker.Register(worker.Class(), worker.Perform)
	}
}

// Configuration is abstract and will configure the worker based on our wrapped type.
func Configure(worker BaseWorker) {
	switch worker := worker.(type) {
	case GoWorkersWrapper:
		workers.Configure(worker.Configuration())
	case GoWorkerWrapper:
		goworker.SetSettings(worker.Configuration())
	}
}

// Something better here
func RunGoWorkers() {
	workers.Run()
}

// Something better here
func RunGoWorker() {
	goworker.Work()
}

func main() {
	myWorker := GoWorkersJob{queue: "default", class: "SomeClass", concurrency: 2, configuration: map[string]string{"server":  "localhost:6379", "process": "1"}}
	Configure(myWorker)
	Process(myWorker)
	PerformAsync(myWorker, "argument1", "argument2")
	go RunGoWorkers()

	myOtherWorker := GoWorkerJob{queue: "myqueue", class: "SomeClass"}
	Configure(myOtherWorker)
	Process(myOtherWorker)
	PerformAsync(myOtherWorker, "argument1", "argument2")
	RunGoWorker()
}
