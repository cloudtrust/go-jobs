package main

import (
	"context"
	"fmt"

	"github.com/AsynkronIT/goconsole"
)

func step1() {
	fmt.Println("*** step1 ***")
}

func step2() {
	fmt.Println("*** step2 ***")
}

func logging() {
	fmt.Println("logging")
}

func heartbeat() {
	fmt.Println("heartbeat")
}

func job1Func(context.Context, interface{}) error {
	fmt.Println("Job1")
	return nil
}

func main() {
	// var job, _ = New("test")
	// job.AddSteps(step1, step2)
	// job.BeforeEachStep(logging, heartbeat)
	// job.AfterEachStep(logging)
	//job.Run()

	var job = NewJob("job1", job1Func)

	var scheduler, _ = NewScheduler()
	scheduler.Register("test1", job)
	scheduler.AddTask("*/10 * * * * *", "test1")
	scheduler.Start()

	// scheduler.Register(job)
	// scheduler.AddTask("@minutely", job.Id)
	// scheduler.AddTask("0 0 0 * * *", job.Id)

	console.ReadLine()
}
