package main

import (
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

func main() {
	var job, _ = New("test")
	job.AddSteps(step1, step2)
	job.BeforeEachStep(logging, heartbeat)
	job.AfterEachStep(logging)
	//job.Run()

	var scheduler, _ = NewScheduler()
	scheduler.AddTask("@minutely", job)
	scheduler.Start()

	console.ReadLine()
}
