package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
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
	loop := 1
	for loop < 10 {
		loop += 1
		fmt.Printf("jobloop %c\n", loop)
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Println("Job1 finished")
	return nil
}

func job2Func(context.Context, interface{}) error {
	fmt.Println("Job2")
	loop := 1
	for loop < 10 {
		loop += 1
		fmt.Printf("jobloop %c\n", loop)
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Println("Job2 finished")
	return nil
}

func job3Func(context.Context, interface{}) error {
	fmt.Println("Job3")
	loop := 1
	for loop < 10 {
		loop += 1
		fmt.Printf("jobloop %c\n", loop)
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Println("Job3 finished")
	return nil
}

func main() {
	// var job, _ = New("test")
	// job.AddSteps(step1, step2)
	// job.BeforeEachStep(logging, heartbeat)
	// job.AfterEachStep(logging)
	//job.Run()

	var job, err = NewJob("Job1", Steps(job1Func, job2Func, job3Func), ExecutionTimeout(1 * time.Second))


	
	if err != nil {
		panic(err)
	}

	var scheduler, _ = NewScheduler()

	scheduler.Register("test1", job)
	scheduler.AddTask("0/10 * * * * *", "test1")
	scheduler.Start()
	// scheduler.Register(job)
	// scheduler.AddTask("@minutely", job.Id)
	// scheduler.AddTask("0 0 0 * * *", job.Id)

	// Critical errors channel.
	var errc = make(chan error)
	go func() {
		var c = make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errc <- fmt.Errorf("%s", <-c)
	}()
	fmt.Println(<-errc)
}
