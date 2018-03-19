package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func step1Func(context.Context, interface{}) (interface{}, error) {
	fmt.Println("Step1")
	loop := 1
	for loop < 10 {
		loop += 1
		fmt.Printf("jobloop %c\n", loop)
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Println("Step1 finished")
	return nil, nil
}

func step2Func(context.Context, interface{}) (interface{}, error) {
	fmt.Println("Step2")
	loop := 1
	for loop < 10 {
		loop += 1
		fmt.Printf("jobloop %c\n", loop)
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Println("Step2 finished")
	return nil, nil
}

func step3Func(context.Context, interface{}) (interface{}, error) {
	fmt.Println("Step3")
	loop := 1
	for loop < 10 {
		loop += 1
		fmt.Printf("jobloop %c\n", loop)
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Println("Step3 finished")
	return nil, nil
}

func main() {
	// var job, _ = New("test")
	// job.AddSteps(step1, step2)
	// job.BeforeEachStep(logging, heartbeat)
	// job.AfterEachStep(logging)
	//job.Run()

	var job, err = NewJob("Job1", Steps(step1Func, step2Func, step3Func))

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
