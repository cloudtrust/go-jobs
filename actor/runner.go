package actor

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/job"
)

const (
	Completed = "COMPLETED"
	Failed    = "FAILED"
)

type RunnerActor struct{}

type Run struct {
	job *job.Job
}

type NextStep struct {
	job       *job.Job
	prevRes   interface{}
	i         int
	stepInfos map[string]string
}

type Failure struct {
	job       *job.Job
	err       error
	stepInfos map[string]string
}

type Success struct {
	job       *job.Job
	result    map[string]string
	stepInfos map[string]string
}

func NewRunnerActor() actor.Actor {
	return &RunnerActor{}
}

func BuildRunnerActorProps() *actor.Props {
	return actor.FromProducer(NewRunnerActor)
}

func (state *RunnerActor) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case actor.Stopped:
		context.Parent().Tell(&RunnerStopped{})
	case *Run:

		//Initialize map Step Status

		var stepInfos = make(map[string]string)

		for _, step := range msg.job.Steps() {
			//TODO check comment avoir le nom de la fonction
			fmt.Println(stepName(step))
			stepInfos[stepName(step)] = "Iddle"
		}

		i := 0
		context.Self().Tell(&NextStep{msg.job, nil, i, stepInfos})

	case *NextStep:
		var step = msg.job.Steps()[msg.i]
		var infos = msg.stepInfos
		var previousRes = msg.prevRes
		infos[stepName(step)] = "Running"
		context.Parent().Tell(&HeartBeat{infos})

		// TODO gérer le passage d'un context en 1° arg
		var res, err = step(nil, previousRes)

		if err != nil {
			infos[stepName(step)] = "Failed"
			context.Parent().Tell(&HeartBeat{infos})
			context.Self().Tell(&Failure{msg.job, err, infos})
			return
		}

		infos[stepName(step)] = "Completed"
		context.Parent().Tell(&HeartBeat{infos})

		var i = msg.i + 1

		if i >= len(msg.job.Steps()) {
			var mapRes, ok = res.(map[string]string)
			if ok {
				context.Self().Tell(&Success{msg.job, mapRes, infos})
			} else {
				//context.Parent().Tell(&Status{Failure, map[string]string{"Reason": "Invalid type result for last step"}})
				err := errors.New("Invalid type result for last step")
				context.Self().Tell(&Failure{msg.job, err, infos})
			}
			return
		}

		// TODO transamettre val (le retour de step) dans le message
		// Ajouter val au context car on veut un contexte tout le temps
		context.Self().Tell(&NextStep{msg.job, res, i, infos})
	case *Failure:
		var result = map[string]string{"Reason": msg.err.Error()}
		var infos = msg.stepInfos

		// if cleanup step exist
		if msg.job.CleanupStep() != nil {
			var cleanStep = msg.job.CleanupStep()
			infos[stepName(cleanStep)] = "Running"
			context.Parent().Tell(&HeartBeat{infos})
			var res, err = cleanStep(nil)

			if err != nil {
				infos[stepName(cleanStep)] = "Failed"
				result["CleanupError"] = err.Error()
			} else {
				infos[stepName(cleanStep)] = "Completed"

				for k, v := range res {
					result[k] = v
				}
			}
		}

		context.Parent().Tell(&Status{Failed, result, infos})

	case *Success:
		context.Parent().Tell(&Status{Completed, msg.result, msg.stepInfos})
	}
}

func stepName(s interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(s).Pointer()).Name()
}
