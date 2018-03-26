package actor

import (
	"errors"
	"reflect"
	"runtime"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/job"
)

const (
	Completed = "COMPLETED"
	Failed    = "FAILED"
)

// RunnerActor has a state with the job in order to be able to automatically restart it if panic occurs.
type RunnerActor struct {
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

func newRunnerActor(j *job.Job) actor.Actor {
	return &RunnerActor{job: j}
}

func BuildRunnerActorProps(j *job.Job) *actor.Props {
	return actor.FromProducer(func() actor.Actor { return newRunnerActor(j) })
}

func (state *RunnerActor) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case *actor.Stopped:
		context.Parent().Tell(&RunnerStopped{})
	case *actor.Started:
		context.Parent().Tell(&RunnerStarted{})

		//Initialize map Step Status
		var stepInfos = make(map[string]string)

		for _, step := range state.job.Steps() {
			stepInfos[stepName(step)] = "Iddle"
		}

		i := 0
		context.Self().Tell(&NextStep{state.job, nil, i, stepInfos})

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
