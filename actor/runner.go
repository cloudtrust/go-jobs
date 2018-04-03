package actor

import (
	"errors"
	"reflect"
	"runtime"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/job"
)

// Job execution status
const (
	Completed = "COMPLETED"
	Failed    = "FAILED"
)

// RunnerActor is the actor in charge of the job execution
// It has a state with the job in order to be able to automatically restart it if panic occurs.
type RunnerActor struct {
	logger Logger
	jobID  string
	job    *job.Job
}

type nextStep struct {
	job       *job.Job
	prevRes   interface{}
	i         int
	stepInfos map[string]string
}

type failure struct {
	job       *job.Job
	err       error
	stepInfos map[string]string
}

type success struct {
	job       *job.Job
	result    map[string]string
	stepInfos map[string]string
}

func newRunnerActor(logger Logger, jobID string, j *job.Job) actor.Actor {
	return &RunnerActor{logger: logger, jobID: jobID, job: j}
}

// BuildRunnerActorProps build the Properties for the actor spawning.
func BuildRunnerActorProps(logger Logger, jobID string, j *job.Job) *actor.Props {
	return actor.FromProducer(func() actor.Actor { return newRunnerActor(logger, jobID, j) })
}

// Receive is the implementation of RunnerActor's behavior
func (state *RunnerActor) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case *actor.Stopped:
		context.Parent().Tell(&RunnerStopped{})
	case *actor.Started:
		context.Parent().Tell(&RunnerStarted{})

		//Initialize map Step Status
		var stepInfos = make(map[string]string)

		for _, step := range state.job.Steps() {
			stepInfos[stepName(step)] = "IDdle"
		}

		i := 0
		context.Self().Tell(&nextStep{state.job, nil, i, stepInfos})

	case *nextStep:
		var step = msg.job.Steps()[msg.i]
		var infos = msg.stepInfos
		var previousRes = msg.prevRes
		infos[stepName(step)] = "Running"
		context.Parent().Tell(&HeartBeat{state.jobID, infos})

		// TODO gérer le passage d'un context en 1° arg
		var res, err = step(nil, previousRes)

		if err != nil {
			infos[stepName(step)] = "Failed"
			context.Parent().Tell(&HeartBeat{state.jobID, infos})
			context.Self().Tell(&failure{msg.job, err, infos})
			return
		}

		infos[stepName(step)] = "Completed"
		context.Parent().Tell(&HeartBeat{state.jobID, infos})

		var i = msg.i + 1

		if i >= len(msg.job.Steps()) {
			var mapRes, ok = res.(map[string]string)
			if ok {
				context.Self().Tell(&success{msg.job, mapRes, infos})
			} else {
				err := errors.New("Invalid type result for last step")
				context.Self().Tell(&failure{msg.job, err, infos})
			}
			return
		}

		// TODO transamettre val (le retour de step) dans le message
		// Ajouter val au context car on veut un contexte tout le temps
		context.Self().Tell(&nextStep{msg.job, res, i, infos})
	case *failure:
		var result = map[string]string{"Reason": msg.err.Error()}
		var infos = msg.stepInfos

		// if cleanup step exist
		if msg.job.CleanupStep() != nil {
			var cleanStep = msg.job.CleanupStep()
			infos[stepName(cleanStep)] = "Running"
			context.Parent().Tell(&HeartBeat{state.jobID, infos})
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

		context.Parent().Tell(&Status{state.jobID, Failed, result, infos})

	case *success:
		context.Parent().Tell(&Status{state.jobID, Completed, msg.result, msg.stepInfos})
	}
}

func stepName(s interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(s).Pointer()).Name()
}
