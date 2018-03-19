package actor

//go:generate mockgen -destination=./mock/actor_test.go -package=mock -mock_names=Actor=Actor github.com/AsynkronIT/protoactor-go/actor Actor

import (
	"fmt"
	"reflect"
	"runtime"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/job"
)

type RunnerActor struct{}

type Run struct {
	job *job.Job
}

type NextStep struct {
	job       *job.Job
	i         int
	stepInfos map[string]string
}

func newRunnerActor() actor.Actor {
	return &RunnerActor{}
}

func BuildRunnerActorProps() *actor.Props {
	return actor.FromProducer(newRunnerActor)
}

func (state *RunnerActor) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case *Run:

		//Initialize map Step Status

		//TODO est ce que on stocke stepInfo dans le state ou on le passe dans le message
		//-> voir si il y a un impact lors d'un restart?
		var stepInfos = make(map[string]string)

		for _, step := range msg.job.Steps() {
			//TODO check comment avoir le nom de la fonction
			fmt.Println(stepName(step))
			stepInfos[stepName(step)] = "Iddle"
		}

		i := 0
		context.Self().Tell(&NextStep{msg.job, i, stepInfos})

	case *NextStep:
		var step = msg.job.Steps()[msg.i]
		var infos = msg.stepInfos
		infos[stepName(step)] = "Running"
		context.Parent().Tell(&HeartBeat{infos})

		//TODO changer Step pour que retourne interface{}, error
		//-> Ainsi possibilité de transmettre des infos d'un step à l'autre
		//ça facilitera le découpage en step plus petit
		var _, err = step(nil, nil)

		if err != nil {
			infos[stepName(step)] = "Failed"
			context.Parent().Tell(&HeartBeat{infos})

			context.Parent().Tell(&Status{"Failure", "err"})
			return
		}

		infos[stepName(step)] = "Completed"
		context.Parent().Tell(&HeartBeat{infos})

		var i = msg.i + 1

		if i >= len(msg.job.Steps()) {
			context.Parent().Tell(&Status{"Done", "mess"})
			return
		}

		// TODO transamettre val (le retour de step) dans le message
		// Ajouter val au context car on veut un contexte tout le temps
		context.Self().Tell(&NextStep{msg.job, i, infos})
	}
}

func stepName(s job.Step) string {
	return runtime.FuncForPC(reflect.ValueOf(s).Pointer()).Name()
}
