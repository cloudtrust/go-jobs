package actor

import (
	"sync"
	"testing"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/cloudtrust/go-jobs/actor/mock"
	"github.com/cloudtrust/go-jobs/job"
	"github.com/golang/mock/gomock"
)

// test nominal

func TestNominalCase(t *testing.T) {
	var wg sync.WaitGroup

	var mockCtrl = gomock.NewController(t)
	defer mockCtrl.Finish()

	var mockLock = mock.NewLock(mockCtrl)
	mockLock.EXPECT().Lock().Return(nil).Times(1)
	mockLock.EXPECT().Unlock().Do(func() { wg.Done() }).Return(nil).Times(1)

	var expectedStepInfos1 = map[string]string{"step1": "Running"}
	var expectedStepInfos2 = map[string]string{"step1": "Completed"}
	var expectedMessage = map[string]string{"Output": "123"}

	var mockStatistics = mock.NewStatistics(mockCtrl)
	mockStatistics.EXPECT().Start().Return(nil).Times(1)
	mockStatistics.EXPECT().Update(expectedStepInfos1).Return(nil).Times(1)
	mockStatistics.EXPECT().Finish(expectedStepInfos2, expectedMessage).Times(1)
	mockStatistics.EXPECT().Cancel(gomock.Any(), gomock.Any()).MaxTimes(0)

	wg.Add(1)

	var job, _ = job.NewJob("job", job.Steps(successfulStep))

	props := actor.FromProducer(NewMasterActor(workerProducerBuilder(mockNewWorkerActor)))
	master := actor.Spawn(props)

	master.Tell(&RegisterJob{})

	wg.Wait()
	master.GracefulStop()
}

func mockNewWorkerActor(j *job.Job, l Lock, s Statistics, options ...WorkerOption) func() actor.Actor{

}

// test panic restart

//test suicide
