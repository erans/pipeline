package gcloud

import (
	"fmt"
	"os"
	"testing"

	"github.com/erans/pipeline"

	logrus "gopkg.in/Sirupsen/logrus.v0"
)

type testHandler struct {
	p pipeline.Pipeline
}

func (h *testHandler) Init(p pipeline.Pipeline) {
	//h.p = p
}

func (h *testHandler) Handle(message *pipeline.Message) error {
	fmt.Printf("Got Msg: %s %v\n", message.ID, message)
	h.p.AckMessage(message)
	fmt.Printf("Message Acked: %s %v\n", message.ID, message)
	return nil
}

func TestPubSubPipeline(t *testing.T) {
	queueName := "test123"
	subscriptionName := "test.test123"
	projectID := "test-test-1"

	th := &testHandler{}

	Log = logrus.New()
	logrus.SetLevel(logrus.DebugLevel)

	Log.Out = os.Stderr
	Log.Level = logrus.DebugLevel

	var p pipeline.Pipeline
	p = NewPubSubPipeline(&projectID, &queueName, nil, &subscriptionName, th)
	original, ok := p.(*PubSubPipeline)
	if ok {
		original.CreateSubscription = true
		original.CreateTopics = true
	}

	th.p = p

	//th.Init(p)
	err := p.Start()
	if err != nil {
		fmt.Printf("%s", err)
		t.Fail()
	}
}
