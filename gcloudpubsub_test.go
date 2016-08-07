package pipeline

import (
	"fmt"
	"os"
	"testing"

	"github.com/erans/pipeline/gcloud"

	logrus "gopkg.in/Sirupsen/logrus.v0"
)

type testHandler struct {
	p *gcloud.PubSubPipeline
}

func (h *testHandler) Init(p Pipeline) {
	//h.p = p
}

func (h *testHandler) Handle(message *Message) error {
	fmt.Printf("Got Msg: %s %v\n", message.ID, message)
	h.p.AckMessage(message)
	fmt.Printf("Message Acked: %s %v\n", message.ID, message)
	return nil
}

func TestPubSubPipeline(t *testing.T) {
	queueName := "test123"
	subscriptionName := "test.test123"
	projectID := "genuine-space-390"

	th := &testHandler{}

	Log = logrus.New()
	logrus.SetLevel(logrus.DebugLevel)

	Log.Out = os.Stderr
	Log.Level = logrus.DebugLevel

	p := NewPubSubPipeline(projectID, queueName, "", subscriptionName, th)
	p.CreateSubscription = true
	p.CreateTopics = true

	th.p = p

	//th.Init(p)
	err := p.Start()
	if err != nil {
		fmt.Printf("%s", err)
		t.Fail()
	}
}
