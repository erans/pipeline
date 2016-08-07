package gcloud

import (
	"fmt"
	"time"

	logrus "gopkg.in/Sirupsen/logrus.v0"

	"github.com/erans/pipeline"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
)

var (
	// Log allows to set a logger to debug the library
	Log *logrus.Logger
)

// PubSubPipeline is the pipeline for use with Google Compute's PubSub
type PubSubPipeline struct {
	projID           string
	queue            string
	resultQueue      string
	subscriptionName string
	handler          pipeline.Handler

	CreateTopics       bool
	CreateSubscription bool

	queueTopic       *pubsub.Topic
	resultQueueTopic *pubsub.Topic
	subscription     *pubsub.Subscription
	client           *pubsub.Client
	ctx              context.Context
}

// NewPubSubPipeline creates a new Google PubSub based pipeline
func NewPubSubPipeline(projID string, queue string, resultQueue string, subscriptionName string, handler pipeline.Handler) *PubSubPipeline {
	return &PubSubPipeline{
		projID:           projID,
		queue:            queue,
		resultQueue:      resultQueue,
		subscriptionName: subscriptionName,
		handler:          handler,

		CreateTopics:       false,
		CreateSubscription: false,
	}
}

func getOrCreateTopic(ctx context.Context, client *pubsub.Client, topicName string, createIfMissing bool) *pubsub.Topic {
	var topic *pubsub.Topic
	var err error
	var exists bool

	topic = client.Topic(topicName)
	if exists, err = topic.Exists(ctx); !exists && createIfMissing {
		if topic, err = client.NewTopic(ctx, topicName); err == nil {
			if Log != nil {
				Log.Debugf("Creating topic: %s\n", topicName)
			}
			return topic
		}
	}

	if Log != nil {
		Log.Debugf("Using topic: %s", topicName)
	}

	return topic
}

func getOrCreateSubscription(ctx context.Context, client *pubsub.Client, name string, topic *pubsub.Topic, createIfMissing bool) *pubsub.Subscription {
	var subscription *pubsub.Subscription
	var err error
	var exists bool

	subscription = client.Subscription(name)
	if exists, err = subscription.Exists(ctx); !exists && createIfMissing {
		if err != nil {
			return nil
		}

		if subscription, err = client.NewSubscription(ctx, name, topic, 120*time.Second, nil); err == nil {
			return subscription
		}

		if Log != nil && err != nil {
			Log.Debugf("Failed to create new subscription: %s", err)
		}

		if Log != nil {
			Log.Debugf("Creating subscription: %s\n", name)
		}
	}

	if Log != nil {
		Log.Debugf("Using subscription: %s", name)
	}

	return subscription
}

func getPipelineMessage(msg *pubsub.Message) *pipeline.Message {
	return &pipeline.Message{
		ID:              msg.ID,
		Data:            msg.Data,
		Attributes:      msg.Attributes,
		InternalMessage: msg,
	}
}

// Start starts the process of getting messages from pubsub
func (p *PubSubPipeline) Start() error {
	var err error
	p.ctx = context.Background()
	p.client, err = pubsub.NewClient(p.ctx, p.projID)
	if err != nil {
		return fmt.Errorf("Failed to create new pubsub client")
	}

	p.queueTopic = getOrCreateTopic(p.ctx, p.client, p.queue, p.CreateTopics)
	if p.resultQueue != "" {
		p.resultQueueTopic = getOrCreateTopic(p.ctx, p.client, p.resultQueue, p.CreateTopics)
	}

	p.subscription = getOrCreateSubscription(p.ctx, p.client, p.subscriptionName, p.queueTopic, p.CreateSubscription)

	it, err := p.subscription.Pull(p.ctx)
	if err != nil {
		return err
	}

	for {
		msg, err := it.Next()
		if err == pubsub.Done {
			break
		}
		if err != nil {
			break
		}

		p.handler.Handle(getPipelineMessage(msg))
	}

	return nil
}

// AckMessage acknolwedges the messages recieved to makring it as processed.
func (p *PubSubPipeline) AckMessage(message *pipeline.Message) error {
	if message.InternalMessage == nil {
		return fmt.Errorf("Message's internal message is nil")
	}
	message.InternalMessage.(*pubsub.Message).Done(true)

	return nil
}

func pipelineMessageToPubSubMessage(message *pipeline.Message) *pubsub.Message {
	return &pubsub.Message{
		Attributes: message.Attributes,
		Data:       message.Data,
	}
}

// PostMessage allows posting a message to another queue, usually for further processing
func (p *PubSubPipeline) PostMessage(outboundTopciName string, message *pipeline.Message) error {
	if _, err := p.resultQueueTopic.Publish(p.ctx, pipelineMessageToPubSubMessage(message)); err != nil {
		return err
	}
	return nil
}
