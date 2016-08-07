package pipeline

import (
	"gopkg.in/Sirupsen/logrus.v0"
)

var (
	// Log allows attaching a logger to the library
	Log *logrus.Logger
)

// Pipeline interface to communicate with various implementations
type Pipeline interface {
	Start() error
	Stop()
	AckMessage(message *Message) error
	PostMessage(outboundTopciName string, message *Message) error
}
