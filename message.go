package pipeline

// Message abstract a pipeline message structure
type Message struct {
	ID              string
	Data            []byte
	Attributes      map[string]string
	InternalMessage interface{}
}
