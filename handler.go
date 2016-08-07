package pipeline

// Handler provides a handle for various pipeling mechanism and
// settings
type Handler interface {
	Init(p Pipeline)
	Handle(message *Message) error
}
