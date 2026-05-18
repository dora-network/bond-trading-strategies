package strategy

type (
	Message uint8
)

const (
	Stop Message = iota
	Pause
	Resume
)
