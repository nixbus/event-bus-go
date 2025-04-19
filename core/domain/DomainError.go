package domain

func NewDomainError(message string) error {
	return DomainError{message: message}
}

type DomainError struct {
	message string
}

func (e DomainError) Error() string {
	return e.message
}
