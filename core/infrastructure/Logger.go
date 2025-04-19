package infrastructure

import (
	"encoding/json"
	"fmt"
	"log"
)

type LogLevel struct {
	Level string `json:"level,omitempty"`
}

type Logger interface {
	Info(namespace string, name string, data map[string]any)
	Error(namespace string, name string, data map[string]any)
	Debug(namespace string, name string, data map[string]any)
}

const (
	LogLevelDebug = 0
	LogLevelInfo  = 1
	LogLevelError = 2
)

type LoggerImpl struct {
	level int
}

func NewLogger(options LogLevel) *LoggerImpl {
	return &LoggerImpl{
		level: toLogLevel(options.Level),
	}
}

func (l *LoggerImpl) Info(namespace string, name string, data map[string]any) {
	if LogLevelInfo >= l.level {
		log.Println(formatPrefix(namespace, name), serialize(data))
	}
}

func (l *LoggerImpl) Error(namespace string, name string, data map[string]any) {
	if LogLevelError >= l.level {
		log.Println(formatPrefix(namespace, name), serialize(data))
	}
}

func (l *LoggerImpl) Debug(namespace string, name string, data map[string]any) {
	if LogLevelDebug >= l.level {
		log.Println(formatPrefix(namespace, name), serialize(data))
	}
}

func toLogLevel(level string) int {
	if level == "info" {
		return LogLevelInfo
	}
	if level == "error" {
		return LogLevelError
	}
	if level == "debug" {
		return LogLevelDebug
	}
	return 100
}

func formatPrefix(namespace string, name string) string {
	return fmt.Sprintf("[%s][%s]", namespace, name)
}

// serialize serializes data to a string
func serialize(data map[string]any) string {
	bytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Sprintf("%v", data)
	}
	return string(bytes)
}
