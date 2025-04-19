package domain

import "time"

type NixEventId string

type NixEventType string

type NixNewEvent struct {
	Id        NixEventId     `json:"id,omitempty"`
	Type      NixEventType   `json:"type"`
	Payload   map[string]any `json:"payload"`
	CreatedAt *time.Time     `json:"created_at,omitempty"`
	UpdatedAt *time.Time     `json:"updated_at,omitempty"`
}

type NixEvent struct {
	Id        NixEventId     `json:"id"`
	Type      NixEventType   `json:"type"`
	Payload   map[string]any `json:"payload"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
}
