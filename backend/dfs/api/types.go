// Package api has type definitions for the parts of the Discord API
// used by the dfs backend.
package api

import "fmt"

// ChannelTypePrivateThread is the Discord channel type of a private thread
const ChannelTypePrivateThread = 12

// Error is returned by the Discord API on failure
type Error struct {
	Code       int     `json:"code"`
	Message    string  `json:"message"`
	RetryAfter float64 `json:"retry_after"` // seconds, only set on 429s
	Global     bool    `json:"global"`
	Status     int     `json:"-"`
}

// Error satisfies the error interface
func (e *Error) Error() string {
	return fmt.Sprintf("discord: %s (code %d, status %d)", e.Message, e.Code, e.Status)
}

// Channel is a Discord channel or thread
type Channel struct {
	ID string `json:"id"`
}

// ThreadCreate is the body for starting a thread without a message
type ThreadCreate struct {
	Name                string `json:"name"`
	Type                int    `json:"type"`
	AutoArchiveDuration int    `json:"auto_archive_duration"`
}

// ChannelEdit is the body for modifying a channel
type ChannelEdit struct {
	Archived bool `json:"archived"`
}

// Attachment is a file attached to a message
type Attachment struct {
	ID       string `json:"id"`
	Filename string `json:"filename"`
	Size     int64  `json:"size"`
	URL      string `json:"url"`
}

// Message is a Discord message
type Message struct {
	ID          string       `json:"id"`
	ChannelID   string       `json:"channel_id"`
	Attachments []Attachment `json:"attachments"`
}

// UploadSlotFile describes one file we want an upload url for
type UploadSlotFile struct {
	ID       string `json:"id"`
	Filename string `json:"filename"`
	FileSize int64  `json:"file_size"`
}

// UploadSlotsRequest asks for signed urls to upload attachments
// directly to the CDN bucket
type UploadSlotsRequest struct {
	Files []UploadSlotFile `json:"files"`
}

// UploadSlot is a signed url to PUT an attachment's bytes to
type UploadSlot struct {
	UploadURL      string `json:"upload_url"`
	UploadFilename string `json:"upload_filename"`
}

// UploadSlotsResponse is returned from the attachments endpoint, in
// request order
type UploadSlotsResponse struct {
	Attachments []UploadSlot `json:"attachments"`
}

// AttachmentRef references an attachment already uploaded to the CDN
type AttachmentRef struct {
	ID               string `json:"id"`
	Filename         string `json:"filename"`
	UploadedFilename string `json:"uploaded_filename"`
}

// MessageCreate is the body for sending a message made of uploaded
// attachments
type MessageCreate struct {
	Attachments []AttachmentRef `json:"attachments"`
}
