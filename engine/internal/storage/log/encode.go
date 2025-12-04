package log

import (
	"bytes"
	"encoding/gob"
)

// EncodeMessage encodes a message to bytes for storage
func EncodeMessage(msg *Message) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)

	// Encode message fields
	if err := encoder.Encode(msg.ID); err != nil {
		return nil, err
	}
	if err := encoder.Encode(msg.ResourcePath); err != nil {
		return nil, err
	}
	if err := encoder.Encode(msg.Partition); err != nil {
		return nil, err
	}
	if err := encoder.Encode(msg.Offset); err != nil {
		return nil, err
	}
	if err := encoder.Encode(msg.Seq); err != nil {
		return nil, err
	}
	if err := encoder.Encode(string(msg.Type)); err != nil {
		return nil, err
	}
	if err := encoder.Encode(msg.Payload); err != nil {
		return nil, err
	}
	if err := encoder.Encode(msg.Headers); err != nil {
		return nil, err
	}
	if err := encoder.Encode(msg.CreatedAt); err != nil {
		return nil, err
	}
	if err := encoder.Encode(msg.VisibleAt); err != nil {
		return nil, err
	}
	if err := encoder.Encode(msg.Attempts); err != nil {
		return nil, err
	}
	if err := encoder.Encode(msg.SchemaVersion); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DecodeMessage decodes bytes back into a message
func DecodeMessage(data []byte) (*Message, error) {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)

	msg := &Message{}

	var typeStr string

	// Decode message fields
	if err := decoder.Decode(&msg.ID); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&msg.ResourcePath); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&msg.Partition); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&msg.Offset); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&msg.Seq); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&typeStr); err != nil {
		return nil, err
	}
	msg.Type = MessageType(typeStr)
	if err := decoder.Decode(&msg.Payload); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&msg.Headers); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&msg.CreatedAt); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&msg.VisibleAt); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&msg.Attempts); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&msg.SchemaVersion); err != nil {
		return nil, err
	}

	return msg, nil
}

// EncodeMessageV1 encodes a message using a more efficient binary format
// This is a future optimization - for now, we use gob for simplicity
func EncodeMessageV1(msg *Message) ([]byte, error) {
	// Placeholder for future binary encoding
	return EncodeMessage(msg)
}

// DecodeMessageV1 decodes a message from binary format
func DecodeMessageV1(data []byte) (*Message, error) {
	// Placeholder for future binary decoding
	return DecodeMessage(data)
}

// MessageSize returns the approximate size of an encoded message
func MessageSize(msg *Message) int {
	size := len(msg.ID) + len(msg.ResourcePath) + 4 + 8 + 8 // ID, Path, Partition, Offset, Seq
	size += len(string(msg.Type)) + len(msg.Payload)
	size += 8 + 8 + 4 + 4 // CreatedAt, VisibleAt, Attempts, SchemaVersion

	// Headers
	for k, v := range msg.Headers {
		size += len(k) + len(v)
	}

	// Encoding overhead (gob)
	size += 256 // rough estimate

	return size
}
