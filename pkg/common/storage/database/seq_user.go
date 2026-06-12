package database

import "context"

type SeqUser interface {
	GetUserMaxSeq(ctx context.Context, conversationID string, userID string) (int64, error)
	SetUserMaxSeq(ctx context.Context, conversationID string, userID string, seq int64) error
	GetUserMinSeq(ctx context.Context, conversationID string, userID string) (int64, error)
	SetUserMinSeq(ctx context.Context, conversationID string, userID string, seq int64) error
	GetUserReadSeq(ctx context.Context, conversationID string, userID string) (int64, error)
	SetUserReadSeq(ctx context.Context, conversationID string, userID string, seq int64) error
	GetUserReadSeqs(ctx context.Context, userID string, conversationID []string) (map[string]int64, error)
	// GetUserConversationIDs retrieves all conversationIDs that have seq records for a given userID.
	// This is used to discover conversations with pending messages that may be missing from the
	// user's conversation table (e.g., due to a failed conversation creation during the first message).
	GetUserConversationIDs(ctx context.Context, userID string) ([]string, error)
}
