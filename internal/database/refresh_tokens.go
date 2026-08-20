package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// CreateRefreshToken opens a login session for a user. tokenHash is the
// SHA-256 of the token handed to the client; the token itself is never stored.
func (s *service) CreateRefreshToken(ctx context.Context, userID uint, tokenHash []byte, expiresAt time.Time) (*RefreshToken, error) {
	token := &RefreshToken{
		UserID:    userID,
		TokenHash: tokenHash,
		ExpiresAt: expiresAt,
		CreatedAt: time.Now(),
	}

	if err := s.db.WithContext(ctx).Create(token).Error; err != nil {
		return nil, fmt.Errorf("insert refresh token: %w", err)
	}
	return token, nil
}

// GetValidRefreshToken looks up a live session by its token hash, with the
// user loaded. It returns ErrRefreshTokenNotFound when the session does not
// exist, was revoked, or has expired.
//
// The three conditions are checked in one query rather than fetched and then
// judged in Go, so there is no window where a session expires between the read
// and the check.
func (s *service) GetValidRefreshToken(ctx context.Context, tokenHash []byte) (*RefreshToken, error) {
	var token RefreshToken

	err := s.db.WithContext(ctx).
		Preload("User").
		Where("token_hash = ? AND revoked_at IS NULL AND expires_at > ?", tokenHash, time.Now()).
		First(&token).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrRefreshTokenNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select refresh token: %w", err)
	}
	return &token, nil
}

// RevokeRefreshToken ends one session. It is not an error to revoke a session
// that is already gone: logging out twice is not a failure, and answering
// differently would tell a caller whether a token was ever real.
func (s *service) RevokeRefreshToken(ctx context.Context, tokenHash []byte) error {
	err := s.db.WithContext(ctx).
		Model(&RefreshToken{}).
		Where("token_hash = ? AND revoked_at IS NULL", tokenHash).
		Update("revoked_at", time.Now()).Error
	if err != nil {
		return fmt.Errorf("revoke refresh token: %w", err)
	}
	return nil
}

// PurgeExpiredRefreshTokens deletes a user's dead sessions: the ones that have
// expired, and the ones they logged out of. It runs at login, which keeps the
// table bounded without a background job — a user cannot pile up rows without
// logging in, and logging in is exactly when we clean them.
func (s *service) PurgeExpiredRefreshTokens(ctx context.Context, userID uint) error {
	err := s.db.WithContext(ctx).
		Where("user_id = ? AND (expires_at <= ? OR revoked_at IS NOT NULL)", userID, time.Now()).
		Delete(&RefreshToken{}).Error
	if err != nil {
		return fmt.Errorf("purge refresh tokens: %w", err)
	}
	return nil
}
