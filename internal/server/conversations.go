package server

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"go-chat-backend/internal/database"
)

// History is read in pages, newest first. A client that sends no limit gets
// defaultPageSize rows. One that asks for more than maxPageSize is capped
// rather than refused, so a typo cannot pull the whole table.
const (
	defaultPageSize = 50
	maxPageSize     = 100
)

// ---------------------------------------------------------------------------
// Wire types
//
// These are the shapes that travel over HTTP, and they are kept apart from the
// GORM models on purpose. A model carries DeletedAt, a password hash and
// association slices that must never reach a client, and renaming a column
// must not silently change the public API.
// ---------------------------------------------------------------------------

type userDTO struct {
	ID       uint   `json:"id"`
	Username string `json:"username"`
}

type conversationDTO struct {
	ID          uint      `json:"id"`
	Title       string    `json:"title"`
	CreatedByID uint      `json:"created_by_id"`
	CreatedAt   time.Time `json:"created_at"`
	Members     []userDTO `json:"members"`
}

type messageDTO struct {
	ID             uint      `json:"id"`
	ConversationID uint      `json:"conversation_id"`
	Sender         userDTO   `json:"sender"`
	Content        string    `json:"content"`
	ClientMsgID    *string   `json:"client_msg_id,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

type messagePageDTO struct {
	Messages []messageDTO `json:"messages"`
	// NextBeforeID is the ?before_id= value for the next, older page. It is
	// null when this page reached the start of the room.
	NextBeforeID *uint `json:"next_before_id"`
}

type createConversationRequest struct {
	Title string `json:"title" binding:"required,min=1,max=200"`
	// MemberIDs is optional. The creator is always added, whatever is here.
	MemberIDs []uint `json:"member_ids" binding:"max=50,dive,gt=0"`
}

type addMemberRequest struct {
	UserID uint `json:"user_id" binding:"required,gt=0"`
}

type sendMessageRequest struct {
	Content string `json:"content" binding:"required,min=1,max=4000"`
	// ClientMsgID is optional. Send the same one again after a timeout and the
	// server returns the first message instead of writing a second one.
	ClientMsgID string `json:"client_msg_id" binding:"omitempty,max=64"`
}

func toUserDTO(u database.User) userDTO {
	return userDTO{ID: u.ID, Username: u.Username}
}

func toConversationDTO(c database.Conversation) conversationDTO {
	members := make([]userDTO, 0, len(c.Members))
	for _, m := range c.Members {
		members = append(members, userDTO{ID: m.UserID, Username: m.User.Username})
	}
	return conversationDTO{
		ID:          c.ID,
		Title:       c.Title,
		CreatedByID: c.CreatedByID,
		CreatedAt:   c.CreatedAt,
		Members:     members,
	}
}

func toMessageDTO(m database.Message) messageDTO {
	return messageDTO{
		ID:             m.ID,
		ConversationID: m.ConversationID,
		Sender:         userDTO{ID: m.SenderID, Username: m.Sender.Username},
		Content:        m.Content,
		ClientMsgID:    m.ClientMsgID,
		CreatedAt:      m.CreatedAt,
	}
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

// CreateConversationHandler handles POST /conversations.
func (s *Server) CreateConversationHandler(c *gin.Context) {
	var req createConversationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid input"})
		return
	}
	userID, _ := currentUser(c)

	conv, err := s.db.CreateConversation(c.Request.Context(), req.Title, userID, req.MemberIDs)
	if errors.Is(err, database.ErrUserNotFound) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "One of the members does not exist"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not create conversation"})
		return
	}

	c.JSON(http.StatusCreated, toConversationDTO(*conv))
}

// ListConversationsHandler handles GET /conversations.
func (s *Server) ListConversationsHandler(c *gin.Context) {
	userID, _ := currentUser(c)

	convs, err := s.db.ListConversationsForUser(c.Request.Context(), userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not list conversations"})
		return
	}

	out := make([]conversationDTO, 0, len(convs))
	for _, conv := range convs {
		out = append(out, toConversationDTO(conv))
	}
	c.JSON(http.StatusOK, gin.H{"conversations": out})
}

// AddMemberHandler handles POST /conversations/:id/members. Any member may add
// anyone. The room is checked before the body, so an outsider never learns
// which user ids exist.
func (s *Server) AddMemberHandler(c *gin.Context) {
	conversationID, ok := s.memberOnly(c)
	if !ok {
		return
	}

	var req addMemberRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid input"})
		return
	}

	if _, err := s.db.GetUserByID(c.Request.Context(), req.UserID); err != nil {
		if errors.Is(err, database.ErrUserNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not look up user"})
		return
	}

	member, err := s.db.AddMember(c.Request.Context(), conversationID, req.UserID)
	if errors.Is(err, database.ErrAlreadyMember) {
		c.JSON(http.StatusConflict, gin.H{"error": "User is already a member"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not add member"})
		return
	}

	c.JSON(http.StatusCreated, toUserDTO(member.User))
}

// SendMessageHandler handles POST /conversations/:id/messages.
//
// This is the only path that ever writes a message. The WebSocket, when it
// arrives, will only deliver what this handler stored.
func (s *Server) SendMessageHandler(c *gin.Context) {
	conversationID, ok := s.memberOnly(c)
	if !ok {
		return
	}

	var req sendMessageRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid input"})
		return
	}
	userID, username := currentUser(c)

	var clientMsgID *string
	if req.ClientMsgID != "" {
		clientMsgID = &req.ClientMsgID
	}

	msg, created, err := s.db.CreateMessage(c.Request.Context(), conversationID, userID, req.Content, clientMsgID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not send message"})
		return
	}

	// The sender is the caller, whose name the token already carries, so the
	// store does not have to read the user row back on the write path.
	out := toMessageDTO(*msg)
	out.Sender.Username = username

	// A repeat of a client_msg_id we already hold created nothing, so it
	// answers 200 instead of 201. The body is the same either way.
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	c.JSON(status, out)

	// Only a real new row is pushed. A retry stored nothing, so pushing again
	// would show the same line twice on every screen in the room — the exact
	// double-post that client_msg_id exists to prevent.
	if created {
		s.broadcastMessage(c, conversationID, out)
	}
}

// ListMessagesHandler handles GET /conversations/:id/messages.
func (s *Server) ListMessagesHandler(c *gin.Context) {
	conversationID, ok := s.memberOnly(c)
	if !ok {
		return
	}

	beforeID, limit, ok := pageParams(c)
	if !ok {
		return
	}

	msgs, err := s.db.ListMessages(c.Request.Context(), conversationID, beforeID, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not list messages"})
		return
	}

	page := messagePageDTO{Messages: make([]messageDTO, 0, len(msgs))}
	for _, m := range msgs {
		page.Messages = append(page.Messages, toMessageDTO(m))
	}
	// A full page means there is probably more behind it. The oldest id on
	// this page is the cursor for the next one.
	if len(msgs) == limit {
		oldest := msgs[len(msgs)-1].ID
		page.NextBeforeID = &oldest
	}

	c.JSON(http.StatusOK, page)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// memberOnly reads :id and checks that the caller is inside that room. On
// failure it has already written the response, and the handler must return.
//
// Everything answers 404 here: a bad id, a room that does not exist, and a
// real room the caller is not in. 403 would confirm the room is real, which
// hands an outsider a way to map which rooms exist.
func (s *Server) memberOnly(c *gin.Context) (uint, bool) {
	raw, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil || raw == 0 {
		conversationNotFound(c)
		return 0, false
	}
	conversationID := uint(raw)

	userID, _ := currentUser(c)
	if err := s.db.EnsureMember(c.Request.Context(), conversationID, userID); err != nil {
		if errors.Is(err, database.ErrConversationNotFound) {
			conversationNotFound(c)
			return 0, false
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not look up conversation"})
		return 0, false
	}
	return conversationID, true
}

func conversationNotFound(c *gin.Context) {
	c.JSON(http.StatusNotFound, gin.H{"error": "Conversation not found"})
}

// pageParams reads ?before_id= and ?limit=. On failure it has already written
// the response.
func pageParams(c *gin.Context) (beforeID uint, limit int, ok bool) {
	if raw := c.Query("before_id"); raw != "" {
		v, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "before_id must be a whole number"})
			return 0, 0, false
		}
		beforeID = uint(v)
	}

	limit = defaultPageSize
	if raw := c.Query("limit"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 1 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be a whole number above zero"})
			return 0, 0, false
		}
		limit = min(v, maxPageSize)
	}
	return beforeID, limit, true
}
