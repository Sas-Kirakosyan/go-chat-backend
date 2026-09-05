package server

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"go-chat-backend/internal/database"
	"go-chat-backend/internal/event"
	"go-chat-backend/internal/metrics"
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

	// UnreadCount is how many messages the caller has not read in this room.
	//
	// It is the first field in this API that no handler computes. A consumer
	// on the broker maintains it in the background, so it can be a moment
	// behind the truth, and that is the trade the whole stage is about: the
	// sender's request does not wait for everyone else's badge to be updated.
	UnreadCount int64 `json:"unread_count"`
}

type messageDTO struct {
	ID             uint    `json:"id"`
	ConversationID uint    `json:"conversation_id"`
	Sender         userDTO `json:"sender"`
	Content        string  `json:"content"`
	ClientMsgID    *string `json:"client_msg_id,omitempty"`

	// Seq is this message's place in its own room: 1, 2, 3, with no holes.
	// A client stores the highest one it has seen per room, and hands it back
	// as ?after_seq= when it reconnects. It is also how a client spots a frame
	// it already has: delivery is at-least-once, so the same seq can arrive
	// twice and the second copy must be dropped, not shown.
	Seq uint `json:"seq"`

	CreatedAt time.Time `json:"created_at"`
}

type messagePageDTO struct {
	Messages []messageDTO `json:"messages"`
	// NextBeforeID is the ?before_id= value for the next, older page. It is
	// null when this page reached the start of the room.
	NextBeforeID *uint `json:"next_before_id"`
	// NextAfterSeq is the ?after_seq= value for the next, newer page. It is
	// null unless the caller asked for a gap and there is more of it: a client
	// that was away for an hour can miss more messages than one page holds.
	NextAfterSeq *uint `json:"next_after_seq,omitempty"`
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
		Seq:            m.Seq,
		CreatedAt:      m.CreatedAt,
	}
}

// eventToMessageDTO builds the same wire shape from an outbox event.
//
// There are two ways into messageDTO now, because there are two sources: a row
// read from Postgres for history, and an event that came through the broker
// for a live push. They must produce the same JSON — a client cannot be asked
// to parse a message differently depending on how it arrived — so both live
// here, next to each other, where a change to one is hard to make without
// seeing the other.
func eventToMessageDTO(ev event.MessageCreated) messageDTO {
	return messageDTO{
		ID:             ev.MessageID,
		ConversationID: ev.ConversationID,
		Sender:         userDTO{ID: ev.SenderID, Username: ev.SenderName},
		Content:        ev.Content,
		ClientMsgID:    ev.ClientMsgID,
		Seq:            ev.Seq,
		CreatedAt:      ev.CreatedAt,
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

	// One query for every badge on the screen, not one per room. A room the
	// user has read has no row, so a missing key means zero.
	//
	// A failure here is not a failure of the page. The rooms are the answer;
	// the badges are decoration, and a list that loads with no badges is far
	// better than a 500. This is the first place in the service where a
	// dependency is allowed to degrade the response instead of ending it, and
	// it is allowed because nothing here is a fact the user relies on.
	unread, err := s.db.UnreadForUser(c.Request.Context(), userID)
	if err != nil {
		logFrom(c).Warn("could not read unread counters, sending the list without them", "err", err)
		unread = nil
	}

	out := make([]conversationDTO, 0, len(convs))
	for _, conv := range convs {
		dto := toConversationDTO(conv)
		dto.UnreadCount = unread[conv.ID]
		out = append(out, dto)
	}
	c.JSON(http.StatusOK, gin.H{"conversations": out})
}

// MarkReadHandler handles POST /conversations/:id/read. It sets the caller's
// unread count for the room back to zero.
//
// It takes no body. "I have read this room" is the only thing a client can say
// here, and a seq in the body would only invite a client to send a number it
// never actually reached.
//
// The consumer's own bookmark is not moved, so a message that arrives during
// this request is still counted from now on. See MarkConversationRead for the
// small race that leaves, and why it is accepted.
func (s *Server) MarkReadHandler(c *gin.Context) {
	conversationID, ok := s.memberOnly(c)
	if !ok {
		return
	}
	userID, _ := currentUser(c)

	if err := s.db.MarkConversationRead(c.Request.Context(), conversationID, userID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not mark the conversation read"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"conversation_id": conversationID, "unread_count": 0})
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
// This is the only path that ever writes a message. The WebSocket only
// delivers what this handler stored.
//
// It does exactly one write, and that is the Stage 5 change. It used to commit
// the message and then publish it, and a process that died between those two
// steps left a message that existed and that nobody was ever told about. Now
// the message row and the instruction to deliver it commit together, and a
// relay picks the instruction up afterwards. This handler no longer knows that
// sockets exist.
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

	msg, created, err := s.db.CreateMessage(c.Request.Context(), conversationID, userID, username, req.Content, clientMsgID)
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

	// Only a real new row is counted, so rate() over this is the real write
	// rate rather than the request rate.
	//
	// There is nothing to push here any more. A retry of a client_msg_id we
	// already hold wrote no message row, and so wrote no outbox row either —
	// the rollback took both — which means the double-post that client_msg_id
	// exists to prevent is now prevented one layer deeper, in the transaction
	// itself, instead of by this if.
	if created {
		metrics.MessagesStored.Inc()
	}
}

// ListMessagesHandler handles GET /conversations/:id/messages.
//
// It serves two different reads through one route, because both answer "give
// me messages from this room" and both need the same membership check:
//
//   - ?before_id=  walks BACKWARDS through history, newest first. This is a
//     person scrolling up.
//   - ?after_seq=  walks FORWARDS through a gap, oldest first. This is a client
//     that reconnected and is catching up.
func (s *Server) ListMessagesHandler(c *gin.Context) {
	conversationID, ok := s.memberOnly(c)
	if !ok {
		return
	}

	q, ok := pageParams(c)
	if !ok {
		return
	}
	if q.gap {
		s.listGap(c, conversationID, q)
		return
	}

	msgs, err := s.db.ListMessages(c.Request.Context(), conversationID, q.beforeID, q.limit)
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
	if len(msgs) == q.limit {
		oldest := msgs[len(msgs)-1].ID
		page.NextBeforeID = &oldest
	}

	c.JSON(http.StatusOK, page)
}

// listGap answers ?after_seq=: everything this client missed, oldest first.
//
// This is the repair path for a dropped socket. Live push is best effort — a
// node with a dead Redis subscription, or a client whose connection died for
// three seconds, simply does not get those frames. Before sequence numbers a
// client could not even tell that had happened. Now it can: it remembers the
// last seq it saw, asks for what came after, and the hole is closed.
func (s *Server) listGap(c *gin.Context, conversationID uint, q pageQuery) {
	msgs, err := s.db.ListMessagesAfterSeq(c.Request.Context(), conversationID, q.afterSeq, q.limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not list messages"})
		return
	}

	page := messagePageDTO{Messages: make([]messageDTO, 0, len(msgs))}
	for _, m := range msgs {
		page.Messages = append(page.Messages, toMessageDTO(m))
	}
	// A full page means the gap is bigger than one page. The newest seq here
	// is where the next request starts. This is not rare: a client away for an
	// hour in a busy room misses far more than maxPageSize messages.
	if len(msgs) == q.limit {
		newest := msgs[len(msgs)-1].Seq
		page.NextAfterSeq = &newest
	}

	// How much live delivery is being missed, in one number. Flat at zero means
	// push is reaching everyone. A rising rate means sockets are dropping, or a
	// node has lost its Redis subscription and nobody noticed.
	metrics.GapMessages.Add(float64(len(msgs)))
	metrics.GapSize.Observe(float64(len(msgs)))

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

// pageQuery is one parsed history request.
type pageQuery struct {
	// beforeID is the ?before_id= cursor: read history backwards from here.
	beforeID uint

	// afterSeq is the ?after_seq= cursor: read the gap forwards from here.
	afterSeq uint

	// gap says the caller asked for ?after_seq=. It cannot be replaced by
	// "afterSeq > 0", because after_seq=0 is a real and useful request: it
	// means "I have nothing yet, start me at the beginning of the room".
	gap bool

	limit int
}

// pageParams reads ?before_id=, ?after_seq= and ?limit=. On failure it has
// already written the response.
func pageParams(c *gin.Context) (pageQuery, bool) {
	var q pageQuery

	rawBefore := c.Query("before_id")
	rawAfter := c.Query("after_seq")

	// The two cursors run in opposite directions, so a request carrying both
	// has no single sensible answer. Refusing is better than silently picking
	// one and handing back a page the client did not ask for.
	if rawBefore != "" && rawAfter != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Use before_id or after_seq, not both"})
		return q, false
	}

	if rawBefore != "" {
		v, err := strconv.ParseUint(rawBefore, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "before_id must be a whole number"})
			return q, false
		}
		q.beforeID = uint(v)
	}

	if rawAfter != "" {
		v, err := strconv.ParseUint(rawAfter, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "after_seq must be a whole number"})
			return q, false
		}
		q.afterSeq = uint(v)
		q.gap = true
	}

	q.limit = defaultPageSize
	if raw := c.Query("limit"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 1 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be a whole number above zero"})
			return q, false
		}
		q.limit = min(v, maxPageSize)
	}
	return q, true
}
