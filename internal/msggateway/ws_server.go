package msggateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/openimsdk/open-im-server/v3/pkg/rpcli"
	"github.com/openimsdk/tools/apiresp"

	"github.com/openimsdk/open-im-server/v3/pkg/common/webhook"
	"github.com/openimsdk/open-im-server/v3/pkg/rpccache"
	pbAuth "github.com/openimsdk/protocol/auth"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/mcontext"

	"github.com/go-playground/validator/v10"
	"github.com/openimsdk/open-im-server/v3/pkg/common/prommetrics"
	"github.com/openimsdk/open-im-server/v3/pkg/common/servererrs"
	"github.com/openimsdk/protocol/msggateway"
	"github.com/openimsdk/tools/discovery"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/utils/idutil"
	"github.com/openimsdk/tools/utils/jsonutil"
	"github.com/openimsdk/tools/utils/timeutil"
	"golang.org/x/sync/errgroup"
)

var wsSuccessResponse, _ = json.Marshal(&apiresp.ApiResponse{})

type LongConnServer interface {
	Run(ctx context.Context) error
	wsHandler(w http.ResponseWriter, r *http.Request)
	GetUserAllCons(userID string) ([]*Client, bool)
	GetUserPlatformCons(userID string, platform int) ([]*Client, bool, bool)
	Validate(s any) error
	SetDiscoveryRegistry(ctx context.Context, client discovery.Conn, config *Config) error
	KickUserConn(client *Client) error
	UnRegister(c *Client)
	SetKickHandlerInfo(i *kickHandler)
	SubUserOnlineStatus(ctx context.Context, client *Client, data *Req) ([]byte, error)
	Compressor
	MessageHandler
}

type WsServer struct {
	websocket         *websocket.Upgrader
	msgGatewayConfig  *Config
	port              int
	wsMaxConnNum      int64
	registerChan      chan *Client
	unregisterChan    chan *Client
	kickHandlerChan   chan *kickHandler
	clients           UserMap
	online            rpccache.OnlineCache
	subscription      *Subscription
	clientPool        sync.Pool
	onlineUserNum     atomic.Int64
	onlineUserConnNum atomic.Int64
	handshakeTimeout  time.Duration
	writeBufferSize   int
	validate          *validator.Validate
	disCov            discovery.Conn
	Compressor
	//Encoder
	MessageHandler
	webhookClient *webhook.Client
	userClient    *rpcli.UserClient
	authClient    *rpcli.AuthClient
	msgClient     *rpcli.MsgClient

	ready atomic.Bool
}

type kickHandler struct {
	clientOK   bool
	oldClients []*Client
	newClient  *Client
}

func (ws *WsServer) SetDiscoveryRegistry(ctx context.Context, disCov discovery.Conn, config *Config) error {
	userConn, err := disCov.GetConn(ctx, config.Discovery.RpcService.User)
	if err != nil {
		return err
	}
	pushConn, err := disCov.GetConn(ctx, config.Discovery.RpcService.Push)
	if err != nil {
		return err
	}
	authConn, err := disCov.GetConn(ctx, config.Discovery.RpcService.Auth)
	if err != nil {
		return err
	}
	msgConn, err := disCov.GetConn(ctx, config.Discovery.RpcService.Msg)
	if err != nil {
		return err
	}
	ws.userClient = rpcli.NewUserClient(userConn)
	ws.authClient = rpcli.NewAuthClient(authConn)
	msgClient := rpcli.NewMsgClient(msgConn)
	ws.msgClient = msgClient
	ws.MessageHandler = NewGrpcHandler(ws.validate, msgClient, rpcli.NewPushMsgServiceClient(pushConn))
	ws.disCov = disCov

	ws.ready.Store(true)
	return nil
}

//func (ws *WsServer) SetUserOnlineStatus(ctx context.Context, client *Client, status int32) {
//	err := ws.userClient.SetUserStatus(ctx, client.UserID, status, client.PlatformID)
//	if err != nil {
//		log.ZWarn(ctx, "SetUserStatus err", err)
//	}
//	switch status {
//	case constant.Online:
//		ws.webhookAfterUserOnline(ctx, &ws.msgGatewayConfig.WebhooksConfig.AfterUserOnline, client.UserID, client.PlatformID, client.IsBackground, client.ctx.GetConnID())
//	case constant.Offline:
//		ws.webhookAfterUserOffline(ctx, &ws.msgGatewayConfig.WebhooksConfig.AfterUserOffline, client.UserID, client.PlatformID, client.ctx.GetConnID())
//	}
//}

func (ws *WsServer) UnRegister(c *Client) {
	ws.unregisterChan <- c
}

func (ws *WsServer) Validate(_ any) error {
	return nil
}

func (ws *WsServer) GetUserAllCons(userID string) ([]*Client, bool) {
	return ws.clients.GetAll(userID)
}

func (ws *WsServer) GetUserPlatformCons(userID string, platform int) ([]*Client, bool, bool) {
	return ws.clients.Get(userID, platform)
}

func NewWsServer(msgGatewayConfig *Config, opts ...Option) *WsServer {
	var config configs
	for _, o := range opts {
		o(&config)
	}
	//userRpcClient := rpcclient.NewUserRpcClient(client, config.Discovery.RpcService.User, config.Share.IMAdminUser)
	upgrader := &websocket.Upgrader{
		HandshakeTimeout: config.handshakeTimeout,
		CheckOrigin:      func(r *http.Request) bool { return true },
	}
	v := validator.New()
	return &WsServer{
		websocket:        upgrader,
		msgGatewayConfig: msgGatewayConfig,
		port:             config.port,
		wsMaxConnNum:     config.maxConnNum,
		writeBufferSize:  config.writeBufferSize,
		handshakeTimeout: config.handshakeTimeout,
		clientPool: sync.Pool{
			New: func() any {
				return new(Client)
			},
		},
		registerChan:    make(chan *Client, 1000),
		unregisterChan:  make(chan *Client, 1000),
		kickHandlerChan: make(chan *kickHandler, 1000),
		validate:        v,
		clients:         newUserMap(),
		subscription:    newSubscription(),
		Compressor:      NewGzipCompressor(),
		webhookClient:   webhook.NewWebhookClient(msgGatewayConfig.WebhooksConfig.URL),
	}
}

func (ws *WsServer) Run(ctx context.Context) error {
	var client *Client

	ctx, cancel := context.WithCancelCause(ctx)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case client = <-ws.registerChan:
				ws.registerClient(client)
			case client = <-ws.unregisterChan:
				ws.unregisterClient(client)
			case onlineInfo := <-ws.kickHandlerChan:
				ws.multiTerminalLoginChecker(onlineInfo.clientOK, onlineInfo.oldClients, onlineInfo.newClient)
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		wsServer := http.Server{Addr: fmt.Sprintf(":%d", ws.port), Handler: nil}
		http.HandleFunc("/", ws.wsHandler)
		go func() {
			defer close(done)
			<-ctx.Done()
			_ = wsServer.Shutdown(context.Background())
		}()
		err := wsServer.ListenAndServe()
		if err == nil {
			err = fmt.Errorf("http server closed")
		}
		cancel(fmt.Errorf("msg gateway %w", err))
	}()

	<-ctx.Done()

	timeout := time.NewTimer(time.Second * 15)
	defer timeout.Stop()
	select {
	case <-timeout.C:
		log.ZWarn(ctx, "msg gateway graceful stop timeout", nil)
	case <-done:
		log.ZDebug(ctx, "msg gateway graceful stop done")
	}
	return context.Cause(ctx)
}

const concurrentRequest = 3

func (ws *WsServer) sendUserOnlineInfoToOtherNode(ctx context.Context, client *Client) error {
	conns, err := ws.disCov.GetConns(ctx, ws.msgGatewayConfig.Discovery.RpcService.MessageGateway)
	if err != nil {
		return err
	}
	if len(conns) == 0 || (len(conns) == 1 && ws.disCov.IsSelfNode(conns[0])) {
		return nil
	}

	wg := errgroup.Group{}
	wg.SetLimit(concurrentRequest)

	// Online push user online message to other node
	for _, v := range conns {
		v := v
		log.ZDebug(ctx, "sendUserOnlineInfoToOtherNode conn")
		if ws.disCov.IsSelfNode(v) {
			log.ZDebug(ctx, "Filter out this node")
			continue
		}

		wg.Go(func() error {
			msgClient := msggateway.NewMsgGatewayClient(v)
			_, err := msgClient.MultiTerminalLoginCheck(ctx, &msggateway.MultiTerminalLoginCheckReq{
				UserID:     client.UserID,
				PlatformID: int32(client.PlatformID), Token: client.token,
			})
			if err != nil {
				log.ZWarn(ctx, "MultiTerminalLoginCheck err", err)
			}
			return nil
		})
	}

	_ = wg.Wait()
	return nil
}

func (ws *WsServer) SetKickHandlerInfo(i *kickHandler) {
	ws.kickHandlerChan <- i
}

func (ws *WsServer) registerClient(client *Client) {
	var (
		userOK     bool
		clientOK   bool
		oldClients []*Client
	)
	oldClients, userOK, clientOK = ws.clients.Get(client.UserID, client.PlatformID)

	log.ZInfo(client.ctx, "registerClient", "userID", client.UserID, "platformID", client.PlatformID)

	if !userOK {
		ws.clients.Set(client.UserID, client)
		log.ZDebug(client.ctx, "user not exist", "userID", client.UserID, "platformID", client.PlatformID)
		prommetrics.OnlineUserGauge.Add(1)
		ws.onlineUserNum.Add(1)
		ws.onlineUserConnNum.Add(1)
	} else {
		ws.multiTerminalLoginChecker(clientOK, oldClients, client)
		log.ZDebug(client.ctx, "user exist", "userID", client.UserID, "platformID", client.PlatformID)
		if clientOK {
			ws.clients.Set(client.UserID, client)
			// There is already a connection to the platform
			log.ZDebug(client.ctx, "repeat login", "userID", client.UserID, "platformID",
				client.PlatformID, "old remote addr", getRemoteAdders(oldClients))
			ws.onlineUserConnNum.Add(1)
		} else {
			ws.clients.Set(client.UserID, client)
			ws.onlineUserConnNum.Add(1)
		}
	}

	wg := sync.WaitGroup{}
	log.ZDebug(client.ctx, "ws.msgGatewayConfig.Discovery.Enable", "discoveryEnable", ws.msgGatewayConfig.Discovery.Enable)

	if ws.msgGatewayConfig.Discovery.Enable != "k8s" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = ws.sendUserOnlineInfoToOtherNode(client.ctx, client)
		}()
	}

	//wg.Add(1)
	//go func() {
	//	defer wg.Done()
	//	ws.SetUserOnlineStatus(client.ctx, client, constant.Online)
	//}()

	wg.Wait()

	// Push a ConversationChangeNotification to trigger the SDK to sync offline messages.
	// This ensures the user receives pending messages immediately upon login,
	// without waiting for a new message from another user to trigger the sync.
	go ws.pushSyncNotification(client)

	log.ZDebug(client.ctx, "user online", "online user Num", ws.onlineUserNum.Load(), "online user conn Num", ws.onlineUserConnNum.Load())
}

// pushSyncNotification ensures offline messages are delivered to the client upon login.
//
// In K8s multi-pod deployments, there are two parallel flows on login:
//  1. HTTP: Client calls REST API → gets conversation list → stores MaxSeq locally
//  2. Push: registerClient sends push notification → Kafka → push service → WebSocket
//
// Flow 2 is async (Kafka latency) and may arrive AFTER flow 1. When it does,
// the SDK compares server MaxSeq with locally-stored MaxSeq (already set by flow 1),
// finds them equal, and decides "nothing to sync".
//
// Solution: Bypass the SDK's comparison by directly pulling and pushing pending
// messages through the WebSocket. Use GetMaxSeq (which has fallback to seq_user table)
// to discover ALL conversations, avoiding dependency on the conversation table
// (which may miss conversations for the receiver).
func (ws *WsServer) pushSyncNotification(client *Client) {
	// Delay to ensure the client's WebSocket and SDK listeners are fully ready
	time.Sleep(time.Millisecond * 500)

	if client.closed.Load() {
		return
	}

	ctx := mcontext.SetOperationID(context.Background(), "pushsync_"+client.UserID+"_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	ctx = mcontext.SetOpUserID(ctx, client.UserID)
	log.ZInfo(ctx, "[pushSyncNotification] START", "userID", client.UserID, "platformID", client.PlatformID)

	// Step 1: Discover ALL conversations with pending messages.
	// Use GetMaxSeq (which has getConversationIDsFallback to seq_user table) instead of
	// GetConversationsHasReadAndMaxSeq, because the latter calls GetConversations() which
	// fails entirely when any conversation record is missing (e.g., si_uid1_uid2).
	var directPushCount int
	if ws.msgClient != nil {
		directPushCount = ws.tryDirectPush(ctx, client)
	}

	log.ZInfo(ctx, "[pushSyncNotification] direct push done", "userID", client.UserID, "pushedCount", directPushCount)

	// Step 2: Always send the ConversationChangeNotification as an additional trigger.
	// Even if direct push succeeded, the notification ensures the full SDK sync machinery
	// runs, updating conversation list, unread counts, etc.
	ws.sendConversationChangeNotification(ctx, client)
}

// tryDirectPush discovers pending offline messages using GetMaxSeq (which has
// seq_user fallback) and pushes them directly to the client via WebSocket.
// Returns the number of messages pushed.
func (ws *WsServer) tryDirectPush(ctx context.Context, client *Client) int {
	// 1a. GetMaxSeq – discovers ALL conversations including those missing from the
	//     conversation table (via getConversationIDsFallback → seq_user MongoDB query).
	maxSeqResp, err := ws.msgClient.GetMaxSeq(ctx, &sdkws.GetMaxSeqReq{
		UserID: client.UserID,
	})
	if err != nil {
		log.ZWarn(ctx, "[pushSyncNotification] GetMaxSeq failed", err, "userID", client.UserID)
		return 0
	}
	if maxSeqResp == nil || len(maxSeqResp.MaxSeqs) == 0 {
		log.ZInfo(ctx, "[pushSyncNotification] GetMaxSeq returned empty, no pending conversations", "userID", client.UserID)
		return 0
	}

	log.ZInfo(ctx, "[pushSyncNotification] GetMaxSeq result",
		"userID", client.UserID,
		"convCount", len(maxSeqResp.MaxSeqs),
		"maxSeqs", maxSeqResp.MaxSeqs,
	)

	// 1b. Extract conversation IDs and get hasReadSeqs
	convIDs := make([]string, 0, len(maxSeqResp.MaxSeqs))
	for convID := range maxSeqResp.MaxSeqs {
		convIDs = append(convIDs, convID)
	}

	hasReadSeqs, err := ws.msgClient.GetHasReadSeqs(ctx, convIDs, client.UserID)
	if err != nil {
		log.ZWarn(ctx, "[pushSyncNotification] GetHasReadSeqs failed", err, "userID", client.UserID)
		// Continue with hasReadSeq=0 for all conversations
		hasReadSeqs = make(map[string]int64)
	}

	// 2. Build SeqRanges for conversations with pending messages.
	//    For conversations where maxSeq > hasReadSeq, we pull [hasReadSeq+1, maxSeq]
	//    (the truly unread range). However, when the unread range is very small (e.g.,
	//    only 1-2 notification seqs), the client may have no content messages to display
	//    — especially if its locally-stored syncedMaxSeqs became stale in a previous
	//    session. To guarantee content delivery, we expand the range backwards by a
	//    minimum window so the client always receives enough recent messages.
	const recentMessageWindow = 50
	var seqRanges []*sdkws.SeqRange
	for convID, maxSeq := range maxSeqResp.MaxSeqs {
		hasReadSeq := hasReadSeqs[convID]
		if maxSeq > hasReadSeq {
			begin := hasReadSeq + 1
			// Expand backward if the unread range is smaller than the minimum window,
			// so the client receives context content messages, not just notifications.
			if maxSeq-begin < recentMessageWindow {
				extendedBegin := maxSeq - recentMessageWindow
				if extendedBegin < 1 {
					extendedBegin = 1
				}
				if extendedBegin < begin {
					begin = extendedBegin
				}
			}
			seqRanges = append(seqRanges, &sdkws.SeqRange{
				ConversationID: convID,
				Begin:          begin,
				End:            maxSeq,
				Num:            maxSeq - begin + 1,
			})
			log.ZInfo(ctx, "[pushSyncNotification] pending seq range",
				"userID", client.UserID,
				"convID", convID,
				"hasReadSeq", hasReadSeq,
				"maxSeq", maxSeq,
				"rangeBegin", begin,
				"rangeEnd", maxSeq,
			)
		}
	}

	if len(seqRanges) == 0 {
		log.ZInfo(ctx, "[pushSyncNotification] no pending messages found", "userID", client.UserID)
		return 0
	}

	log.ZInfo(ctx, "[pushSyncNotification] pulling messages",
		"userID", client.UserID,
		"rangeCount", len(seqRanges),
	)

	// 3. Pull the actual messages
	pullResp, err := ws.msgClient.PullMessageBySeqs(ctx, &sdkws.PullMessageBySeqsReq{
		UserID:    client.UserID,
		SeqRanges: seqRanges,
	})
	if err != nil {
		log.ZWarn(ctx, "[pushSyncNotification] PullMessageBySeqs failed", err, "userID", client.UserID)
		return 0
	}
	if pullResp == nil {
		log.ZInfo(ctx, "[pushSyncNotification] PullMessageBySeqs returned nil", "userID", client.UserID)
		return 0
	}

	log.ZInfo(ctx, "[pushSyncNotification] PullMessageBySeqs result",
		"userID", client.UserID,
		"msgConvCount", len(pullResp.Msgs),
		"notifConvCount", len(pullResp.NotificationMsgs),
	)

	// 4. Push each message to the client
	pushCount := 0
	for convID, pullMsgs := range pullResp.Msgs {
		for _, msgItem := range pullMsgs.Msgs {
			if err := client.PushMessage(ctx, msgItem); err != nil {
				log.ZWarn(ctx, "[pushSyncNotification] push msg failed", err,
					"userID", client.UserID, "convID", convID, "clientMsgID", msgItem.ClientMsgID, "seq", msgItem.Seq)
			} else {
				pushCount++
				log.ZDebug(ctx, "[pushSyncNotification] pushed msg",
					"userID", client.UserID, "convID", convID, "seq", msgItem.Seq)
			}
		}
	}
	for convID, pullMsgs := range pullResp.NotificationMsgs {
		for _, msgItem := range pullMsgs.Msgs {
			if err := client.PushMessage(ctx, msgItem); err != nil {
				log.ZWarn(ctx, "[pushSyncNotification] push notif msg failed", err,
					"userID", client.UserID, "convID", convID, "clientMsgID", msgItem.ClientMsgID)
			} else {
				pushCount++
			}
		}
	}

	log.ZInfo(ctx, "[pushSyncNotification] direct push complete",
		"userID", client.UserID,
		"totalPushed", pushCount,
	)

	return pushCount
}

// sendConversationChangeNotification sends the generic ConversationChangeNotification
// that triggers the SDK's sync machinery (GetMaxSeq → PullMessageBySeqs).
func (ws *WsServer) sendConversationChangeNotification(ctx context.Context, client *Client) {
	tips := &sdkws.ConversationUpdateTips{
		UserID: client.UserID,
	}

	n := sdkws.NotificationElem{Detail: jsonutil.StructToJsonString(tips)}
	content, err := json.Marshal(&n)
	if err != nil {
		log.ZWarn(ctx, "[pushSyncNotification] marshal notification failed", err, "userID", client.UserID)
		return
	}

	msgData := &sdkws.MsgData{
		SendID:      client.UserID,
		RecvID:      client.UserID,
		Content:     content,
		MsgFrom:     constant.SysMsgType,
		ContentType: constant.ConversationChangeNotification,
		SessionType: constant.SingleChatType,
		CreateTime:  timeutil.GetCurrentTimestampByMill(),
		ClientMsgID: idutil.GetMsgIDByMD5(client.UserID),
		Options: map[string]bool{
			constant.IsHistory:    false,
			constant.IsPersistent: false,
			constant.IsSenderSync: false,
		},
	}

	if err := client.PushMessage(ctx, msgData); err != nil {
		log.ZWarn(ctx, "[pushSyncNotification] push notification failed", err, "userID", client.UserID)
	} else {
		log.ZInfo(ctx, "[pushSyncNotification] sent ConversationChangeNotification", "userID", client.UserID, "platformID", client.PlatformID)
	}
}

func getRemoteAdders(client []*Client) string {
	var ret string
	for i, c := range client {
		if i == 0 {
			ret = c.ctx.GetRemoteAddr()
		} else {
			ret += "@" + c.ctx.GetRemoteAddr()
		}
	}
	return ret
}

func (ws *WsServer) KickUserConn(client *Client) error {
	ws.clients.DeleteClients(client.UserID, []*Client{client})
	return client.KickOnlineMessage()
}

func (ws *WsServer) multiTerminalLoginChecker(clientOK bool, oldClients []*Client, newClient *Client) {
	kickTokenFunc := func(kickClients []*Client) {
		var kickTokens []string
		ws.clients.DeleteClients(newClient.UserID, kickClients)
		for _, c := range kickClients {
			kickTokens = append(kickTokens, c.token)
			err := c.KickOnlineMessage()
			if err != nil {
				log.ZWarn(c.ctx, "KickOnlineMessage", err)
			}
		}
		ctx := mcontext.WithMustInfoCtx(
			[]string{newClient.ctx.GetOperationID(), newClient.ctx.GetUserID(),
				constant.PlatformIDToName(newClient.PlatformID), newClient.ctx.GetConnID()},
		)
		if err := ws.authClient.KickTokens(ctx, kickTokens); err != nil {
			log.ZWarn(newClient.ctx, "kickTokens err", err)
		}
	}

	// If reconnect: When multiple msgGateway instances are deployed, a client may disconnect from instance A and reconnect to instance B.
	// During this process, instance A might still be executing, resulting in two clients with the same token existing simultaneously.
	// This situation needs to be filtered to prevent duplicate clients.
	checkSameTokenFunc := func(oldClients []*Client) []*Client {
		var clientsNeedToKick []*Client

		for _, c := range oldClients {
			if c.token == newClient.token {
				log.ZDebug(newClient.ctx, "token is same, not kick",
					"userID", newClient.UserID,
					"platformID", newClient.PlatformID,
					"token", newClient.token)
				continue
			}

			clientsNeedToKick = append(clientsNeedToKick, c)
		}

		return clientsNeedToKick
	}

	switch ws.msgGatewayConfig.Share.MultiLogin.Policy {
	case constant.DefalutNotKick:
	case constant.PCAndOther:
		if constant.PlatformIDToClass(newClient.PlatformID) == constant.TerminalPC {
			return
		}
		clients, ok := ws.clients.GetAll(newClient.UserID)
		clientOK = ok
		oldClients = make([]*Client, 0, len(clients))
		for _, c := range clients {
			if constant.PlatformIDToClass(c.PlatformID) == constant.TerminalPC {
				continue
			}
			oldClients = append(oldClients, c)
		}

		fallthrough
	case constant.AllLoginButSameTermKick:
		if !clientOK {
			return
		}

		oldClients = checkSameTokenFunc(oldClients)

		ws.clients.DeleteClients(newClient.UserID, oldClients)
		for _, c := range oldClients {
			err := c.KickOnlineMessage()
			if err != nil {
				log.ZWarn(c.ctx, "KickOnlineMessage", err)
			}
		}

		ctx := mcontext.WithMustInfoCtx(
			[]string{newClient.ctx.GetOperationID(), newClient.ctx.GetUserID(),
				constant.PlatformIDToName(newClient.PlatformID), newClient.ctx.GetConnID()},
		)
		req := &pbAuth.InvalidateTokenReq{
			PreservedToken: newClient.token,
			UserID:         newClient.UserID,
			PlatformID:     int32(newClient.PlatformID),
		}
		if err := ws.authClient.InvalidateToken(ctx, req); err != nil {
			log.ZWarn(newClient.ctx, "InvalidateToken err", err, "userID", newClient.UserID,
				"platformID", newClient.PlatformID)
		}
	case constant.AllLoginButSameClassKick:
		clients, ok := ws.clients.GetAll(newClient.UserID)
		if !ok {
			return
		}

		var kickClients []*Client
		for _, client := range clients {
			if constant.PlatformIDToClass(client.PlatformID) == constant.PlatformIDToClass(newClient.PlatformID) {
				{
					kickClients = append(kickClients, client)
				}
			}
		}
		kickClients = checkSameTokenFunc(kickClients)

		kickTokenFunc(kickClients)
	}
}

func (ws *WsServer) unregisterClient(client *Client) {
	defer ws.clientPool.Put(client)
	isDeleteUser := ws.clients.DeleteClients(client.UserID, []*Client{client})
	if isDeleteUser {
		ws.onlineUserNum.Add(-1)
		prommetrics.OnlineUserGauge.Dec()
	}
	ws.onlineUserConnNum.Add(-1)
	ws.subscription.DelClient(client)
	//ws.SetUserOnlineStatus(client.ctx, client, constant.Offline)
	log.ZDebug(client.ctx, "user offline", "close reason", client.closedErr, "online user Num",
		ws.onlineUserNum.Load(), "online user conn Num",
		ws.onlineUserConnNum.Load(),
	)
}

// validateRespWithRequest checks if the response matches the expected userID and platformID.
func (ws *WsServer) validateRespWithRequest(ctx *UserConnContext, resp *pbAuth.ParseTokenResp) error {
	userID := ctx.GetUserID()
	platformID := int32(ctx.GetPlatformID())
	if resp.UserID != userID {
		return servererrs.ErrTokenInvalid.WrapMsg(fmt.Sprintf("token uid %s != userID %s", resp.UserID, userID))
	}
	if resp.PlatformID != platformID {
		return servererrs.ErrTokenInvalid.WrapMsg(fmt.Sprintf("token platform %d != platformID %d", resp.PlatformID, platformID))
	}
	return nil
}

func (ws *WsServer) handlerError(ctx *UserConnContext, w http.ResponseWriter, r *http.Request, err error) {
	if !ctx.ShouldSendResp() {
		httpError(ctx, err)
		return
	}
	// the browser cannot get the response of upgrade failure
	data, err := json.Marshal(apiresp.ParseError(err))
	if err != nil {
		log.ZError(ctx, "json marshal failed", err)
		return
	}
	conn, upgradeErr := ws.websocket.Upgrade(w, r, nil)
	if upgradeErr != nil {
		log.ZWarn(ctx, "websocket upgrade failed", upgradeErr, "respErr", err, "resp", string(data))
		return
	}
	defer conn.Close()
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		log.ZWarn(ctx, "WriteMessage failed", err, "respErr", err, "resp", string(data))
		return
	}
}

func (ws *WsServer) wsHandler(w http.ResponseWriter, r *http.Request) {
	// Create a new connection context
	connContext := newContext(w, r)

	// Check if the current number of online user connections exceeds the maximum limit
	if ws.onlineUserConnNum.Load() >= ws.wsMaxConnNum {
		// If it exceeds the maximum connection number, return an error via HTTP and stop processing
		ws.handlerError(connContext, w, r, servererrs.ErrConnOverMaxNumLimit.WrapMsg("over max conn num limit"))
		return
	}

	// Parse essential arguments (e.g., user ID, Token)
	err := connContext.ParseEssentialArgs()
	if err != nil {
		// If there's an error during parsing, return an error via HTTP and stop processing
		ws.handlerError(connContext, w, r, err)
		return
	}

	// Call the authentication client to parse the Token obtained from the context
	resp, err := ws.authClient.ParseToken(connContext, connContext.GetToken())
	if err != nil {
		ws.handlerError(connContext, w, r, err)
		return
	}

	// Validate the authentication response matches the request (e.g., user ID and platform ID)
	err = ws.validateRespWithRequest(connContext, resp)
	if err != nil {
		// If validation fails, return an error via HTTP and stop processing
		ws.handlerError(connContext, w, r, err)
		return
	}
	conn, err := ws.websocket.Upgrade(w, r, nil)
	if err != nil {
		log.ZWarn(connContext, "websocket upgrade failed", err)
		return
	}
	if connContext.ShouldSendResp() {
		if err := conn.WriteMessage(websocket.TextMessage, wsSuccessResponse); err != nil {
			log.ZWarn(connContext, "WriteMessage first response", err)
			return
		}
	}

	log.ZDebug(connContext, "new conn", "token", connContext.GetToken())

	var pingInterval time.Duration
	if connContext.GetPlatformID() == constant.WebPlatformID {
		pingInterval = pingPeriod
	}

	client := new(Client)
	client.ResetClient(connContext, NewWebSocketClientConn(conn, maxMessageSize, pongWait, pingInterval), ws)

	// Register the client with the server and start message processing
	ws.registerChan <- client
	go client.readMessage()
}
