package dify

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

type ChatMessageStreamResponse struct {
	Event          string `json:"event"`
	TaskID         string `json:"task_id"`
	ID             string `json:"id"`
	Answer         string `json:"answer"`
	CreatedAt      int64  `json:"created_at"`
	ConversationID string `json:"conversation_id"`
	Data           struct {
		Id             string `json:"id"`
		WorkflowId     string `json:"workflow_id"`
		SequenceNumber int    `json:"sequence_number"`
		Inputs         struct {
			SysQuery          string        `json:"sys.query"`
			SysFiles          []interface{} `json:"sys.files"`
			SysConversationId string        `json:"sys.conversation_id"`
			SysUserId         string        `json:"sys.user_id"`
			SysDialogueCount  int           `json:"sys.dialogue_count"`
			SysAppId          string        `json:"sys.app_id"`
			SysWorkflowId     string        `json:"sys.workflow_id"`
			SysWorkflowRunId  string        `json:"sys.workflow_run_id"`
		} `json:"inputs"`
		CreatedAt int `json:"created_at"`
	} `json:"data"`
}

// type ChatMessageStreamResponse struct {
// 	Event          string `json:"event"`
// 	ConversationId string `json:"conversation_id"`
// 	MessageId      string `json:"message_id"`
// 	CreatedAt      int    `json:"created_at"`
// 	TaskId         string `json:"task_id"`
// 	WorkflowRunId  string `json:"workflow_run_id"`
// 	Data           struct {
// 		Id             string `json:"id"`
// 		WorkflowId     string `json:"workflow_id"`
// 		SequenceNumber int    `json:"sequence_number"`
// 		Inputs         struct {
// 			SysQuery          string        `json:"sys.query"`
// 			SysFiles          []interface{} `json:"sys.files"`
// 			SysConversationId string        `json:"sys.conversation_id"`
// 			SysUserId         string        `json:"sys.user_id"`
// 			SysDialogueCount  int           `json:"sys.dialogue_count"`
// 			SysAppId          string        `json:"sys.app_id"`
// 			SysWorkflowId     string        `json:"sys.workflow_id"`
// 			SysWorkflowRunId  string        `json:"sys.workflow_run_id"`
// 		} `json:"inputs"`
// 		CreatedAt int `json:"created_at"`
// 	} `json:"data"`
// }

type ChatMessageStreamChannelResponse struct {
	ChatMessageStreamResponse
	Err error `json:"-"`
}

func (api *API) ChatMessagesStreamRaw(ctx context.Context, req *ChatMessageRequest) (*http.Response, error) {
	req.ResponseMode = "streaming"

	httpReq, err := api.createBaseRequest(ctx, http.MethodPost, "/v1/chat-messages", req)
	if err != nil {
		return nil, err
	}
	return api.c.sendRequest(httpReq)
}

func (api *API) ChatMessagesStream(ctx context.Context, req *ChatMessageRequest) (chan ChatMessageStreamChannelResponse, error) {
	httpResp, err := api.ChatMessagesStreamRaw(ctx, req)
	if err != nil {
		return nil, err
	}

	streamChannel := make(chan ChatMessageStreamChannelResponse)
	go api.chatMessagesStreamHandle(ctx, httpResp, streamChannel)
	return streamChannel, nil
}

func (api *API) chatMessagesStreamHandle(ctx context.Context, resp *http.Response, streamChannel chan ChatMessageStreamChannelResponse) {
	defer resp.Body.Close()
	defer close(streamChannel)

	reader := bufio.NewReader(resp.Body)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			line, err := reader.ReadBytes('\n')
			if err != nil {
				streamChannel <- ChatMessageStreamChannelResponse{
					Err: fmt.Errorf("error reading line: %w", err),
				}
				return
			}
			fmt.Println("read_line", string(line))

			if !bytes.HasPrefix(line, []byte("data:")) {
				continue
			}
			line = bytes.TrimPrefix(line, []byte("data:"))

			var resp ChatMessageStreamChannelResponse
			if err = json.Unmarshal(line, &resp); err != nil {
				streamChannel <- ChatMessageStreamChannelResponse{
					Err: fmt.Errorf("error unmarshalling event: %w", err),
				}
				continue
			} else if resp.Event == "error" {
				streamChannel <- ChatMessageStreamChannelResponse{
					Err: errors.New("error streaming event: " + string(line)),
				}
				return
			} else if resp.Event == "message" && resp.Answer == "" {
				streamChannel <- resp
				return
			}
			streamChannel <- resp
		}
	}
}
