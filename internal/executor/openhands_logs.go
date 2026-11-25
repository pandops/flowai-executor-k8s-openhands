package executor

import "strings"

type publishedLog struct {
	stream  string
	content string
}

// publishedLogsFromOpenHands extracts only text OpenHands explicitly publishes.
// Assistant messages are labelled reasoning in the operator UI; tool and
// observation output is work. Status and protocol metadata are ignored.
func publishedLogsFromOpenHands(value any) []publishedLog {
	logs := make([]publishedLog, 0)
	seen := make(map[string]struct{})
	appendLog := func(stream, content string) {
		content = strings.TrimSpace(content)
		if content == "" {
			return
		}
		fingerprint := stream + "\x00" + content
		if _, exists := seen[fingerprint]; exists {
			return
		}
		seen[fingerprint] = struct{}{}
		logs = append(logs, publishedLog{stream: stream, content: content})
	}
	collectPublishedLogs(value, "", appendLog)
	return logs
}

// toolCallLogs remembers, for one claim, the logs each OpenHands tool call
// already published. OpenHands repeats an action's text in its observation
// (a terminal command, a finish message), so a log the same tool_call_id
// already published is dropped; a new tool call with the same text, such as
// a command run again, is published again. Only the claim's frame loop uses it.
type toolCallLogs map[string]struct{}

// unpublished returns the frame's logs that its tool call has not published yet.
func (seen toolCallLogs) unpublished(frame any, logs []publishedLog) []publishedLog {
	object, _ := frame.(map[string]any)
	callID := firstString(object, "tool_call_id")
	if callID == "" {
		return logs
	}
	out := make([]publishedLog, 0, len(logs))
	for _, l := range logs {
		key := callID + "\x00" + l.stream + "\x00" + l.content
		if _, done := seen[key]; done {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, l)
	}
	return out
}

func collectPublishedLogs(value any, contextKind string, appendLog func(string, string)) {
	object, ok := value.(map[string]any)
	if !ok {
		if list, listOK := value.([]any); listOK {
			for _, item := range list {
				collectPublishedLogs(item, contextKind, appendLog)
			}
		}
		return
	}

	kind := strings.ToLower(firstString(object, "kind", "type", "event_type"))
	role := strings.ToLower(firstString(object, "role", "source"))
	if role == "assistant" || role == "agent" {
		appendTextContent(object["content"], "reasoning", appendLog)
		appendTextContent(object["message"], "reasoning", appendLog)
	}
	for _, key := range []string{"reasoning", "reasoning_content", "thought"} {
		appendTextContent(object[key], "reasoning", appendLog)
	}

	workContext := strings.Contains(kind, "observation") || strings.Contains(kind, "tool") ||
		strings.Contains(kind, "action") || strings.Contains(kind, "command") || strings.Contains(kind, "terminal") ||
		strings.Contains(contextKind, "observation") || strings.Contains(contextKind, "tool") || strings.Contains(contextKind, "action")
	if workContext {
		for _, key := range []string{"content", "action", "observation", "output", "stdout", "stderr", "message"} {
			appendTextContent(object[key], "work", appendLog)
		}
	}

	for key, nested := range object {
		switch key {
		case "content", "message", "reasoning", "reasoning_content", "thought", "action", "observation", "output", "stdout", "stderr":
			continue
		}
		collectPublishedLogs(nested, kind, appendLog)
	}
}

func appendTextContent(value any, stream string, appendLog func(string, string)) {
	switch typed := value.(type) {
	case string:
		appendLog(stream, typed)
	case []any:
		for _, part := range typed {
			appendTextContent(part, stream, appendLog)
		}
	case map[string]any:
		if text, ok := typed["text"].(string); ok {
			appendLog(stream, text)
		}
		for _, key := range []string{"content", "command", "output", "stdout", "stderr", "message", "summary"} {
			appendTextContent(typed[key], stream, appendLog)
		}
	}
}

func firstString(object map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := object[key].(string); ok {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
