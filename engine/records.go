package engine

// 以下方法要求调用方持有 e.mu（屏障/批次串行期间记录，保证顺序确定）。

func (e *Engine) recordInvalid(r InvalidEventRecord) {
	e.invalidEvents = append(e.invalidEvents, r)
}

func (e *Engine) recordLate(r LateEventRecord) {
	e.latedEvents = append(e.latedEvents, r)
}

func (e *Engine) recordSide(r SideOutputRecord) {
	e.sideOutputs = append(e.sideOutputs, r)
}

// InvalidEventsCount / LateEventsCount / SideOutputsCount 供测试断言。
func (e *Engine) InvalidEventsCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.invalidEvents)
}

func (e *Engine) LateEventsCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.latedEvents)
}

func (e *Engine) SideOutputsCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.sideOutputs)
}

// policyString 暴露校验器当前策略名称（用于异常事件记录）。
func (e *Engine) policyString() string {
	if e.cfg.RejectInvalidTime {
		return "reject"
	}
	return "assign-current-time"
}
