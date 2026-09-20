package checkpoint

import (
	"encoding/json"
	"errors"
)

// 可区分的失败原因。
var (
	// ErrWriteFailed 检查点写入失败（原子写保证不会留下半更新状态）。
	ErrWriteFailed = errors.New("checkpoint write failed")
	// ErrCorrupt 检查点损坏（校验和不匹配 / 截断 / 格式错误）。
	ErrCorrupt = errors.New("checkpoint corrupt")
	// ErrNoCheckpoint 不存在检查点（首次启动）。
	ErrNoCheckpoint = errors.New("no checkpoint exists")
)

// PendingOutput 是已写入检查点但尚未确认投递的输出。
type PendingOutput struct {
	Seq             int64  `json:"seq"`
	Key             string `json:"key"`
	WindowID        string `json:"window_id"`
	Kind            string `json:"kind"`
	IsCorrection    bool   `json:"is_correction"`
	Value           int64  `json:"value"`
	EmittedAtUnixMs int64  `json:"emitted_at_unix_ms"`
}

// State 是一次检查点的完整快照。
type State struct {
	Version         int                        `json:"version"`
	WatermarkUnixMs int64                      `json:"watermark_unix_ms"`
	KeyStates       map[string]json.RawMessage `json:"key_states"`
	NextOutputSeq   int64                      `json:"next_output_seq"`
	CommittedSeq    int64                      `json:"committed_seq"`
	Pending         []PendingOutput            `json:"pending"`
	SpillFiles      []string                   `json:"spill_files"`
	CreatedAtUnixMs int64                      `json:"created_at_unix_ms"`
}

// Store 是检查点存储抽象。
type Store interface {
	Save(state *State) error
	Load() (*State, error)
	Clear() error
}
