package checkpoint

import "encoding/json"

// jsonMarshalState / jsonUnmarshalState 供内存存储复用标准 JSON 编解码。
func jsonMarshalState(state *State) ([]byte, error) {
	return json.Marshal(state)
}

func jsonUnmarshalState(data []byte) (*State, error) {
	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, ErrCorrupt
	}
	return &st, nil
}
