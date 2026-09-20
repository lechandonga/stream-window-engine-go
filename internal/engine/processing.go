package engine

import (
	"context"
	"hash/fnv"
	"sync"

	"github.com/lechandonga/stream-window-engine-go/internal/sink"
)

// microBatch 是并发模型的屏障大小。
const microBatch = 32

// writeBatch 并发写入一个已预处理微批：
// 按分组键哈希分片到多个 worker，同键事件永远在同一 worker 保序处理；
// 全部写完（屏障）后主循环才触发窗口与检查点。
// 由于触发顺序由确定性键序决定、水位线在预处理阶段已冻结，
// 无论并发度多少，输出序列都与串行处理完全一致。
func (e *Engine) writeBatch(ctx context.Context, ready []prepared, dst sink.Sink) error {
	n := e.cfg.Concurrency
	if n <= 1 {
		for _, p := range ready {
			if err := e.writeEvent(ctx, p, dst); err != nil {
				return err
			}
		}
		return nil
	}

	shards := make([][]prepared, n)
	for _, p := range ready {
		h := fnv.New32a()
		_, _ = h.Write([]byte(p.ev.Key))
		idx := int(h.Sum32()) % n
		shards[idx] = append(shards[idx], p)
	}

	var (
		wg      sync.WaitGroup
		errOnce error
		once    sync.Once
	)
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for i := 0; i < n; i++ {
		if len(shards[i]) == 0 {
			continue
		}
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for _, p := range shards[idx] {
				if err := e.writeEvent(cctx, p, dst); err != nil {
					once.Do(func() {
						errOnce = err
						cancel()
					})
					return
				}
			}
		}(i)
	}
	wg.Wait()
	return errOnce
}
