package aggregate

// Registry 按名称查找聚合器。
type Registry struct{ m map[string]Aggregator }

// NewRegistry 创建注册表并注册内置聚合器。
func NewRegistry() *Registry {
	return &Registry{m: map[string]Aggregator{
		"sum_count": SumCount{},
	}}
}

// Get 按名称获取聚合器。
func (r *Registry) Get(name string) (Aggregator, bool) {
	a, ok := r.m[name]
	return a, ok
}
