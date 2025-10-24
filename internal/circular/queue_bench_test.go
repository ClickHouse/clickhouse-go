package circular

import (
	"testing"
)

// Benchmarks for the circular queue implementation.
//
// The circular queue maintains constant memory by reusing the same underlying array.

// BenchmarkQueue_MemoryConstancy demonstrates that the circular queue
// maintains constant memory usage as elements are pushed and pulled,
// without reallocating the underlying array.
func BenchmarkQueue_MemoryConstancy(b *testing.B) {
	const capacity = 1000
	queue := New[int](capacity)

	// Fill the queue initially
	for i := 0; i < capacity; i++ {
		queue.Push(i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		queue.Pull()
		queue.Push(i)
	}
}

// BenchmarkQueue_PushPull measures push/pull performance
func BenchmarkQueue_PushPull(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"Size10", 10},
		{"Size100", 100},
		{"Size1000", 1000},
		{"Size10000", 10000},
	}

	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			queue := New[int](s.size)

			// Fill queue to half capacity
			for i := 0; i < s.size/2; i++ {
				queue.Push(i)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				queue.Push(i)
				queue.Pull()
			}
		})
	}
}

// BenchmarkQueue_Compact measures compaction performance
func BenchmarkQueue_Compact(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"Size100", 100},
		{"Size1000", 1000},
		{"Size10000", 10000},
	}

	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				queue := New[int](s.size)
				for j := 0; j < s.size; j++ {
					queue.Push(j)
				}
				b.StartTimer()

				// Remove even numbers
				for range queue.Compact(func(val int) bool { return val%2 == 0 }) {
				}
			}
		})
	}
}

// BenchmarkQueue_All measures iteration performance
func BenchmarkQueue_All(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"Size100", 100},
		{"Size1000", 1000},
		{"Size10000", 10000},
	}

	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			queue := New[int](s.size)
			for i := 0; i < s.size; i++ {
				queue.Push(i)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				for range queue.All() {
				}
			}
		})
	}
}
