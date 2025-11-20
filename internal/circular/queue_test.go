package circular

import (
	"reflect"
	"testing"
)

func TestQueue_NewAndCapacity(t *testing.T) {
	b := New[int](5)
	if b.Cap() != 5 {
		t.Errorf("expected capacity 5, got %d", b.Cap())
	}
	if b.Len() != 0 {
		t.Errorf("expected length 0, got %d", b.Len())
	}
}

func TestQueue_PushPull(t *testing.T) {
	b := New[int](3)

	// Push elements
	if !b.Push(1) {
		t.Error("expected push to succeed")
	}
	if !b.Push(2) {
		t.Error("expected push to succeed")
	}
	if !b.Push(3) {
		t.Error("expected push to succeed")
	}

	if b.Len() != 3 {
		t.Errorf("expected length 3, got %d", b.Len())
	}

	// Queue should be full
	if !b.IsFull() {
		t.Error("expected queue to be full")
	}
	if b.Push(4) {
		t.Error("expected push to fail when full")
	}

	// Pull elements in FIFO order
	val, ok := b.Pull()
	if !ok || val != 1 {
		t.Errorf("expected 1, got %d, ok=%v", val, ok)
	}

	val, ok = b.Pull()
	if !ok || val != 2 {
		t.Errorf("expected 2, got %d, ok=%v", val, ok)
	}

	val, ok = b.Pull()
	if !ok || val != 3 {
		t.Errorf("expected 3, got %d, ok=%v", val, ok)
	}

	// Queue should be empty
	if !b.IsEmpty() {
		t.Error("expected queue to be empty")
	}

	val, ok = b.Pull()
	if ok {
		t.Errorf("expected pull to fail when empty, got %d", val)
	}
}

func TestQueue_WrapAround(t *testing.T) {
	b := New[int](3)

	// Fill the queue
	b.Push(1)
	b.Push(2)
	b.Push(3)

	// Pull two elements
	b.Pull()
	b.Pull()

	// Push two more (will wrap around)
	if !b.Push(4) {
		t.Error("expected push to succeed after wrap")
	}
	if !b.Push(5) {
		t.Error("expected push to succeed after wrap")
	}

	// Check FIFO order
	val, _ := b.Pull()
	if val != 3 {
		t.Errorf("expected 3, got %d", val)
	}
	val, _ = b.Pull()
	if val != 4 {
		t.Errorf("expected 4, got %d", val)
	}
	val, _ = b.Pull()
	if val != 5 {
		t.Errorf("expected 5, got %d", val)
	}
}

func TestQueue_All(t *testing.T) {
	b := New[int](5)
	b.Push(10)
	b.Push(20)
	b.Push(30)

	expected := []int{10, 20, 30}
	i := 0
	for index, value := range b.All() {
		if index != i {
			t.Errorf("expected index %d, got %d", i, index)
		}
		if value != expected[i] {
			t.Errorf("expected value %d, got %d", expected[i], value)
		}
		i++
	}

	if i != 3 {
		t.Errorf("expected 3 iterations, got %d", i)
	}
}

func TestQueue_DeleteFunc(t *testing.T) {
	b := New[int](7)
	b.Push(0)
	b.Push(1)
	b.Push(2)
	b.Push(3)
	b.Push(4)
	b.Push(5)
	b.Push(6)

	// Remove even numbers
	removed := []int{}
	for val := range b.DeleteFunc(func(val int) bool { return val%2 == 0 }) {
		removed = append(removed, val)
	}

	if !reflect.DeepEqual([]int{0, 2, 4, 6}, removed) {
		t.Errorf("expected [0,2,4,6] removed, found %v", removed)
	}

	if b.Len() != 3 {
		t.Errorf("expected length 3, got %d", b.Len())
	}

	expected := []int{1, 3, 5}
	for i, val := range b.All() {
		if i >= len(expected) {
			t.Errorf("unexpected extra value at index %d: %d", i, val)
			break
		}
		if val != expected[i] {
			t.Errorf("iteration %d: expected %d, got %d", i, expected[i], val)
		}
	}
	if b.Len() != len(expected) {
		t.Errorf("expected %d values, got %d", len(expected), b.Len())
	}
}

func TestQueue_Clear(t *testing.T) {
	b := New[int](3)
	b.Push(1)
	b.Push(2)
	b.Push(3)

	cleared := []int{}
	for val := range b.Clear() {
		cleared = append(cleared, val)
	}

	if !reflect.DeepEqual([]int{1, 2, 3}, cleared) {
		t.Errorf("expected [0,2,4,6] removed, found %v", cleared)
	}

	if !b.IsEmpty() {
		t.Error("expected queue to be empty after clear")
	}

	if b.Len() != 0 {
		t.Errorf("expected length 0, got %d", b.Len())
	}
}

func TestQueue_DeleteFuncWithWrapAround(t *testing.T) {
	b := New[int](5)

	// Fill queue
	for i := 1; i <= 5; i++ {
		b.Push(i)
	}

	// Pull some to create wrap-around
	b.Pull()
	b.Pull()

	// Add more elements
	b.Push(6)
	b.Push(7)

	// Now queue has: [3, 4, 5, 6, 7] with wrap-around in physical array

	// Remove values divisible by 3
	removed := []int{}
	for val := range b.DeleteFunc(func(val int) bool { return val%3 == 0 }) {
		removed = append(removed, val)
	}

	// Should have removed 3 and 6
	if len(removed) != 2 {
		t.Errorf("expected 2 removed, got %d", len(removed))
	}

	// Should have: [4, 5, 7] (removed 3 and 6)
	if b.Len() != 3 {
		t.Errorf("expected length 3, got %d", b.Len())
	}

	expected := []int{4, 5, 7}
	for i, val := range b.All() {
		if i >= len(expected) {
			t.Errorf("unexpected extra value at index %d: %d", i, val)
			break
		}
		if val != expected[i] {
			t.Errorf("iteration %d: expected %d, got %d", i, expected[i], val)
		}
	}
	if b.Len() != len(expected) {
		t.Errorf("expected %d values, got %d", len(expected), b.Len())
	}
}
