package filexfer

import (
	"testing"
	"time"
)

func pushDecision(p *CompressionPolicy, current CompressionMode, ratio float64, prepareOverWrite float64) CompressionDecision {
	logical := int64(1000)
	wire := int64(float64(logical) / ratio)
	if wire <= 0 {
		wire = 1
	}
	write := 100 * time.Millisecond
	prepare := time.Duration(prepareOverWrite * float64(write))
	return p.Decide(current, CompressionMetrics{
		LogicalSize:    logical,
		WireSize:       wire,
		PrepareLatency: prepare,
		WriteLatency:   write,
	})
}

func TestCompressionPolicyDowngradesStepwise(t *testing.T) {
	p := NewCompressionPolicy()
	mode := CompressionModeZstdDefault

	for i := 0; i < 8; i++ {
		d := pushDecision(p, mode, 0.80, 1.20)
		if d.Next != mode {
			mode = d.Next
		}
	}
	if mode != CompressionModeNone {
		t.Fatalf("expected mode to eventually downgrade to none, got %v", mode)
	}
}

func TestCompressionPolicyUpgradesStepwise(t *testing.T) {
	p := NewCompressionPolicy()
	mode := CompressionModeNone

	for i := 0; i < 10; i++ {
		d := pushDecision(p, mode, 1.20, 0.50)
		if d.Next != mode {
			mode = d.Next
		}
	}
	if mode != CompressionModeZstdDefault {
		t.Fatalf("expected mode to upgrade to zstd default, got %v", mode)
	}
}

func TestCompressionPolicyHysteresisPreventsFlap(t *testing.T) {
	p := NewCompressionPolicy()
	mode := CompressionModeZstdLevel1

	d1 := pushDecision(p, mode, 0.85, 1.20)
	if d1.Next != mode {
		t.Fatalf("expected first downgrade signal to hold, got %v", d1.Next)
	}
	d2 := pushDecision(p, mode, 1.20, 0.50)
	if d2.Next != mode {
		t.Fatalf("expected opposite signal to hold, got %v", d2.Next)
	}
}

func TestCompressionPolicyHandlesZeroLatency(t *testing.T) {
	p := NewCompressionPolicy()
	d := p.Decide(CompressionModeZstdDefault, CompressionMetrics{
		LogicalSize:    1000,
		WireSize:       900,
		PrepareLatency: 0,
		WriteLatency:   0,
	})
	if d.Next != CompressionModeZstdDefault {
		t.Fatalf("expected no change for zero-latency edge case, got %v", d.Next)
	}
}
