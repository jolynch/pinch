package filexfer

import (
	"math"
	"testing"
	"time"
)

func pushDecision(p *CompressionPolicy, current CompressionMode, ratio float64, readOverWrite float64) CompressionDecision {
	logical := int64(1000)
	wire := int64(float64(logical) / ratio)
	if wire <= 0 {
		wire = 1
	}
	write := 100 * time.Millisecond
	prepare := time.Duration(readOverWrite * float64(write))
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
		d := pushDecision(p, mode, 1.20, 0.10)
		if d.Next != mode {
			mode = d.Next
		}
	}
	if mode != CompressionModeZstdLevel1 {
		t.Fatalf("expected mode to upgrade to zstd level1, got %v", mode)
	}
}

func TestCompressionPolicyCanUpgradeFromNoneWithLatencySignalOnly(t *testing.T) {
	p := NewCompressionPolicy()
	mode := CompressionModeNone

	// While in "none", measured ratio is ~1.0 by definition, so upgrade must rely on latency signal.
	for i := 0; i < 4; i++ {
		d := pushDecision(p, mode, 1.0, 0.10)
		if d.Next != mode {
			mode = d.Next
		}
	}
	if mode == CompressionModeNone {
		t.Fatalf("expected mode to upgrade from none when write dominates prepare time")
	}
}

func TestCompressionPolicyUpgradeIgnoresRatio(t *testing.T) {
	p := NewCompressionPolicy()
	mode := CompressionModeLz4

	for i := 0; i < 4; i++ {
		d := pushDecision(p, mode, 0.80, 0.10)
		if d.Next != mode {
			mode = d.Next
		}
	}
	if mode != CompressionModeZstdLevel1 {
		t.Fatalf("expected upgrade to zstd despite poor ratio, got %v", mode)
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

func TestCompressionPolicyEMAInitializesFromFirstSample(t *testing.T) {
	p := NewCompressionPolicy()
	d := pushDecision(p, CompressionModeZstdLevel1, 1.25, 0.60)
	if !nearlyEqual(d.Ratio, 1.25) {
		t.Fatalf("expected first EMA ratio to equal sample, got %.6f", d.Ratio)
	}
	if !nearlyEqual(d.ReadOverWrite, 0.60) {
		t.Fatalf("expected first EMA read/write to equal sample, got %.6f", d.ReadOverWrite)
	}
}

func TestCompressionPolicyEMAAppliesAlphaOnSecondSample(t *testing.T) {
	p := NewCompressionPolicy()
	_ = pushDecision(p, CompressionModeZstdLevel1, 1.25, 0.60)
	d := pushDecision(p, CompressionModeZstdLevel1, 0.75, 1.20)

	firstSampleRatio := quantizedRatioFromInput(1.25)
	secondSampleRatio := quantizedRatioFromInput(0.75)
	wantRatio := (emaAlpha * secondSampleRatio) + ((1 - emaAlpha) * firstSampleRatio)
	wantReadOverWrite := (emaAlpha * 1.20) + ((1 - emaAlpha) * 0.60)
	if !nearlyEqual(d.Ratio, wantRatio) {
		t.Fatalf("unexpected EMA ratio: got %.6f want %.6f", d.Ratio, wantRatio)
	}
	if !nearlyEqual(d.ReadOverWrite, wantReadOverWrite) {
		t.Fatalf("unexpected EMA read/write: got %.6f want %.6f", d.ReadOverWrite, wantReadOverWrite)
	}
}

func nearlyEqual(got, want float64) bool {
	const epsilon = 1e-9
	return math.Abs(got-want) < epsilon
}

func quantizedRatioFromInput(input float64) float64 {
	logical := int64(1000)
	wire := int64(float64(logical) / input)
	if wire <= 0 {
		wire = 1
	}
	return compressionRatio(logical, wire)
}
