package policy

import (
	"time"

	"github.com/jolynch/pinch/internal/filexfer/codec"
)

type CompressionMode uint8

const (
	CompressionModeZstdDefault CompressionMode = iota
	CompressionModeZstdLevel1
	CompressionModeLz4
	CompressionModeNone
)

const (
	emaAlpha             = 0.80
	hystStreak           = 2
	downRatioCut         = 0.90
	downReadOverWriteCut = 1.10
	upReadOverWriteCut   = 0.25
)

type CompressionMetrics struct {
	LogicalSize    int64
	WireSize       int64
	PrepareLatency time.Duration
	WriteLatency   time.Duration
}

type CompressionDecision struct {
	Next          CompressionMode
	Reason        string
	Ratio         float64
	ReadOverWrite float64
}

type CompressionPolicy struct {
	emaRatio         float64
	emaReadOverWrite float64
	emaInitialized   bool
	downgradeStreak  int
	upgradeStreak    int
}

func NewCompressionPolicy() *CompressionPolicy {
	return &CompressionPolicy{}
}

func (p *CompressionPolicy) Decide(current CompressionMode, m CompressionMetrics) CompressionDecision {
	ratio := compressionRatio(m.LogicalSize, m.WireSize)
	readOverWrite := safeLatencyRatio(m.PrepareLatency, m.WriteLatency)
	p.record(ratio, readOverWrite)
	avgRatio := p.emaRatio
	avgReadOverWrite := p.emaReadOverWrite

	upgrade := shouldUpgrade(avgReadOverWrite)
	downgrade := !upgrade && (avgRatio < downRatioCut || avgReadOverWrite > downReadOverWriteCut)

	if downgrade {
		p.downgradeStreak++
		p.upgradeStreak = 0
	} else if upgrade {
		p.upgradeStreak++
		p.downgradeStreak = 0
	} else {
		p.downgradeStreak = 0
		p.upgradeStreak = 0
	}

	decision := CompressionDecision{
		Next:          current,
		Reason:        "hold",
		Ratio:         avgRatio,
		ReadOverWrite: avgReadOverWrite,
	}
	if p.downgradeStreak >= hystStreak {
		next := downgradeMode(current)
		if next != current {
			decision.Next = next
			decision.Reason = "downgrade"
		}
		p.downgradeStreak = 0
	}
	if p.upgradeStreak >= hystStreak {
		next := upgradeMode(current)
		if next != current {
			decision.Next = next
			decision.Reason = "upgrade"
		}
		p.upgradeStreak = 0
	}
	return decision
}

func (p *CompressionPolicy) record(ratio float64, readOverWrite float64) {
	if p == nil {
		return
	}
	if !p.emaInitialized {
		p.emaRatio = ratio
		p.emaReadOverWrite = readOverWrite
		p.emaInitialized = true
		return
	}
	p.emaRatio = (emaAlpha * ratio) + ((1 - emaAlpha) * p.emaRatio)
	p.emaReadOverWrite = (emaAlpha * readOverWrite) + ((1 - emaAlpha) * p.emaReadOverWrite)
}

func safeLatencyRatio(prepare time.Duration, write time.Duration) float64 {
	if write <= 0 {
		if prepare <= 0 {
			return 1.0
		}
		return 9999.0
	}
	if prepare <= 0 {
		return 0.0
	}
	return float64(prepare) / float64(write)
}

func shouldUpgrade(avgReadOverWrite float64) bool {
	if avgReadOverWrite >= upReadOverWriteCut {
		return false
	}
	// "Use stronger compression" is controlled purely by latency signal.
	return true
}

func downgradeMode(current CompressionMode) CompressionMode {
	switch current {
	case CompressionModeZstdDefault:
		return CompressionModeZstdLevel1
	case CompressionModeZstdLevel1:
		return CompressionModeLz4
	case CompressionModeLz4:
		return CompressionModeNone
	default:
		return CompressionModeNone
	}
}

func upgradeMode(current CompressionMode) CompressionMode {
	switch current {
	case CompressionModeNone:
		return CompressionModeLz4
	case CompressionModeLz4:
		return CompressionModeZstdLevel1
	default:
		return CompressionModeZstdLevel1
	}
}

func CompressionModeFromStored(raw uint8) CompressionMode {
	mode := CompressionMode(raw)
	switch mode {
	case CompressionModeZstdDefault, CompressionModeZstdLevel1, CompressionModeLz4, CompressionModeNone:
		return mode
	default:
		return CompressionModeNone
	}
}

func FrameCompTokenForMode(mode CompressionMode) string {
	switch mode {
	case CompressionModeLz4:
		return codec.EncodingLz4
	case CompressionModeNone:
		return "none"
	default:
		return codec.EncodingZstd
	}
}

func compressionRatio(logicalSize int64, wireSize int64) float64 {
	if wireSize <= 0 {
		return 0
	}
	return float64(logicalSize) / float64(wireSize)
}
