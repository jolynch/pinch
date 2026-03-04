package filexfer

import "time"

type CompressionMode uint8

const (
	CompressionModeZstdDefault CompressionMode = iota
	CompressionModeZstdLevel1
	CompressionModeLz4
	CompressionModeNone
)

const (
	compressionPolicyWindowSize             = 8
	compressionPolicyHysteresisStreak       = 2
	compressionPolicyDowngradeRatio         = 0.90
	compressionPolicyDowngradePrepareOverW  = 1.10
	compressionPolicyUpgradeRatio           = 1.05
	compressionPolicyUpgradePrepareOverW    = 0.70
)

type CompressionMetrics struct {
	LogicalSize     int64
	WireSize        int64
	PrepareLatency  time.Duration
	WriteLatency    time.Duration
}

type CompressionDecision struct {
	Next             CompressionMode
	Reason           string
	Ratio            float64
	PrepareOverWrite float64
}

type CompressionPolicy struct {
	ratios          []float64
	prepareOverWire []float64
	index           int
	size            int
	downgradeStreak int
	upgradeStreak   int
}

func NewCompressionPolicy() *CompressionPolicy {
	return &CompressionPolicy{
		ratios:          make([]float64, compressionPolicyWindowSize),
		prepareOverWire: make([]float64, compressionPolicyWindowSize),
	}
}

func (p *CompressionPolicy) Decide(current CompressionMode, m CompressionMetrics) CompressionDecision {
	ratio := compressionRatio(m.LogicalSize, m.WireSize)
	prepareOverWrite := safeLatencyRatio(m.PrepareLatency, m.WriteLatency)
	p.record(ratio, prepareOverWrite)
	avgRatio, avgPrepareOverWrite := p.averages()

	downgrade := avgRatio < compressionPolicyDowngradeRatio || avgPrepareOverWrite > compressionPolicyDowngradePrepareOverW
	upgrade := avgPrepareOverWrite < compressionPolicyUpgradePrepareOverW && avgRatio > compressionPolicyUpgradeRatio

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
		Next:             current,
		Reason:           "hold",
		Ratio:            avgRatio,
		PrepareOverWrite: avgPrepareOverWrite,
	}
	if p.downgradeStreak >= compressionPolicyHysteresisStreak {
		next := downgradeMode(current)
		if next != current {
			decision.Next = next
			decision.Reason = "downgrade"
		}
		p.downgradeStreak = 0
	}
	if p.upgradeStreak >= compressionPolicyHysteresisStreak {
		next := upgradeMode(current)
		if next != current {
			decision.Next = next
			decision.Reason = "upgrade"
		}
		p.upgradeStreak = 0
	}
	return decision
}

func (p *CompressionPolicy) record(ratio float64, prepareOverWrite float64) {
	if p == nil {
		return
	}
	p.ratios[p.index] = ratio
	p.prepareOverWire[p.index] = prepareOverWrite
	p.index = (p.index + 1) % compressionPolicyWindowSize
	if p.size < compressionPolicyWindowSize {
		p.size++
	}
}

func (p *CompressionPolicy) averages() (float64, float64) {
	if p == nil || p.size == 0 {
		return 0, 0
	}
	sumRatio := 0.0
	sumPrepareOverWrite := 0.0
	for i := 0; i < p.size; i++ {
		sumRatio += p.ratios[i]
		sumPrepareOverWrite += p.prepareOverWire[i]
	}
	return sumRatio / float64(p.size), sumPrepareOverWrite / float64(p.size)
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
	case CompressionModeZstdLevel1:
		return CompressionModeZstdDefault
	default:
		return CompressionModeZstdDefault
	}
}

func compressionModeFromStored(raw uint8) CompressionMode {
	mode := CompressionMode(raw)
	switch mode {
	case CompressionModeZstdDefault, CompressionModeZstdLevel1, CompressionModeLz4, CompressionModeNone:
		return mode
	default:
		return CompressionModeLz4
	}
}

func frameCompTokenForMode(mode CompressionMode) string {
	switch mode {
	case CompressionModeLz4:
		return EncodingLz4
	case CompressionModeNone:
		return "none"
	default:
		return EncodingZstd
	}
}
