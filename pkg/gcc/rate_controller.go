// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"log"
	"math"
	"sync"
	"time"
)

const (
	decreaseEMAAlpha = 0.95
	beta             = 0.85
	logInterval      = 2 * time.Second
)

type rateController struct {
	now                  now
	initialTargetBitrate int
	minBitrate           int
	maxBitrate           int

	dsWriter func(DelayStats)

	lock               sync.Mutex
	init               bool
	delayStats         DelayStats
	target             int
	lastUpdate         time.Time
	lastState          state
	lastLog            time.Time
	latestRTT          time.Duration
	latestReceivedRate int
	latestDecreaseRate *exponentialMovingAverage
}

type exponentialMovingAverage struct {
	average      float64
	variance     float64
	stdDeviation float64
}

func (a *exponentialMovingAverage) update(value float64) {
	if a.average == 0.0 {
		a.average = value
	} else {
		x := value - a.average
		a.average += decreaseEMAAlpha * x
		a.variance = (1 - decreaseEMAAlpha) * (a.variance + decreaseEMAAlpha*x*x)
		a.stdDeviation = math.Sqrt(a.variance)
	}
}

func newRateController(
	now now, initialTargetBitrate, minBitrate, maxBitrate int, dsw func(DelayStats),
) *rateController {
	return &rateController{
		now:                  now,
		initialTargetBitrate: initialTargetBitrate,
		minBitrate:           minBitrate,
		maxBitrate:           maxBitrate,
		dsWriter:             dsw,
		init:                 false,
		delayStats:           DelayStats{},
		target:               initialTargetBitrate,
		lastUpdate:           time.Time{},
		lastState:            stateIncrease,
		latestRTT:            0,
		latestReceivedRate:   0,
		latestDecreaseRate:   &exponentialMovingAverage{},
	}
}

func (c *rateController) onReceivedRate(rate int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.latestReceivedRate = rate
}

func (c *rateController) updateRTT(rtt time.Duration) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.latestRTT = rtt
}

func (c *rateController) onDelayStats(ds DelayStats) {
	now := time.Now()

	if !c.init {
		c.delayStats = ds
		c.delayStats.State = stateIncrease
		c.init = true

		return
	}
	c.delayStats = ds
	c.delayStats.State = c.delayStats.State.transition(ds.Usage)

	if c.delayStats.State == stateHold {
		return
	}

	var next DelayStats

	c.lock.Lock()

	switch c.delayStats.State {
	case stateHold:
		// should never occur due to check above, but makes the linter happy
	case stateIncrease:
		c.target = clampInt(c.increase(now), c.minBitrate, c.maxBitrate)
		if now.Sub(c.lastLog) > logInterval {
			mode := "EXPONENTIAL"
			if c.latestDecreaseRate.average > 0 &&
				float64(c.target) > c.latestDecreaseRate.average-3*c.latestDecreaseRate.stdDeviation &&
				float64(c.target) < c.latestDecreaseRate.average+3*c.latestDecreaseRate.stdDeviation {
				mode = "ADDITIVE"
			}
			log.Printf("[GCC] %s target=%.2f Mbps, recvRate=%.2f Mbps, decAvg=%.2f Mbps",
				mode, float64(c.target)/1e6, float64(c.latestReceivedRate)/1e6, c.latestDecreaseRate.average/1e6)
			c.lastLog = now
		}
		next = DelayStats{
			Measurement:      c.delayStats.Measurement,
			Estimate:         c.delayStats.Estimate,
			Threshold:        c.delayStats.Threshold,
			LastReceiveDelta: c.delayStats.LastReceiveDelta,
			Usage:            c.delayStats.Usage,
			State:            c.delayStats.State,
			TargetBitrate:    c.target,
		}

	case stateDecrease:
		c.target = clampInt(c.decrease(), c.minBitrate, c.maxBitrate)
		next = DelayStats{
			Measurement:      c.delayStats.Measurement,
			Estimate:         c.delayStats.Estimate,
			Threshold:        c.delayStats.Threshold,
			LastReceiveDelta: c.delayStats.LastReceiveDelta,
			Usage:            c.delayStats.Usage,
			State:            c.delayStats.State,
			TargetBitrate:    c.target,
		}
	}

	c.lock.Unlock()

	c.dsWriter(next)
}

func (c *rateController) increase(now time.Time) int {
	if c.latestDecreaseRate.average > 0 &&
		float64(c.target) > c.latestDecreaseRate.average-3*c.latestDecreaseRate.stdDeviation &&
		float64(c.target) < c.latestDecreaseRate.average+3*c.latestDecreaseRate.stdDeviation {
		bitsPerFrame := float64(c.target) / 30.0
		packetsPerFrame := math.Ceil(bitsPerFrame / (1200 * 8))
		expectedPacketSizeBits := bitsPerFrame / packetsPerFrame

		responseTime := 100*time.Millisecond + c.latestRTT
		alpha := 0.5 * math.Min(float64(now.Sub(c.lastUpdate).Milliseconds())/float64(responseTime.Milliseconds()), 1.0)
		increase := int(math.Max(1000.0, alpha*expectedPacketSizeBits))
		c.lastUpdate = now

		return int(math.Min(float64(c.target+increase), 1.5*float64(c.target)))
	}
	eta := math.Pow(1.08, math.Min(float64(now.Sub(c.lastUpdate).Milliseconds())/1000, 1.0))
	c.lastUpdate = now

	rate := int(eta * float64(c.target))

	// maximum increase to 1.5 * rate
	maxRate := int(1.5 * float64(c.target))
	if rate > maxRate {
		return maxRate
	}

	if rate < c.target {
		return c.target
	}

	return rate
}

func (c *rateController) decrease() int {
	// Use the higher of target and latestReceivedRate as the reference for
	// the decrease. When the encoder underproduces (static scenes, resolution
	// tier caps, BANDWIDTH_FRACTION < 1.0), latestReceivedRate can be far
	// below target, causing catastrophic drops (e.g. 50 → 7 Mbps).
	ref := c.target
	if c.latestReceivedRate > ref {
		ref = c.latestReceivedRate
	}
	target := int(beta * float64(ref))
	log.Printf("[GCC] DECREASE: ref=%.2f Mbps (target=%.2f, recvRate=%.2f) -> new=%.2f Mbps (beta=%.2f), decAvg=%.2f Mbps",
		float64(ref)/1e6, float64(c.target)/1e6, float64(c.latestReceivedRate)/1e6,
		float64(target)/1e6, beta, c.latestDecreaseRate.average/1e6)
	c.latestDecreaseRate.update(float64(ref))
	c.lastUpdate = c.now()

	return target
}
