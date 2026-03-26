// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"log"
	"math"
	"sync"
	"time"

	"github.com/pion/interceptor/internal/cc"
	"github.com/pion/logging"
)

const (
	// constants from
	// https://datatracker.ietf.org/doc/html/draft-ietf-rmcat-gcc-02#section-6

	increaseLossThreshold = 0.02
	increaseTimeThreshold = 200 * time.Millisecond
	increaseFactor        = 1.05

	decreaseLossThreshold = 0.1
	decreaseTimeThreshold = 200 * time.Millisecond
)

// LossStats contains internal statistics of the loss based controller.
type LossStats struct {
	TargetBitrate int
	AverageLoss   float64
}

type lossBasedBandwidthEstimator struct {
	lock           sync.Mutex
	maxBitrate     int
	minBitrate     int
	bitrate        int
	averageLoss    float64
	lastLossUpdate time.Time
	lastIncrease   time.Time
	lastDecrease   time.Time
	log            logging.LeveledLogger
}

func newLossBasedBWE(initialBitrate int, loggerFactory logging.LoggerFactory) *lossBasedBandwidthEstimator {
	return &lossBasedBandwidthEstimator{
		lock:           sync.Mutex{},
		maxBitrate:     100_000_000, // 100 mbit
		minBitrate:     100_000,     // 100 kbit
		bitrate:        initialBitrate,
		averageLoss:    0,
		lastLossUpdate: time.Time{},
		lastIncrease:   time.Time{},
		lastDecrease:   time.Time{},
		log:            loggerFactory.NewLogger("gcc_loss_controller"),
	}
}

func (e *lossBasedBandwidthEstimator) getEstimate(wantedRate int) LossStats {
	e.lock.Lock()
	defer e.lock.Unlock()

	if e.bitrate <= 0 {
		e.bitrate = clampInt(wantedRate, e.minBitrate, e.maxBitrate)
	}
	// Return the minimum of wantedRate and internal estimate, but do NOT
	// mutate e.bitrate. The loss controller must maintain its own independent
	// estimate that is only updated by updateLossEstimate(). The original
	// code permanently ratcheted down e.bitrate whenever the delay controller
	// had a decrease event, preventing the loss estimate from recovering.
	targetBitrate := min(wantedRate, e.bitrate)

	return LossStats{
		TargetBitrate: targetBitrate,
		AverageLoss:   e.averageLoss,
	}
}

func (e *lossBasedBandwidthEstimator) updateLossEstimate(results []cc.Acknowledgment) {
	if len(results) == 0 {
		return
	}

	// Count only genuine losses: packets the receiver explicitly marked as
	// not received. Evicted history entries also have Arrival.IsZero() but
	// can be distinguished because their Departure is also zero (they are
	// zero-value Acknowledgment structs from slots that didn't match any
	// history entry).
	packetsLost := 0
	packetsTotal := 0
	for _, p := range results {
		if p.Departure.IsZero() {
			continue // evicted from history, not a real loss report
		}
		packetsTotal++
		if p.Arrival.IsZero() {
			packetsLost++
		}
	}

	if packetsTotal == 0 {
		return
	}

	e.lock.Lock()
	defer e.lock.Unlock()

	lossRatio := float64(packetsLost) / float64(packetsTotal)
	e.averageLoss = e.average(time.Since(e.lastLossUpdate), e.averageLoss, lossRatio)
	e.lastLossUpdate = time.Now()

	increaseLoss := math.Max(e.averageLoss, lossRatio)
	decreaseLoss := math.Min(e.averageLoss, lossRatio)

	if increaseLoss < increaseLossThreshold && time.Since(e.lastIncrease) > increaseTimeThreshold {
		old := e.bitrate
		e.lastIncrease = time.Now()
		e.bitrate = clampInt(int(increaseFactor*float64(e.bitrate)), e.minBitrate, e.maxBitrate)
		log.Printf("[GCC-LOSS] INCREASE: %.2f -> %.2f Mbps (avgLoss=%.4f, pkts=%d, lost=%d)",
			float64(old)/1e6, float64(e.bitrate)/1e6, e.averageLoss, packetsTotal, packetsLost)
	} else if decreaseLoss > decreaseLossThreshold && time.Since(e.lastDecrease) > decreaseTimeThreshold {
		old := e.bitrate
		e.lastDecrease = time.Now()
		e.bitrate = clampInt(int(float64(e.bitrate)*(1-0.5*decreaseLoss)), e.minBitrate, e.maxBitrate)
		log.Printf("[GCC-LOSS] DECREASE: %.2f -> %.2f Mbps (avgLoss=%.4f, decLoss=%.4f, pkts=%d, lost=%d)",
			float64(old)/1e6, float64(e.bitrate)/1e6, e.averageLoss, decreaseLoss, packetsTotal, packetsLost)
	}
}

func (e *lossBasedBandwidthEstimator) average(delta time.Duration, prev, sample float64) float64 {
	return sample + math.Exp(-float64(delta.Milliseconds())/200.0)*(prev-sample)
}
