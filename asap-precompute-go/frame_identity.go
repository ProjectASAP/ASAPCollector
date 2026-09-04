package precompute

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
)

// SummaryFrameIdentity is the plan-derived identity attached to one summary data point.
type SummaryFrameIdentity struct {
	IdentityVersion     uint32
	PlanID              uint64
	PlanVersion         uint64
	BackendCompat       string
	Materialization     uint64
	SeriesIdentity      string
	SchemaID            string
	ProducerID          string
	ProducerEpoch       string
	WindowStartUnixNano uint64
	WindowEndUnixNano   uint64
	Sequence            uint64
	Kind                string
	Encoding            StateEncoding
	CheckpointID        string
	BaseCheckpointID    string
}

type frameLineageKey struct {
	planID          uint64
	planVersion     uint64
	materialization uint64
	seriesIdentity  string
	producerID      string
	producerEpoch   string
}

type frameLineageState struct {
	sequence       uint64
	checkpointID   string
	checkpointAtMS uint64
}

// FrameSequencer owns sender ordering for all concrete series in one process.
type FrameSequencer struct {
	mu       sync.Mutex
	lineages map[frameLineageKey]frameLineageState
}

// FrameEmission describes one already-serialized envelope awaiting identity.
type FrameEmission struct {
	SeriesIdentity      string
	WindowStartUnixNano uint64
	WindowEndUnixNano   uint64
	NowUnixMS           uint64
	EmittedFull         bool
}

// NextBatchForEmission allocates a batch transactionally. If any envelope is
// invalid, no lineage sequence or checkpoint state is advanced.
func (s *FrameSequencer) NextBatchForEmission(plan CollectorPlan, rule TransmissionRule, producerEpoch string, emissions []FrameEmission) ([]SummaryFrameIdentity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cloned := make(map[frameLineageKey]frameLineageState, len(s.lineages))
	for key, state := range s.lineages {
		cloned[key] = state
	}
	candidate := FrameSequencer{lineages: cloned}
	frames := make([]SummaryFrameIdentity, 0, len(emissions))
	for _, emission := range emissions {
		frame, err := candidate.NextForEmission(
			plan, rule, producerEpoch, emission.SeriesIdentity,
			emission.WindowStartUnixNano, emission.WindowEndUnixNano,
			emission.NowUnixMS, emission.EmittedFull,
		)
		if err != nil {
			return nil, err
		}
		frames = append(frames, frame)
	}
	s.lineages = candidate.lineages
	return frames, nil
}

// Next returns a full checkpoint for the first frame in every lineage and
// periodic recovery checkpoints for delta rules.
func (s *FrameSequencer) Next(plan CollectorPlan, rule TransmissionRule, producerEpoch, seriesIdentity string, windowStart, windowEnd, nowUnixMS uint64) (SummaryFrameIdentity, error) {
	return s.next(plan, rule, producerEpoch, seriesIdentity, windowStart, windowEnd, nowUnixMS, nil)
}

// NextForEmission binds identity to the bytes the runtime actually emitted.
// A required full/checkpoint frame can never be mislabeled as a delta.
func (s *FrameSequencer) NextForEmission(plan CollectorPlan, rule TransmissionRule, producerEpoch, seriesIdentity string, windowStart, windowEnd, nowUnixMS uint64, emittedFull bool) (SummaryFrameIdentity, error) {
	return s.next(plan, rule, producerEpoch, seriesIdentity, windowStart, windowEnd, nowUnixMS, &emittedFull)
}

func (s *FrameSequencer) next(plan CollectorPlan, rule TransmissionRule, producerEpoch, seriesIdentity string, windowStart, windowEnd, nowUnixMS uint64, emittedFull *bool) (SummaryFrameIdentity, error) {
	if producerEpoch == "" || seriesIdentity == "" || windowStart >= windowEnd || rule.ProducerID != plan.CollectorID {
		return SummaryFrameIdentity{}, errors.New("invalid frame lineage")
	}
	matched := false
	for _, candidate := range plan.TransmissionRules {
		if reflect.DeepEqual(candidate, rule) {
			matched = true
			break
		}
	}
	if !matched {
		return SummaryFrameIdentity{}, errors.New("transmission rule does not belong to plan")
	}
	key := frameLineageKey{
		planID: plan.Envelope.PlanID, planVersion: plan.Envelope.PlanVersion,
		materialization: rule.Materialization, seriesIdentity: seriesIdentity,
		producerID: rule.ProducerID, producerEpoch: producerEpoch,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lineages == nil {
		s.lineages = make(map[frameLineageKey]frameLineageState)
	}
	state := s.lineages[key]
	state.sequence++
	checkpointDue := rule.FullCheckpointEveryMS != nil && nowUnixMS >= state.checkpointAtMS &&
		nowUnixMS-state.checkpointAtMS >= *rule.FullCheckpointEveryMS
	full := state.sequence == 1 || rule.Mode == TransmissionModeFull || checkpointDue
	if emittedFull != nil {
		if full && !*emittedFull {
			return SummaryFrameIdentity{}, errors.New("runtime emitted delta when a full checkpoint was required")
		}
		full = *emittedFull
	}
	kind := "delta"
	if full {
		kind = "full"
		state.checkpointID = fmt.Sprintf("%s:%s:%d:%d", rule.ProducerID, producerEpoch, windowStart, state.sequence)
		state.checkpointAtMS = nowUnixMS
	}
	s.lineages[key] = state
	frame := SummaryFrameIdentity{
		IdentityVersion: 1, PlanID: plan.Envelope.PlanID, PlanVersion: plan.Envelope.PlanVersion,
		BackendCompat: plan.Envelope.BackendCompat, Materialization: rule.Materialization,
		SeriesIdentity: seriesIdentity, SchemaID: rule.SchemaID, ProducerID: rule.ProducerID,
		ProducerEpoch: producerEpoch, WindowStartUnixNano: windowStart, WindowEndUnixNano: windowEnd,
		Sequence: state.sequence, Kind: kind, Encoding: rule.Encoding,
	}
	if full {
		frame.CheckpointID = state.checkpointID
	} else {
		frame.BaseCheckpointID = state.checkpointID
	}
	return frame, nil
}

// OTLPAttributes returns the exact reserved attribute names consumed by ASAPQuery-backend.
func (f SummaryFrameIdentity) OTLPAttributes() map[string]string {
	attrs := map[string]string{
		"asap.frame.identity_version": strconv.FormatUint(uint64(f.IdentityVersion), 10),
		"asap.frame.plan_id":          strconv.FormatUint(f.PlanID, 10),
		"asap.frame.plan_version":     strconv.FormatUint(f.PlanVersion, 10),
		"asap.frame.backend_compat":   f.BackendCompat,
		"asap.frame.materialization":  strconv.FormatUint(f.Materialization, 10),
		"asap.frame.series_identity":  f.SeriesIdentity,
		"asap.frame.schema_id":        f.SchemaID,
		"asap.frame.producer_id":      f.ProducerID,
		"asap.frame.producer_epoch":   f.ProducerEpoch,
		"asap.frame.sequence":         strconv.FormatUint(f.Sequence, 10),
		"asap.frame.kind":             f.Kind,
		"asap.frame.encoding":         string(f.Encoding),
	}
	if f.CheckpointID != "" {
		attrs["asap.frame.checkpoint_id"] = f.CheckpointID
	}
	if f.BaseCheckpointID != "" {
		attrs["asap.frame.base_checkpoint_id"] = f.BaseCheckpointID
	}
	return attrs
}
