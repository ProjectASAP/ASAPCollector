package precompute

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrFrameChecksum = errors.New("precompute: frame checksum mismatch")
	ErrFrameGap      = errors.New("precompute: frame sequence gap")
	ErrFrameBase     = errors.New("precompute: unknown delta base checkpoint")
	ErrFrameConflict = errors.New("precompute: conflicting frame replay")
)

const frameChecksumAttribute = "asap.frame.payload_checksum"

// FrameChecksum implements the ASAP-FRAME-V1 checksum. The canonical header
// is the lexicographically sorted sequence of length-prefixed attribute keys
// and values, excluding the checksum itself.
func FrameChecksum(attrs map[string]string, payload []byte) string {
	keys := make([]string, 0, len(attrs))
	for key := range attrs {
		if key != frameChecksumAttribute {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	header := make([]byte, 0, len(keys)*32)
	var size [4]byte
	for _, key := range keys {
		value := attrs[key]
		binary.BigEndian.PutUint32(size[:], uint32(len(key)))
		header = append(header, size[:]...)
		header = append(header, key...)
		binary.BigEndian.PutUint32(size[:], uint32(len(value)))
		header = append(header, size[:]...)
		header = append(header, value...)
	}
	h := sha256.New()
	h.Write([]byte("ASAP-FRAME-V1"))
	binary.BigEndian.PutUint32(size[:], uint32(len(header)))
	h.Write(size[:])
	h.Write(header)
	h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

// SealFrameAttributes returns a copy with a checksum attached.
func SealFrameAttributes(attrs map[string]string, payload []byte) map[string]string {
	sealed := make(map[string]string, len(attrs)+1)
	for key, value := range attrs {
		sealed[key] = value
	}
	sealed[frameChecksumAttribute] = FrameChecksum(sealed, payload)
	return sealed
}

type receivedFrameKey struct {
	planID, planVersion, materialization uint64
	series, producer, epoch              string
}

type receivedFrameState struct {
	sequence    uint64
	checkpoint  string
	fingerprint [32]byte
}

type frameReceiver struct {
	mu       sync.Mutex
	lineages map[receivedFrameKey]receivedFrameState
}

type frameReceipt struct {
	key               receivedFrameKey
	state             receivedFrameState
	framed, duplicate bool
}

func parseRequiredUint(attrs map[string]string, name string) (uint64, error) {
	raw := attrs[name]
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || value == 0 {
		return 0, fmt.Errorf("precompute: invalid %s", name)
	}
	return value, nil
}

func (r *frameReceiver) prepare(env *SketchEnvelope) (frameReceipt, error) {
	a := env.FrameAttributes
	if len(a) == 0 {
		return frameReceipt{}, nil
	} // legacy, unframed input
	planID, err := parseRequiredUint(a, "asap.frame.plan_id")
	if err != nil {
		return frameReceipt{}, err
	}
	version, err := parseRequiredUint(a, "asap.frame.plan_version")
	if err != nil {
		return frameReceipt{}, err
	}
	materialization, err := parseRequiredUint(a, "asap.frame.materialization")
	if err != nil {
		return frameReceipt{}, err
	}
	sequence, err := parseRequiredUint(a, "asap.frame.sequence")
	if err != nil {
		return frameReceipt{}, err
	}
	key := receivedFrameKey{planID, version, materialization, a["asap.frame.series_identity"], a["asap.frame.producer_id"], a["asap.frame.producer_epoch"]}
	if key.series == "" || key.producer == "" || key.epoch == "" {
		return frameReceipt{}, errors.New("precompute: incomplete frame lineage")
	}
	want := strings.ToLower(a[frameChecksumAttribute])
	if want == "" || want != FrameChecksum(a, env.Payload) {
		return frameReceipt{}, ErrFrameChecksum
	}
	fingerprint := sha256.Sum256(append([]byte(want+"\x00"), env.Payload...))

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lineages == nil {
		r.lineages = make(map[receivedFrameKey]receivedFrameState)
	}
	previous := r.lineages[key]
	if sequence == previous.sequence && sequence != 0 {
		if previous.fingerprint != fingerprint {
			return frameReceipt{}, ErrFrameConflict
		}
		return frameReceipt{framed: true, duplicate: true}, nil
	}
	if sequence != previous.sequence+1 {
		return frameReceipt{}, ErrFrameGap
	}
	kind := a["asap.frame.kind"]
	next := receivedFrameState{sequence: sequence, checkpoint: previous.checkpoint, fingerprint: fingerprint}
	switch kind {
	case "full":
		next.checkpoint = a["asap.frame.checkpoint_id"]
		if next.checkpoint == "" {
			return frameReceipt{}, ErrFrameBase
		}
	case "delta":
		if previous.checkpoint == "" || a["asap.frame.base_checkpoint_id"] != previous.checkpoint {
			return frameReceipt{}, ErrFrameBase
		}
	default:
		return frameReceipt{}, errors.New("precompute: invalid frame kind")
	}
	return frameReceipt{key: key, state: next, framed: true}, nil
}

func (r *frameReceiver) commit(receipt frameReceipt) {
	if !receipt.framed || receipt.duplicate {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lineages == nil {
		r.lineages = make(map[receivedFrameKey]receivedFrameState)
	}
	r.lineages[receipt.key] = receipt.state
}
