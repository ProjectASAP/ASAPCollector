// gorilla-gateway — disk-buffering S3 proxy between OTel agents and MinIO.
//
// Agents write Gorilla TSDB blocks via S3 PUT (one block every AGENT_WINDOW_INTERVAL,
// typically 1 minute). The gateway persists each received block to a local buffer
// directory and uploads complete blocks to MinIO on GATEWAY_FLUSH_INTERVAL (default 1h).
//
// A co-located thanos-buffer-store sidecar (quay.io/thanos/thanos:v0.41.0 with a
// FILESYSTEM objstore) reads the same buffer directory and exposes the in-flight
// blocks via the Thanos Store gRPC API, giving Thanos Query ~30 s data freshness
// regardless of the 1 h flush interval.
//
// Env vars:
//   GATEWAY_LISTEN              HTTP listen address          (default :9100)
//   GATEWAY_UPSTREAM_ENDPOINT   MinIO host:port              (default minio:9000)
//   GATEWAY_ACCESS_KEY          S3 access key                (default asap)
//   GATEWAY_SECRET_KEY          S3 secret key                (default asap-local-only)
//   GATEWAY_BUFFER_DIR          local disk buffer path       (default /var/gorilla-gateway/buffer)
//   GATEWAY_FLUSH_INTERVAL      upload cadence to MinIO      (default 1h)
//   GATEWAY_MINIO_SYNC_GRACE    wait after upload before     (default 90s)
//                               deleting local copy; must be
//                               > thanos store sync-block-duration
package main

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		log.Printf("warning: %s=%q invalid duration, using %s", key, v, def)
	}
	return def
}

func main() {
	listenAddr    := envOr("GATEWAY_LISTEN",            ":9100")
	upstreamEndpt := envOr("GATEWAY_UPSTREAM_ENDPOINT", "minio:9000")
	accessKey     := envOr("GATEWAY_ACCESS_KEY",        "asap")
	secretKey     := envOr("GATEWAY_SECRET_KEY",        "asap-local-only")
	bufferDir     := envOr("GATEWAY_BUFFER_DIR",        "/var/gorilla-gateway/buffer")
	flushInterval := envDuration("GATEWAY_FLUSH_INTERVAL",    time.Hour)
	syncGrace     := envDuration("GATEWAY_MINIO_SYNC_GRACE",  90*time.Second)

	if err := os.MkdirAll(bufferDir, 0755); err != nil {
		log.Fatalf("buffer dir: %v", err)
	}

	mc, err := minio.New(upstreamEndpt, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		log.Fatalf("minio client: %v", err)
	}

	db := newDiskBuffer(bufferDir)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	go runFlushLoop(ctx, db, mc, flushInterval, syncGrace)

	mux := http.NewServeMux()
	mux.HandleFunc("/", makeS3Handler(db))
	mux.HandleFunc("/v1/blocks", db.handleListBlocks)

	srv := &http.Server{Addr: listenAddr, Handler: mux}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()

	log.Printf("gorilla-gateway: listen=%s upstream=%s buffer=%s flush=%s grace=%s",
		listenAddr, upstreamEndpt, bufferDir, flushInterval, syncGrace)
	_ = srv.ListenAndServe()
}

func makeS3Handler(db *DiskBuffer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bucket, key := parsePath(r.URL.Path)

		switch r.Method {
		case http.MethodPut:
			if key == "" {
				// CreateBucket — silently accept; buckets are pre-created on MinIO.
				w.WriteHeader(http.StatusOK)
				return
			}
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "read body: "+err.Error(), http.StatusInternalServerError)
				return
			}
			_ = r.Body.Close()

			if err := db.Write(bucket, key, data); err != nil {
				http.Error(w, "write: "+err.Error(), http.StatusInternalServerError)
				return
			}

			etag := fmt.Sprintf("%q", md5.Sum(data))
			w.Header().Set("ETag", etag)
			w.Header().Set("Content-Length", "0")
			w.WriteHeader(http.StatusOK)

		case http.MethodHead:
			if key == "" {
				w.WriteHeader(http.StatusOK) // HeadBucket: always exists
			} else {
				http.NotFound(w, r) // HeadObject: not found → agent will PUT
			}

		case http.MethodGet:
			// Minimal S3 XML so S3 clients don't error on startup probes.
			w.Header().Set("Content-Type", "application/xml")
			if bucket == "" {
				fmt.Fprintf(w,
					`<?xml version="1.0" encoding="UTF-8"?>`+
						`<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`+
						`<Owner><ID>gorilla-gateway</ID><DisplayName>gorilla-gateway</DisplayName></Owner>`+
						`<Buckets></Buckets></ListAllMyBucketsResult>`)
			} else {
				fmt.Fprintf(w,
					`<?xml version="1.0" encoding="UTF-8"?>`+
						`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`+
						`<Name>%s</Name><IsTruncated>false</IsTruncated>`+
						`</ListBucketResult>`, bucket)
			}

		default:
			log.Printf("unhandled %s %s", r.Method, r.URL.Path)
			http.Error(w, "not implemented", http.StatusNotImplemented)
		}
	}
}

// parsePath splits /bucket/key... into (bucket, key). key may be empty.
func parsePath(p string) (bucket, key string) {
	p = strings.TrimPrefix(p, "/")
	if i := strings.Index(p, "/"); i >= 0 {
		return p[:i], p[i+1:]
	}
	return p, ""
}
