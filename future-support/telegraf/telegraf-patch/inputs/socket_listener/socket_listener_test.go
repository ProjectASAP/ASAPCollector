package socket_listener

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/common/socket"
	"github.com/influxdata/telegraf/plugins/inputs"
	_ "github.com/influxdata/telegraf/plugins/parsers/all"
	influxparser "github.com/influxdata/telegraf/plugins/parsers/influx"
	promparser "github.com/influxdata/telegraf/plugins/parsers/prometheus"
	"github.com/influxdata/telegraf/plugins/parsers/value"
	"github.com/influxdata/telegraf/testutil"
)

var pki = testutil.NewPKI("../../../testutil/pki")

type initableParser interface {
	Init() error
}

func mustInitParser(t *testing.T, parser telegraf.Parser) {
	t.Helper()
	if p, ok := parser.(initableParser); ok {
		require.NoError(t, p.Init())
	}
}

type prometheusSocketParser struct {
	*promparser.Parser
}

func newPrometheusSocketParser() *prometheusSocketParser {
	return &prometheusSocketParser{
		Parser: &promparser.Parser{
			MetricVersion: 2,
			Log:           &testutil.Logger{},
		},
	}
}

func ensureTrailingNewline(buf []byte) []byte {
	if len(buf) == 0 || buf[len(buf)-1] == '\n' {
		return buf
	}
	withNL := make([]byte, len(buf)+1)
	copy(withNL, buf)
	withNL[len(buf)] = '\n'
	return withNL
}

func (p *prometheusSocketParser) Parse(buf []byte) ([]telegraf.Metric, error) {
	return p.Parser.Parse(ensureTrailingNewline(buf))
}

func (p *prometheusSocketParser) ParseLine(line string) (telegraf.Metric, error) {
	metrics, err := p.Parse([]byte(line))
	if err != nil {
		return nil, err
	}

	if len(metrics) < 1 {
		return nil, errors.New("no metrics in line")
	}

	if len(metrics) > 1 {
		return nil, errors.New("more than one metric in line")
	}

	return metrics[0], nil
}

func TestSocketListener(t *testing.T) {
	messages := [][]byte{
		[]byte(`# TYPE test gauge
test{foo="bar"} 1 123456789
test{foo="baz"} 2 123456790
`),
		[]byte(`test{foo="zab"} 3 123456791
`),
	}
	expected := []telegraf.Metric{
		metric.New(
			"prometheus",
			map[string]string{"foo": "bar"},
			map[string]interface{}{"test": float64(1)},
			time.UnixMilli(123456789),
		),
		metric.New(
			"prometheus",
			map[string]string{"foo": "baz"},
			map[string]interface{}{"test": float64(2)},
			time.UnixMilli(123456790),
		),
		metric.New(
			"prometheus",
			map[string]string{"foo": "zab"},
			map[string]interface{}{"test": float64(3)},
			time.UnixMilli(123456791),
		),
	}

	tests := []struct {
		name       string
		schema     string
		buffersize config.Size
		encoding   string
	}{
		{
			name:       "TCP",
			schema:     "tcp",
			buffersize: config.Size(1024),
		},
		{
			name:   "TCP with TLS",
			schema: "tcp+tls",
		},
		{
			name:       "TCP with gzip encoding",
			schema:     "tcp",
			buffersize: config.Size(1024),
			encoding:   "gzip",
		},
		{
			name:       "UDP",
			schema:     "udp",
			buffersize: config.Size(1024),
		},
		{
			name:       "UDP with gzip encoding",
			schema:     "udp",
			buffersize: config.Size(1024),
			encoding:   "gzip",
		},
		{
			name:       "unix socket",
			schema:     "unix",
			buffersize: config.Size(1024),
		},
		{
			name:   "unix socket with TLS",
			schema: "unix+tls",
		},
		{
			name:     "unix socket with gzip encoding",
			schema:   "unix",
			encoding: "gzip",
		},
		{
			name:       "unixgram socket",
			schema:     "unixgram",
			buffersize: config.Size(1024),
		},
	}

	serverTLS := pki.TLSServerConfig()
	clientTLS := pki.TLSClientConfig()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proto := strings.TrimSuffix(tt.schema, "+tls")

			// Prepare the address and socket if needed
			var serverAddr string
			var tlsCfg *tls.Config
			switch proto {
			case "tcp", "udp":
				serverAddr = "127.0.0.1:0"
			case "unix", "unixgram":
				if runtime.GOOS == "windows" {
					t.Skip("Skipping on Windows, as unixgram sockets are not supported")
				}

				// Create a socket
				// The Maximum length of the socket path is 104/108 characters, path created with t.TempDir() is too long for some cases
				// (it combines test name with subtest name and some random numbers in the path).
				// Therefore, in this case, it is safer to stick with `os.MkdirTemp()`.
				//nolint:usetesting // Ignore "os.CreateTemp("", ...) could be replaced by os.CreateTemp(t.TempDir(), ...) in TestSocketListener" finding.
				sock, err := os.CreateTemp("", "sock-")
				require.NoError(t, err)
				defer os.Remove(sock.Name())
				defer sock.Close()
				serverAddr = sock.Name()
			}

			// Setup plugin according to test specification
			plugin := &SocketListener{
				ServiceAddress: proto + "://" + serverAddr,
				Config: socket.Config{
					ContentEncoding: tt.encoding,
					ReadBufferSize:  tt.buffersize,
				},
				Log: &testutil.Logger{},
			}
			if strings.HasSuffix(tt.schema, "tls") {
				plugin.ServerConfig = *serverTLS
				var err error
				tlsCfg, err = clientTLS.TLSConfig()
				require.NoError(t, err)
			}
			parser := newPrometheusSocketParser()
			mustInitParser(t, parser)
			plugin.SetParser(parser)

			// Start the plugin
			var acc testutil.Accumulator
			require.NoError(t, plugin.Init())
			require.NoError(t, plugin.Start(&acc))
			defer plugin.Stop()

			addr := plugin.socket.Address()

			// Create a noop client
			// Server is async, so verify no errors at the end.
			client, err := createClient(plugin.ServiceAddress, addr, tlsCfg)
			require.NoError(t, err)
			require.NoError(t, client.Close())

			// Setup the client for submitting data
			client, err = createClient(plugin.ServiceAddress, addr, tlsCfg)
			require.NoError(t, err)

			// Send the data with the correct encoding
			encoder, err := internal.NewContentEncoder(tt.encoding)
			require.NoError(t, err)

			for i, msg := range messages {
				m, err := encoder.Encode(msg)
				require.NoErrorf(t, err, "encoding failed for msg %d", i)
				_, err = client.Write(m)
				require.NoErrorf(t, err, "sending msg %d failed", i)
			}

			// Test the resulting metrics and compare against expected results
			require.Eventuallyf(t, func() bool {
				acc.Lock()
				defer acc.Unlock()
				return acc.NMetrics() >= uint64(len(expected))
			}, time.Second, 100*time.Millisecond, "did not receive metrics (%d)", acc.NMetrics())
			actual := acc.GetTelegrafMetrics()
			testutil.RequireMetricsEqual(t, expected, actual, testutil.SortMetrics())
		})
	}
}

// go test -v -test.fullpath=true -timeout 0 -run ^TestSocketListenerInsertionThroughput$ github.com/influxdata/telegraf/plugins/inputs/socket_listener -cpuprofile /tmp/sl_cpuprof.pb.gz -memprofile /tmp/sl_memprof.pb.gz
// go tool pprof /tmp/sl_cpuprof.pb.gz
// go tool pprof /tmp/sl_memprof.pb.gz
func TestSocketListenerInsertionThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput test in short mode")
	}

	const (
		totalMessages = 500000
		minThroughput = 1000.0 // metrics/sec
		baseTimestamp = int64(123456789)
		batchSize     = 5000
	)

	plugin := &SocketListener{
		ServiceAddress: "tcp://127.0.0.1:0",
		Config: socket.Config{
			ReadBufferSize: config.Size(256 * 1024),
		},
		SplitConfig: socket.SplitConfig{
			SplittingStrategy: "newline",
		},
		Log: &testutil.Logger{},
	}
	parser := newPrometheusSocketParser()
	mustInitParser(t, parser)
	plugin.SetParser(parser)

	var acc testutil.Accumulator
	require.NoError(t, plugin.Init())
	require.NoError(t, plugin.Start(&acc))
	defer plugin.Stop()

	addr := plugin.socket.Address()

	client, err := createClient(plugin.ServiceAddress, addr, nil)
	require.NoError(t, err)
	defer client.Close()

	lineTemplate := []byte(`throughput_test{source="unit"} 1 `)
	lineLen := len(`throughput_test{source="unit"} 1 123456789` + "\n")
	batch := make([]byte, 0, lineLen*batchSize)

	start := time.Now()
	linesInBatch := 0

	writeBatch := func(buf []byte) {
		if len(buf) == 0 {
			return
		}
		sent := 0
		for sent < len(buf) {
			n, err := client.Write(buf[sent:])
			require.NoError(t, err)
			sent += n
		}
	}

	for i := 0; i < totalMessages; i++ {
		batch = append(batch, lineTemplate...)
		batch = strconv.AppendInt(batch, baseTimestamp+int64(i), 10)
		batch = append(batch, '\n')
		linesInBatch++

		if linesInBatch == batchSize {
			writeBatch(batch)
			batch = batch[:0]
			linesInBatch = 0
		}
	}
	writeBatch(batch)

	getMetricCount := func() uint64 {
		acc.Lock()
		defer acc.Unlock()
		return acc.NMetrics()
	}

	require.Eventuallyf(t, func() bool {
		return getMetricCount() >= uint64(totalMessages)
	}, 10*time.Second, 50*time.Millisecond, "did not receive %d metrics (only %d)", totalMessages, getMetricCount())

	elapsed := time.Since(start)
	throughput := float64(totalMessages) / elapsed.Seconds()
	t.Logf("inserted %d metrics in %s (~%.0f metrics/s)", totalMessages, elapsed, throughput)
	require.GreaterOrEqualf(t, throughput, minThroughput, "insertion throughput too low")

	actual := acc.GetTelegrafMetrics()
	require.Len(t, actual, totalMessages)
	for _, m := range actual {
		require.Equal(t, "prometheus", m.Name())
		require.Equal(t, "unit", m.Tags()["source"])
		fields := m.Fields()
		require.Contains(t, fields, "throughput_test")
		require.Equal(t, float64(1), fields["throughput_test"])
	}
}

func TestLargeReadBufferTCP(t *testing.T) {
	// Construct a buffer-size setting of 1000KiB
	var bufsize config.Size
	require.NoError(t, bufsize.UnmarshalText([]byte("1000KiB")))

	// Setup plugin with a sufficient read buffer
	plugin := &SocketListener{
		ServiceAddress: "tcp://127.0.0.1:0",
		Config: socket.Config{
			ReadBufferSize: bufsize,
		},
		SplitConfig: socket.SplitConfig{
			SplittingStrategy: "newline",
		},
		Log: &testutil.Logger{},
	}
	parser := &value.Parser{
		MetricName: "test",
		DataType:   "string",
	}
	mustInitParser(t, parser)
	plugin.SetParser(parser)

	// Create a large message with the readbuffer size
	message := bytes.Repeat([]byte{'a'}, int(bufsize)-2)
	expected := []telegraf.Metric{
		metric.New(
			"test",
			map[string]string{},
			map[string]interface{}{"value": string(message)},
			time.Unix(0, 0),
		),
	}

	// Start the plugin
	var acc testutil.Accumulator
	require.NoError(t, plugin.Init())
	require.NoError(t, plugin.Start(&acc))
	defer plugin.Stop()

	addr := plugin.socket.Address()

	// Setup the client for submitting data
	client, err := createClient(plugin.ServiceAddress, addr, nil)
	require.NoError(t, err)
	defer client.Close()

	_, err = client.Write(append(message, '\n'))
	require.NoError(t, err)
	client.Close()

	getError := func() error {
		acc.Lock()
		defer acc.Unlock()
		return acc.FirstError()
	}

	// Test the resulting metrics and compare against expected results
	require.Eventuallyf(t, func() bool {
		return acc.NMetrics() >= uint64(len(expected))
	}, time.Second, 100*time.Millisecond, "did not receive metrics (%d): %v", acc.NMetrics(), getError())
	actual := acc.GetTelegrafMetrics()
	testutil.RequireMetricsEqual(t, expected, actual, testutil.IgnoreTime())
}

func TestLargeReadBufferUnixgram(t *testing.T) {
	// Construct a buffer-size setting of 100KiB
	// Assuming that the testing environment has net.core.wmem_max set to a value greater than 100KiB
	if runtime.GOOS == "windows" {
		t.Skip("Skipping on Windows, as unixgram sockets are not supported")
	}

	if runtime.GOOS == "darwin" {
		t.Skip("Skipping on macOS (darwin), as unixgram write buffer size cannot be changed (default 2048 bytes)")
	}

	var bufsize config.Size
	require.NoError(t, bufsize.UnmarshalText([]byte("100KiB")))

	// Create a socket
	sock, err := os.CreateTemp(t.TempDir(), "sock-")
	require.NoError(t, err)
	defer sock.Close()

	var serverAddr = sock.Name()

	// Setup plugin with a sufficient read buffer
	plugin := &SocketListener{
		ServiceAddress: "unixgram" + "://" + serverAddr,
		Config: socket.Config{
			ReadBufferSize: bufsize,
		},
		Log: &testutil.Logger{},
	}
	parser := &value.Parser{
		MetricName: "test",
		DataType:   "string",
	}
	mustInitParser(t, parser)
	plugin.SetParser(parser)

	// Create a large message with the readbuffer size
	message := bytes.Repeat([]byte{'a'}, int(bufsize))
	expected := []telegraf.Metric{
		metric.New(
			"test",
			map[string]string{},
			map[string]interface{}{"value": string(message)},
			time.Unix(0, 0),
		),
	}

	// Start the plugin
	var acc testutil.Accumulator
	require.NoError(t, plugin.Init())
	require.NoError(t, plugin.Start(&acc))
	defer plugin.Stop()

	addr := plugin.socket.Address()

	// Setup the client for submitting data
	client, err := createClient(plugin.ServiceAddress, addr, nil)
	require.NoError(t, err)
	defer client.Close()

	// Check the socket write buffer size
	unixConn, ok := client.(*net.UnixConn)
	require.True(t, ok, "client is not a *net.UnixConn")
	if err := unixConn.SetWriteBuffer(len(message)); err != nil {
		t.Skipf("Failed to set write buffer size: %v. Skipping test.", err)
	}

	// Write the message
	_, err = client.Write(message)
	require.NoError(t, err)
	client.Close()

	getError := func() error {
		acc.Lock()
		defer acc.Unlock()
		return acc.FirstError()
	}

	// Test the resulting metrics and compare against expected results
	require.Eventuallyf(t, func() bool {
		return acc.NMetrics() >= uint64(len(expected))
	}, time.Second, 100*time.Millisecond, "did not receive metrics (%d): %v", acc.NMetrics(), getError())
	actual := acc.GetTelegrafMetrics()
	testutil.RequireMetricsEqual(t, expected, actual, testutil.IgnoreTime())
}

func TestCases(t *testing.T) {
	// Get all directories in testdata
	folders, err := os.ReadDir("testcases")
	require.NoError(t, err)

	// Register the plugin
	inputs.Add("socket_listener", func() telegraf.Input {
		return &SocketListener{}
	})

	for _, f := range folders {
		// Only handle folders
		if !f.IsDir() {
			continue
		}

		// Compare options
		options := []cmp.Option{
			testutil.IgnoreTime(),
			testutil.SortMetrics(),
		}

		t.Run(f.Name(), func(t *testing.T) {
			testcasePath := filepath.Join("testcases", f.Name())
			configFilename := filepath.Join(testcasePath, "telegraf.conf")
			inputFilename := filepath.Join(testcasePath, "sequence.json")
			expectedFilename := filepath.Join(testcasePath, "expected.out")
			expectedErrorFilename := filepath.Join(testcasePath, "expected.err")

			// Prepare the influx parser for expectations
			parser := &influxparser.Parser{}
			mustInitParser(t, parser)

			// Read the input sequence
			sequence, err := readInputData(inputFilename)
			require.NoError(t, err)
			require.NotEmpty(t, sequence)

			// Read the expected output if any
			var expected []telegraf.Metric
			if _, err := os.Stat(expectedFilename); err == nil {
				var err error
				expected, err = testutil.ParseMetricsFromFile(expectedFilename, parser)
				require.NoError(t, err)
			}

			// Read the expected output if any
			var expectedErrors []string
			if _, err := os.Stat(expectedErrorFilename); err == nil {
				var err error
				expectedErrors, err = testutil.ParseLinesFromFile(expectedErrorFilename)
				require.NoError(t, err)
				require.NotEmpty(t, expectedErrors)
			}

			// Configure the plugin
			cfg := config.NewConfig()
			require.NoError(t, cfg.LoadConfig(configFilename))
			require.Len(t, cfg.Inputs, 1)

			// Setup and start the plugin
			var acc testutil.Accumulator
			plugin := cfg.Inputs[0].Input.(*SocketListener)
			require.NoError(t, plugin.Init())
			require.NoError(t, plugin.Start(&acc))
			defer plugin.Stop()

			// Create a client without TLS
			addr := plugin.socket.Address()
			client, err := createClient(plugin.ServiceAddress, addr, nil)
			require.NoError(t, err)

			// Write the given sequence
			for i, step := range sequence {
				if step.Wait > 0 {
					time.Sleep(time.Duration(step.Wait))
					continue
				}
				require.NotEmpty(t, step.raw, "nothing to send")
				_, err := client.Write(step.raw)
				require.NoErrorf(t, err, "writing step %d failed: %v", i, err)
			}
			require.NoError(t, client.Close())

			getNErrors := func() int {
				acc.Lock()
				defer acc.Unlock()
				return len(acc.Errors)
			}
			require.Eventuallyf(t, func() bool {
				return getNErrors() >= len(expectedErrors)
			}, 3*time.Second, 100*time.Millisecond, "did not receive errors (%d/%d)", getNErrors(), len(expectedErrors))

			require.Len(t, acc.Errors, len(expectedErrors))
			sort.SliceStable(acc.Errors, func(i, j int) bool {
				return acc.Errors[i].Error() < acc.Errors[j].Error()
			})
			for i, err := range acc.Errors {
				require.ErrorContains(t, err, expectedErrors[i])
			}

			require.Eventuallyf(t, func() bool {
				acc.Lock()
				defer acc.Unlock()
				return acc.NMetrics() >= uint64(len(expected))
			}, 3*time.Second, 100*time.Millisecond, "did not receive metrics (%d/%d)", acc.NMetrics(), len(expected))

			// Check the metric nevertheless as we might get some metrics despite errors.
			actual := acc.GetTelegrafMetrics()
			testutil.RequireMetricsEqual(t, expected, actual, options...)
		})
	}
}

// element provides a way to configure the
// write sequence for the socket.
type element struct {
	Message string          `json:"message"`
	File    string          `json:"file"`
	Wait    config.Duration `json:"wait"`
	raw     []byte
}

func readInputData(filename string) ([]element, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var sequence []element
	if err := json.Unmarshal(content, &sequence); err != nil {
		return nil, err
	}

	for i, step := range sequence {
		if step.Message != "" && step.File != "" {
			return nil, errors.New("both message and file set in sequence")
		} else if step.Message != "" {
			step.raw = []byte(step.Message)
		} else if step.File != "" {
			path := filepath.Dir(filename)
			path = filepath.Join(path, step.File)
			step.raw, err = os.ReadFile(path)
			if err != nil {
				return nil, err
			}
		}
		sequence[i] = step
	}

	return sequence, nil
}

func createClient(endpoint string, addr net.Addr, tlsCfg *tls.Config) (net.Conn, error) {
	// Determine the protocol in a crude fashion
	parts := strings.SplitN(endpoint, "://", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid endpoint %q", endpoint)
	}
	protocol := parts[0]

	if tlsCfg == nil {
		return net.Dial(protocol, addr.String())
	}

	if protocol == "unix" {
		tlsCfg.InsecureSkipVerify = true
	}
	return tls.Dial(protocol, addr.String(), tlsCfg)
}
