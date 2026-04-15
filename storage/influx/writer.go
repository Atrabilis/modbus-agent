package influx

import (
	"context"
	"fmt"
	"os"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	influxapi "github.com/influxdata/influxdb-client-go/v2/api"
)

const backendType = "influxdb2"

type Writer struct {
	name      string
	available bool

	measurement string

	client influxdb2.Client

	writeAPI influxapi.WriteAPI
}

func NewWriter(name string, cfg Config) (*Writer, error) {
	if err := cfg.Validate(name); err != nil {
		return nil, err
	}

	host := os.Getenv(cfg.HostEnv)
	token := os.Getenv(cfg.TokenEnv)
	org := os.Getenv(cfg.OrgEnv)
	if host == "" {
		return nil, fmt.Errorf("env %s is empty", cfg.HostEnv)
	}
	if token == "" {
		return nil, fmt.Errorf("env %s is empty", cfg.TokenEnv)
	}
	if org == "" {
		return nil, fmt.Errorf("env %s is empty", cfg.OrgEnv)
	}

	w := &Writer{
		name:        name,
		measurement: cfg.Measurement,
	}

	w.client = influxdb2.NewClient(host, token)
	ping, err := w.client.Ping(context.Background())
	if err != nil || !ping {
		fmt.Printf("WARNING: storage output %q (type=%s) is not reachable\n", name, backendType)
		w.available = false
	} else {
		w.available = true
		fmt.Printf("Storage output %q (type=%s) is reachable\n", name, backendType)
	}
	w.writeAPI = w.client.WriteAPI(org, cfg.Bucket)
	errCh := w.writeAPI.Errors()
	go func() {
		for err := range errCh {
			fmt.Printf("Error writing to storage output %q (type=%s): %v\n", name, backendType, err)
		}
	}()

	return w, nil
}

func (w *Writer) Name() string {
	if w == nil {
		return ""
	}
	return w.name
}

func (w *Writer) Available() bool {
	if w == nil {
		return false
	}
	return w.available
}

func (w *Writer) Write(tags map[string]string, fields map[string]interface{}, ts time.Time) {
	if w == nil {
		return
	}
	if w.available {
		w.writeAPI.WritePoint(influxdb2.NewPoint(w.measurement, tags, fields, ts))
	}
}

func (w *Writer) Flush() {
	if w == nil {
		return
	}
	if w.available {
		fmt.Printf("Flushing storage output %q (type=%s)\n", w.name, backendType)
		w.writeAPI.Flush()
	}
}

func (w *Writer) Close() {
	if w == nil {
		return
	}
	if w.client != nil {
		w.client.Close()
	}
}
