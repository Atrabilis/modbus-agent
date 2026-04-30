package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/atrabilis/modbus-agent/internal"
	"github.com/atrabilis/modbus-agent/storage"
	"github.com/atrabilis/modbus-agent/storage/influx"
	"github.com/atrabilis/modbus-agent/storage/timescale"
	"github.com/atrabilis/modbus-agent/storage/timescale_shadow"

	dotenv "github.com/joho/godotenv"
)

var (
	configPath      = flag.String("configPath", "", "Path to the config file")
	envPath         = flag.String("envPath", "/etc/atamostec/modbus-agent/modbus-agent.env", "Path to the dotenv file")
	interrogateOnly = flag.Bool("interrogateOnly", false, "Read devices without writing to configured storage outputs")
)

type storageWriter interface {
	Name() string
	Available() bool
	Write(tags map[string]string, fields map[string]interface{}, ts time.Time)
	Flush()
	Close()
}

type sample struct {
	Tags      map[string]string
	Fields    map[string]interface{}
	Timestamp time.Time
}

const timescaleWriteWorkers = 8

func aggregateSamplesForTimescale(samples []sample) []sample {
	rowsByKey := make(map[string]*sample, len(samples))
	order := make([]string, 0, len(samples))

	for i, s := range samples {
		deviceName := firstNonEmptyTagValue(s.Tags, "device_name", "device")
		slaveName := firstNonEmptyTagValue(s.Tags, "slave_name", "slave")

		// Keep malformed samples isolated so writer warnings remain visible.
		key := fmt.Sprintf("invalid:%d", i)
		if deviceName != "" && slaveName != "" {
			seriesKey, _ := internal.BuildSeriesMetadata(s.Tags)
			key = fmt.Sprintf("%s|%s|%s|%s", s.Timestamp.UTC().Format(time.RFC3339Nano), deviceName, slaveName, seriesKey)
		}

		existing, ok := rowsByKey[key]
		if !ok {
			rowsByKey[key] = &sample{
				Tags:      copyStringMap(s.Tags),
				Fields:    copyInterfaceMap(s.Fields),
				Timestamp: s.Timestamp,
			}
			order = append(order, key)
			continue
		}

		for k, v := range s.Tags {
			if strings.TrimSpace(v) == "" {
				continue
			}
			if strings.TrimSpace(existing.Tags[k]) == "" {
				existing.Tags[k] = v
			}
		}
		for k, v := range s.Fields {
			existing.Fields[k] = v
		}
	}

	rows := make([]sample, 0, len(order))
	for _, key := range order {
		rows = append(rows, *rowsByKey[key])
	}
	return rows
}

func firstNonEmptyTagValue(tags map[string]string, keys ...string) string {
	for _, k := range keys {
		if tags == nil {
			return ""
		}
		if v := strings.TrimSpace(tags[k]); v != "" {
			return v
		}
	}
	return ""
}

func copyStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyInterfaceMap(in map[string]interface{}) map[string]interface{} {
	if len(in) == 0 {
		return map[string]interface{}{}
	}
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func writeSamplesWithWorkers(w storageWriter, rows []sample, workers int) {
	if len(rows) == 0 {
		return
	}
	if workers < 1 {
		workers = 1
	}
	if workers > len(rows) {
		workers = len(rows)
	}
	if workers == 1 {
		for _, s := range rows {
			w.Write(s.Tags, s.Fields, s.Timestamp)
		}
		return
	}

	jobs := make(chan sample, workers*2)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range jobs {
				w.Write(s.Tags, s.Fields, s.Timestamp)
			}
		}()
	}
	for _, s := range rows {
		jobs <- s
	}
	close(jobs)
	wg.Wait()
}

func isTimescaleLikeWriter(w storageWriter) bool {
	switch w.(type) {
	case *timescale.Writer, *timescale_shadow.Writer:
		return true
	default:
		return false
	}
}

func pollDevice(plant string, dev internal.Device, ts time.Time) ([]sample, string, error) {
	var out strings.Builder
	fmt.Fprintf(&out, "Device: %s\n", dev.Name)

	session, err := internal.NewPollSession(dev)
	if err != nil {
		addr := dev.IP + ":" + strconv.Itoa(dev.Port)
		fmt.Fprintf(&out, "  ERROR: unable to connect to Modbus endpoint %s (%s): %v\n", addr, dev.TransportMode(), err)
		return nil, out.String(), err
	}
	defer session.Close()

	deviceSamples := make([]sample, 0, 256)
	for _, slave := range dev.Slaves {
		fmt.Fprintf(&out, "  Slave: %s\n", slave.Name)
		if !internal.RunSlaveHealthcheck(dev, slave, session) {
			fmt.Fprintf(&out, "    healthcheck failed for slave %s; skipping\n", slave.Name)
			if internal.ShouldFailRunOnDeviceFailure(dev) {
				return nil, out.String(), fmt.Errorf("healthcheck failed for slave %s", slave.Name)
			}
			continue
		}
		session.SetSlaveID(byte(slave.SlaveID))

		for _, reg := range slave.Registers {
			resp, err := internal.ReadRegisters(session, reg, slave.Offset)
			if err != nil {
				fmt.Fprintf(&out, "    read err fc=%d addr=%d words=%d: %v\n", reg.FunctionCode, reg.Register, reg.Words, err)
				continue
			}
			want := internal.ExpectedResponseBytes(reg)
			if len(resp) != want {
				fmt.Fprintf(&out, "    unexpected length at addr=%d: got=%d want=%d\n", reg.Register, len(resp), want)
				continue
			}
			if reg.Name == "" {
				fmt.Fprintf(&out, "    register at addr=%d has empty name; skipping\n", reg.Register)
				continue
			}
			decodedResp, err := internal.DecodeResponseBytes(reg, resp)
			if err != nil {
				fmt.Fprintf(&out, "    decode err fc=%d addr=%d words=%d: %v\n", reg.FunctionCode, reg.Register, reg.Words, err)
				continue
			}
			// writeValue: float64 for numeric types, string for STR/UTF8/HEX.
			var writeValue interface{}
			switch reg.Datatype {
			case "U8":
				v := float64(internal.U8(decodedResp)) * reg.Gain
				fmt.Fprintf(&out, "    [%s] %-28s -> %.6f %s\n", ts, reg.Name, v, reg.Unit)
				writeValue = v

			case "U16":
				v := float64(internal.U16(decodedResp)) * reg.Gain
				fmt.Fprintf(&out, "    [%s] %-28s -> %.6f %s\n", ts, reg.Name, v, reg.Unit)
				writeValue = v

			case "S16":
				v := float64(internal.S16(decodedResp)) * reg.Gain
				fmt.Fprintf(&out, "    [%s] %-28s -> %.6f %s\n", ts, reg.Name, v, reg.Unit)
				writeValue = v

			case "U32":
				v := float64(internal.U32(decodedResp)) * reg.Gain
				fmt.Fprintf(&out, "    [%s] %-28s -> %.6f %s\n", ts, reg.Name, v, reg.Unit)
				writeValue = v

			case "S32":
				v := float64(internal.S32(decodedResp)) * reg.Gain
				fmt.Fprintf(&out, "    [%s] %-28s -> %.6f %s\n", ts, reg.Name, v, reg.Unit)
				writeValue = v

			case "STR", "UTF8":
				s := internal.UTF8(decodedResp)
				fmt.Fprintf(&out, "    [%s] %-28s -> %s %s\n", ts, reg.Name, s, reg.Unit)
				writeValue = s

			case "HEX":
				s := internal.RawHex(decodedResp)
				fmt.Fprintf(&out, "    [%s] %-28s -> %s %s\n", ts, reg.Name, s, reg.Unit)
				writeValue = s

			case "U32LE":
				v := float64(internal.U32LE(decodedResp)) * reg.Gain
				fmt.Fprintf(&out, "    [%s] %-28s -> %.6f %s\n", ts, reg.Name, v, reg.Unit)
				writeValue = v

			case "S32LE":
				v := float64(internal.S32LE(decodedResp)) * reg.Gain
				fmt.Fprintf(&out, "    [%s] %-28s -> %.6f %s\n", ts, reg.Name, v, reg.Unit)
				writeValue = v

			case "F32BE":
				v := float64(internal.F32BE(decodedResp)) * reg.Gain
				fmt.Fprintf(&out, "    [%s] %-28s -> %.6f %s\n", ts, reg.Name, v, reg.Unit)
				writeValue = v

			case "U64BE":
				v := float64(internal.U64BE(decodedResp)) * reg.Gain
				fmt.Fprintf(&out, "    [%s] %-28s -> %.6f %s\n", ts, reg.Name, v, reg.Unit)
				writeValue = v

			case "S64BE":
				v := float64(internal.S64BE(decodedResp)) * reg.Gain
				fmt.Fprintf(&out, "    [%s] %-28s -> %.6f %s\n", ts, reg.Name, v, reg.Unit)
				writeValue = v

			default:
				fmt.Fprintf(&out, "    unknown datatype=%q at addr=%d (raw=% x)\n", reg.Datatype, reg.Register, resp)
				continue
			}

			// One sample per register. raw_<name> is kept for backends that use raw telemetry.
			tags := internal.MergeTags(plant, &dev, &slave, &reg)
			fields := map[string]interface{}{
				reg.Name:          writeValue,
				"raw_" + reg.Name: internal.RawHex(resp),
			}
			deviceSamples = append(deviceSamples, sample{Tags: tags, Fields: fields, Timestamp: ts})
		}
	}

	return deviceSamples, out.String(), nil
}

func main() {
	// Stage 0: bootstrap (flags, environment, and execution timing).
	fmt.Println("Time of execution:", time.Now().UTC().Format("2006-01-02 15:04:05"))
	flag.Parse()
	if *configPath == "" {
		log.Fatalf("Registers file path is required")
	}

	if err := dotenv.Load(*envPath); err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	ts := time.Now().Truncate(time.Minute).UTC()
	begin := time.Now()

	// Stage 1: load polling configuration (devices/slaves/registers).
	var devices internal.Devices
	if err := internal.LoadRegisters(*configPath, &devices); err != nil {
		log.Fatalf("Error loading registers file: %v", err)
	}

	var writers []storageWriter

	// Stage 2: initialize storage outputs from storage.outputs[].
	if *interrogateOnly {
		fmt.Println("Interrogation-only mode enabled: skipping writes to all configured storage outputs")
	} else {
		storageCfg, err := storage.LoadConfig(*configPath)
		if err != nil {
			log.Fatalf("Error loading storage config: %v", err)
		}

		for i, output := range storageCfg.Outputs {
			if !output.IsEnabled() {
				continue
			}

			switch strings.ToLower(strings.TrimSpace(output.Type)) {
			case "influxdb2":
				w, err := influx.NewWriter(output.Name, influx.Config{
					HostEnv:     output.Influxdb2.HostEnv,
					TokenEnv:    output.Influxdb2.TokenEnv,
					OrgEnv:      output.Influxdb2.OrgEnv,
					Bucket:      output.Influxdb2.Bucket,
					Measurement: output.Influxdb2.Measurement,
				})
				if err != nil {
					log.Fatalf("Error initializing storage output %q: %v", output.Name, err)
				}
				writers = append(writers, w)
			case "timescaledb":
				w, err := timescale.NewWriter(output.Name, timescale.Config{
					HostEnv:     output.Timescaledb.HostEnv,
					PortEnv:     output.Timescaledb.PortEnv,
					UserEnv:     output.Timescaledb.UserEnv,
					PasswordEnv: output.Timescaledb.PasswordEnv,
					DatabaseEnv: output.Timescaledb.DatabaseEnv,
					Schema:      output.Timescaledb.Schema,
					Table:       output.Timescaledb.Table,
				})
				if err != nil {
					log.Fatalf("Error initializing storage output %q: %v", output.Name, err)
				}
				writers = append(writers, w)
			case "timescaledb_shadow":
				w, err := timescale_shadow.NewWriter(output.Name, timescale_shadow.Config{
					HostEnv:     output.TimescaledbShadow.HostEnv,
					PortEnv:     output.TimescaledbShadow.PortEnv,
					UserEnv:     output.TimescaledbShadow.UserEnv,
					PasswordEnv: output.TimescaledbShadow.PasswordEnv,
					DatabaseEnv: output.TimescaledbShadow.DatabaseEnv,
					Schema:      output.TimescaledbShadow.Schema,
					Table:       output.TimescaledbShadow.Table,
				})
				if err != nil {
					log.Fatalf("Error initializing storage output %q: %v", output.Name, err)
				}
				writers = append(writers, w)
			default:
				log.Fatalf("Unsupported storage output type %q at storage.outputs[%d]", output.Type, i)
			}
		}

		if len(writers) == 0 {
			log.Fatalf("No enabled storage outputs configured")
		}
		defer func() {
			for _, w := range writers {
				w.Close()
			}
		}()
	}

	// Stage 3: connect, run optional healthchecks, read/decode Modbus data, and accumulate samples.
	samples := make([]sample, 0, 1024)

	type devicePollResult struct {
		index            int
		deviceName       string
		failRunOnFailure bool
		samples          []sample
		output           string
		err              error
	}

	fmt.Printf("Processing %d devices concurrently...\n", len(devices.Devices))
	results := make(chan devicePollResult, len(devices.Devices))
	var wg sync.WaitGroup
	for idx, devItem := range devices.Devices {
		dev := devItem.Device
		wg.Add(1)
		go func(idx int, plant string, dev internal.Device) {
			defer wg.Done()

			deviceSamples, output, err := pollDevice(plant, dev, ts)
			results <- devicePollResult{
				index:            idx,
				deviceName:       dev.Name,
				failRunOnFailure: internal.ShouldFailRunOnDeviceFailure(dev),
				samples:          deviceSamples,
				output:           output,
				err:              err,
			}
		}(idx, devices.Plant, dev)
	}
	wg.Wait()
	close(results)

	failRunErrors := make([]string, 0, 4)
	orderedResults := make([]devicePollResult, len(devices.Devices))
	for res := range results {
		orderedResults[res.index] = res
	}
	for _, res := range orderedResults {
		if strings.TrimSpace(res.output) != "" {
			fmt.Print(res.output)
		}
		samples = append(samples, res.samples...)
		if res.err != nil && res.failRunOnFailure {
			failRunErrors = append(failRunErrors, fmt.Sprintf("%s (%v)", res.deviceName, res.err))
		}
	}
	if len(failRunErrors) > 0 {
		log.Fatalf("Device failures with healthcheck.on_fail=fail_run: %s", strings.Join(failRunErrors, "; "))
	}

	// Stage 4: dispatch accumulated samples to each configured output.
	if len(writers) > 0 {
		dispatchBegin := time.Now()
		fmt.Printf("Dispatching %d samples to %d storage outputs\n", len(samples), len(writers))
		var timescaleRows []sample
		timescaleRowsReady := false

		for _, w := range writers {
			outputBegin := time.Now()
			if !w.Available() {
				fmt.Printf("Skipping output: %s (backend unavailable)\n", w.Name())
				continue
			}
			fmt.Printf("Writing to output: %s\n", w.Name())

			if isTimescaleLikeWriter(w) {
				if !timescaleRowsReady {
					aggBegin := time.Now()
					timescaleRows = aggregateSamplesForTimescale(samples)
					timescaleRowsReady = true
					fmt.Printf("Timescale row aggregation: %d samples -> %d row upserts (%s)\n", len(samples), len(timescaleRows), time.Since(aggBegin))
				}
				workers := runtime.GOMAXPROCS(0)
				if workers > timescaleWriteWorkers {
					workers = timescaleWriteWorkers
				}
				if workers > len(timescaleRows) {
					workers = len(timescaleRows)
				}
				if workers < 1 {
					workers = 1
				}
				fmt.Printf("Timescale write workers: %d\n", workers)
				writeBegin := time.Now()
				writeSamplesWithWorkers(w, timescaleRows, workers)
				writeDuration := time.Since(writeBegin)
				flushBegin := time.Now()
				w.Flush()
				flushDuration := time.Since(flushBegin)
				fmt.Printf("Output %s timings: write=%s flush=%s total=%s\n", w.Name(), writeDuration, flushDuration, time.Since(outputBegin))
				continue
			}

			writeBegin := time.Now()
			for _, s := range samples {
				w.Write(s.Tags, s.Fields, s.Timestamp)
			}
			writeDuration := time.Since(writeBegin)
			flushBegin := time.Now()
			w.Flush()
			flushDuration := time.Since(flushBegin)
			fmt.Printf("Output %s timings: write=%s flush=%s total=%s\n", w.Name(), writeDuration, flushDuration, time.Since(outputBegin))
		}
		fmt.Printf("Stage 4 dispatch time: %s\n", time.Since(dispatchBegin))
	}

	// Stage 5: finish execution.
	fmt.Println("Time taken:", time.Since(begin))
}
