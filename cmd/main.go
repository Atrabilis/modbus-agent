package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/atrabilis/modbus-agent/internal"
	"github.com/atrabilis/modbus-agent/storage"
	"github.com/atrabilis/modbus-agent/storage/influx"

	"github.com/goburrow/modbus"
	dotenv "github.com/joho/godotenv"
)

var (
	configPath      = flag.String("configPath", "", "Path to the config file")
	envPath         = flag.String("envPath", "/etc/atamostec/modbus-agent/modbus-agent.env", "Path to the dotenv file")
	interrogateOnly = flag.Bool("interrogateOnly", false, "Read devices without writing to configured storage outputs")
)

type storageWriter interface {
	Name() string
	Write(tags map[string]string, fields map[string]interface{}, ts time.Time)
	Flush()
	Close()
}

type sample struct {
	Tags      map[string]string
	Fields    map[string]interface{}
	Timestamp time.Time
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

			switch output.Type {
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

	for _, devItem := range devices.Devices {
		dev := devItem.Device
		fmt.Println("Device:", dev.Name)
		failRunOnFailure := internal.ShouldFailRunOnDeviceFailure(dev)

		addr := dev.IP + ":" + strconv.Itoa(dev.Port)
		handler := modbus.NewTCPClientHandler(addr)
		handler.Timeout = 5 * time.Second

		if err := handler.Connect(); err != nil {
			log.Printf("Device %s: unable to connect to Modbus endpoint %s: %v", dev.Name, addr, err)
			if failRunOnFailure {
				log.Fatalf("Device %s failed and healthcheck.on_fail=fail_run", dev.Name)
			}
			continue
		}
		client := modbus.NewClient(handler)
		if !internal.RunDeviceHealthcheck(dev, handler, client) {
			if err := handler.Close(); err != nil {
				log.Printf("close error for device %s after healthcheck failure: %v", dev.Name, err)
			}
			if failRunOnFailure {
				log.Fatalf("Device %s failed healthcheck and healthcheck.on_fail=fail_run", dev.Name)
			}
			log.Printf("Device %s: skipping polling because healthcheck failed", dev.Name)
			continue
		}

		for _, slave := range dev.Slaves {
			fmt.Println("  Slave:", slave.Name)
			handler.SlaveId = byte(slave.SlaveID)

			for _, reg := range slave.Registers {
				resp, err := internal.ReadRegisters(client, reg, slave.Offset)
				if err != nil {
					log.Printf("    read err fc=%d addr=%d words=%d: %v", reg.FunctionCode, reg.Register, reg.Words, err)
					continue
				}
				want := internal.ExpectedResponseBytes(reg)
				if len(resp) != want {
					log.Printf("    unexpected length at addr=%d: got=%d want=%d", reg.Register, len(resp), want)
					continue
				}
				if reg.Name == "" {
					log.Printf("    register at addr=%d has empty name; skipping", reg.Register)
					continue
				}
				decodedResp, err := internal.DecodeResponseBytes(reg, resp)
				if err != nil {
					log.Printf("    decode err fc=%d addr=%d words=%d: %v", reg.FunctionCode, reg.Register, reg.Words, err)
					continue
				}
				// writeValue: float64 for numeric types, string for STR/UTF8/HEX.
				var writeValue interface{}
				switch reg.Datatype {
				case "U8":
					v := float64(internal.U8(decodedResp)) * reg.Gain
					fmt.Printf("    [%s] %-28s -> %.6f %s\n", ts, reg.Name, v, reg.Unit)
					writeValue = v

				case "U16":
					v := float64(internal.U16(decodedResp)) * reg.Gain
					fmt.Printf("    [%s] %-28s -> %.6f %s\n", ts, reg.Name, v, reg.Unit)
					writeValue = v

				case "S16":
					v := float64(internal.S16(decodedResp)) * reg.Gain
					fmt.Printf("    [%s] %-28s -> %.6f %s\n", ts, reg.Name, v, reg.Unit)
					writeValue = v

				case "U32":
					v := float64(internal.U32(decodedResp)) * reg.Gain
					fmt.Printf("    [%s] %-28s -> %.6f %s\n", ts, reg.Name, v, reg.Unit)
					writeValue = v

				case "S32":
					v := float64(internal.S32(decodedResp)) * reg.Gain
					fmt.Printf("    [%s] %-28s -> %.6f %s\n", ts, reg.Name, v, reg.Unit)
					writeValue = v

				case "STR", "UTF8":
					s := internal.UTF8(decodedResp)
					fmt.Printf("    [%s] %-28s -> %s %s\n", ts, reg.Name, s, reg.Unit)
					writeValue = s

				case "HEX":
					s := internal.RawHex(decodedResp)
					fmt.Printf("    [%s] %-28s -> %s %s\n", ts, reg.Name, s, reg.Unit)
					writeValue = s

				case "U32LE":
					v := float64(internal.U32LE(decodedResp)) * reg.Gain
					fmt.Printf("    [%s] %-28s -> %.6f %s\n", ts, reg.Name, v, reg.Unit)
					writeValue = v

				case "S32LE":
					v := float64(internal.S32LE(decodedResp)) * reg.Gain
					fmt.Printf("    [%s] %-28s -> %.6f %s\n", ts, reg.Name, v, reg.Unit)
					writeValue = v

				default:
					log.Printf("    unknown datatype=%q at addr=%d (raw=% x)", reg.Datatype, reg.Register, resp)
					continue
				}

				// One sample per register. raw_<name> preserves the raw Modbus response.
				tags := internal.MergeTags(&dev, &slave, &reg)
				fields := map[string]interface{}{
					reg.Name:          writeValue,
					"raw_" + reg.Name: internal.RawHex(resp),
				}
				samples = append(samples, sample{Tags: tags, Fields: fields, Timestamp: ts})
			}
		}

		// Close TCP once all slaves for this device have been processed.
		if err := handler.Close(); err != nil {
			log.Printf("close error for device %s: %v", dev.Name, err)
		}

	}

	// Stage 4: dispatch accumulated samples to each configured output.
	if len(writers) > 0 {
		fmt.Printf("Dispatching %d samples to %d storage outputs\n", len(samples), len(writers))
		for _, w := range writers {
			fmt.Printf("Writing to output: %s\n", w.Name())
			for _, s := range samples {
				w.Write(s.Tags, s.Fields, s.Timestamp)
			}
			w.Flush()
		}
	}

	// Stage 5: finish execution.
	fmt.Println("Time taken:", time.Since(begin))
}
