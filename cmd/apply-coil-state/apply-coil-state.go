package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/goburrow/modbus"
	"gopkg.in/yaml.v3"
)

type DesiredStateFile struct {
	Device DeviceConfig `yaml:"device"`
	Writes []CoilWrite  `yaml:"writes"`
}

type DeviceConfig struct {
	Name    string `yaml:"name"`
	IP      string `yaml:"ip"`
	Port    int    `yaml:"port"`
	SlaveID int    `yaml:"slave_id"`
	Offset  int    `yaml:"offset"`
}

type CoilWrite struct {
	Register int    `yaml:"register"`
	Name     string `yaml:"name"`
	Value    bool   `yaml:"value"`
}

func effectiveAddress(register int, offset int) (uint16, error) {
	addr := register - offset
	if addr < 0 || addr > 65535 {
		return 0, fmt.Errorf("effective address out of range: register=%d offset=%d -> %d", register, offset, addr)
	}
	return uint16(addr), nil
}

func loadDesiredState(path string) (*DesiredStateFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg DesiredStateFile
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	if cfg.Device.IP == "" {
		return nil, fmt.Errorf("device.ip is required")
	}
	if cfg.Device.Port <= 0 {
		return nil, fmt.Errorf("device.port must be > 0")
	}
	if cfg.Device.SlaveID < 0 || cfg.Device.SlaveID > 255 {
		return nil, fmt.Errorf("device.slave_id must be between 0 and 255")
	}
	if len(cfg.Writes) == 0 {
		return nil, fmt.Errorf("writes is empty")
	}

	for i, w := range cfg.Writes {
		if w.Register < 0 || w.Register > 65535 {
			return nil, fmt.Errorf("writes[%d].register out of range: %d", i, w.Register)
		}
	}

	return &cfg, nil
}

func readSingleCoil(client modbus.Client, address uint16) (bool, []byte, error) {
	resp, err := client.ReadCoils(address, 1)
	if err != nil {
		return false, nil, err
	}
	if len(resp) < 1 {
		return false, resp, fmt.Errorf("empty coil response")
	}
	value := (resp[0] & 0x01) != 0
	return value, resp, nil
}

func writeSingleCoil(client modbus.Client, address uint16, value bool) ([]byte, error) {
	var rawValue uint16
	if value {
		rawValue = 0xFF00
	} else {
		rawValue = 0x0000
	}
	return client.WriteSingleCoil(address, rawValue)
}

func coilLabel(register int) string {
	if register >= 8257 && register <= 8320 {
		return fmt.Sprintf("M%d", register-8256)
	}
	return ""
}

func main() {
	configPath := flag.String("config", "", "Path to desired-state YAML")
	timeout := flag.Duration("timeout", 5*time.Second, "TCP timeout")
	dryRun := flag.Bool("dry-run", false, "Print intended writes without applying them")
	verify := flag.Bool("verify", true, "Read back each coil after write")
	readBefore := flag.Bool("read-before", true, "Read current value before write")
	flag.Parse()

	if *configPath == "" {
		log.Fatal("flag --config is required")
	}

	cfg, err := loadDesiredState(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ts := time.Now().UTC().Format("2006-01-02 15:04:05")
	begin := time.Now()

	addr := fmt.Sprintf("%s:%d", cfg.Device.IP, cfg.Device.Port)
	handler := modbus.NewTCPClientHandler(addr)
	handler.Timeout = *timeout
	handler.SlaveId = byte(cfg.Device.SlaveID)

	if err := handler.Connect(); err != nil {
		log.Fatalf("error connecting to Modbus (%s): %v", addr, err)
	}
	defer func() {
		if err := handler.Close(); err != nil {
			log.Printf("close error: %v", err)
		}
	}()

	client := modbus.NewClient(handler)

	fmt.Printf("Time of execution: %s\n", ts)
	fmt.Printf("Device: %s\n", cfg.Device.Name)
	fmt.Printf("Address: %s\n", addr)
	fmt.Printf("Slave: %d\n", cfg.Device.SlaveID)
	fmt.Printf("Dry run: %v\n", *dryRun)
	fmt.Printf("Verify: %v\n", *verify)
	fmt.Println("------------------------------------------------------------------------")

	for _, w := range cfg.Writes {
		label := coilLabel(w.Register)
		targetText := fmt.Sprintf("coil_%d", w.Register)
		if label != "" {
			targetText = fmt.Sprintf("%s (%s)", label, targetText)
		}
		if w.Name != "" {
			targetText = fmt.Sprintf("%s [%s]", targetText, w.Name)
		}

		effectiveAddr, err := effectiveAddress(w.Register, cfg.Device.Offset)
		if err != nil {
			log.Printf("    [%s] %s address error: %v", ts, targetText, err)
			continue
		}

		desiredFloat := 0.0
		if w.Value {
			desiredFloat = 1.0
		}

		if *readBefore {
			currentValue, rawResp, err := readSingleCoil(client, effectiveAddr)
			if err != nil {
				log.Printf("    [%s] %s read-before error: %v", ts, targetText, err)
				continue
			}

			currentFloat := 0.0
			if currentValue {
				currentFloat = 1.0
			}

			fmt.Printf(
				"    [%s] %s current -> %.6f (doc=%d wire=%d raw=% X)\n",
				ts, targetText, currentFloat, w.Register, effectiveAddr, rawResp,
			)
		}

		if *dryRun {
			fmt.Printf(
				"    [%s] %s desired -> %.6f (doc=%d wire=%d dry-run, no write)\n",
				ts, targetText, desiredFloat, w.Register, effectiveAddr,
			)
			continue
		}

		writeResp, err := writeSingleCoil(client, effectiveAddr, w.Value)
		if err != nil {
			log.Printf("    [%s] %s write error: %v", ts, targetText, err)
			continue
		}

		fmt.Printf(
			"    [%s] %s write   -> %.6f (doc=%d wire=%d raw=% X)\n",
			ts, targetText, desiredFloat, w.Register, effectiveAddr, writeResp,
		)

		if *verify {
			verifiedValue, rawResp, err := readSingleCoil(client, effectiveAddr)
			if err != nil {
				log.Printf("    [%s] %s verify error: %v", ts, targetText, err)
				continue
			}

			verifiedFloat := 0.0
			if verifiedValue {
				verifiedFloat = 1.0
			}

			status := "OK"
			if verifiedValue != w.Value {
				status = "MISMATCH"
			}

			fmt.Printf(
				"    [%s] %s verify  -> %.6f (doc=%d wire=%d raw=% X) [%s]\n",
				ts, targetText, verifiedFloat, w.Register, effectiveAddr, rawResp, status,
			)
		}
	}

	fmt.Println("------------------------------------------------------------------------")
	fmt.Printf("Time taken: %s\n", time.Since(begin))
}
