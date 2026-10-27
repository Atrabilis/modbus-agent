package main

import (
	"fmt"
	"io"
	"os"

	"github.com/atrabilis/modbus-agent/internal"
	"github.com/atrabilis/modbus-agent/storage"
	"github.com/atrabilis/modbus-agent/storage/postgres"
	"github.com/atrabilis/modbus-agent/storage/postgres_shadow"
)

// runCheckConfig validates the configuration at path and returns the process
// exit status: 0 when the run would start and every register can be decoded,
// 1 otherwise.
func runCheckConfig(path string) int {
	return validateConfigFile(path, os.Stdout)
}

func validateConfigFile(path string, out io.Writer) int {
	var devices internal.Devices
	if err := internal.LoadRegisters(path, &devices); err != nil {
		fmt.Fprintf(out, "ERROR: cannot load registers file %s: %v\n", path, err)
		return 1
	}

	total := 0
	for _, item := range devices.Devices {
		for _, slave := range item.Device.Slaves {
			total += len(slave.Registers)
		}
	}

	problems := 0
	if _, err := devices.Resolution(); err != nil {
		fmt.Fprintf(out, "INVALID %v\n", err)
		problems++
	}
	problems += validateStorage(path, out)

	_, issues := internal.ValidateDevices(devices)
	for _, issue := range issues {
		fmt.Fprintf(out, "INVALID %s\n", issue)
	}
	if len(issues) > 0 {
		fmt.Fprintf(out, "%s: %d of %d registers are invalid\n", path, len(issues), total)
	}
	if len(issues) > 0 || problems > 0 {
		return 1
	}
	fmt.Fprintf(out, "%s: %d registers OK\n", path, total)
	return 0
}

// validateStorage reports storage outputs the agent would refuse to start
// with, and returns how many problems it found. Legacy type names are only
// warned about.
func validateStorage(path string, out io.Writer) int {
	cfg, err := storage.LoadConfig(path)
	if err != nil {
		fmt.Fprintf(out, "INVALID storage: %v\n", err)
		return 1
	}

	problems := 0
	enabled := 0
	for _, output := range cfg.Outputs {
		if !output.IsEnabled() {
			continue
		}
		enabled++
		kind, pgCfg, deprecated, err := output.Resolve()
		if err != nil {
			fmt.Fprintf(out, "INVALID storage: %v\n", err)
			problems++
			continue
		}
		if deprecated != "" {
			fmt.Fprintf(out, "WARNING: %s\n", deprecated)
		}
		switch kind {
		case storage.KindPostgres:
			err = postgres.Config(pgCfg).Validate(output.Name)
		case storage.KindPostgresShadow:
			err = postgres_shadow.Config(pgCfg).Validate(output.Name)
		}
		if err != nil {
			fmt.Fprintf(out, "INVALID storage: %v\n", err)
			problems++
		}
	}
	if enabled == 0 {
		fmt.Fprintln(out, "INVALID storage: no enabled storage outputs")
		problems++
	}
	return problems
}
