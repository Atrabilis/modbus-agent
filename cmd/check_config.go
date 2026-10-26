package main

import (
	"fmt"
	"io"
	"os"

	"github.com/atrabilis/modbus-agent/internal"
)

// runCheckConfig validates the register configuration at path and returns the
// process exit status: 0 when every register can be decoded, 1 otherwise.
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

	_, issues := internal.ValidateDevices(devices)
	for _, issue := range issues {
		fmt.Fprintf(out, "INVALID %s\n", issue)
	}
	if len(issues) > 0 {
		fmt.Fprintf(out, "%s: %d of %d registers are invalid\n", path, len(issues), total)
		return 1
	}
	fmt.Fprintf(out, "%s: %d registers OK\n", path, total)
	return 0
}
