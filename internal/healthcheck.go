package internal

import (
	"fmt"
	"log"
	"time"

	"github.com/goburrow/modbus"
)

type modbusReadClient interface {
	ReadCoils(address, quantity uint16) (results []byte, err error)
	ReadHoldingRegisters(address, quantity uint16) (results []byte, err error)
	ReadInputRegisters(address, quantity uint16) (results []byte, err error)
}

func ShouldFailRunOnDeviceFailure(dev Device) bool {
	return dev.Healthcheck != nil && dev.Healthcheck.IsEnabled() && dev.Healthcheck.OnFailMode() == "fail_run"
}

func RunDeviceHealthcheck(dev Device, handler *modbus.TCPClientHandler, client modbusReadClient) bool {
        return RunSlaveHealthcheck(dev, Slave{}, handler, client)
}

func RunSlaveHealthcheck(dev Device, slave Slave, handler *modbus.TCPClientHandler, client modbusReadClient) bool {
        if dev.Healthcheck == nil || !dev.Healthcheck.IsEnabled() {
                return true
        }

        if len(dev.Healthcheck.Probes) == 0 {
                log.Printf("Device %s: healthcheck is enabled but probes are empty", dev.Name)
                return false
        }

        policy := dev.Healthcheck.SuccessPolicyMode()
        passed := 0
        for i, probe := range dev.Healthcheck.Probes {
                if runHealthProbe(dev.Name, i, probe, &slave, handler, client) {
                        passed++
                        if policy == "any" {
                                log.Printf("Device %s: healthcheck passed (policy=any)", dev.Name)
                                return true
                        }
                } else if policy == "all" {
                        log.Printf("Device %s: healthcheck failed (policy=all)", dev.Name)
                        return false
                }
        }

        if policy == "all" {
                log.Printf("Device %s: healthcheck passed (policy=all)", dev.Name)
                return true
        }

        if passed > 0 {
                log.Printf("Device %s: healthcheck passed (policy=any)", dev.Name)
                return true
        }
        log.Printf("Device %s: healthcheck failed (policy=any)", dev.Name)
        return false
}

func runHealthProbe(devName string, idx int, probe HealthcheckProbe, slave *Slave, handler *modbus.TCPClientHandler, client modbusReadClient) bool {
        probeName := probe.Name
        if probeName == "" {
                probeName = fmt.Sprintf("probe_%d", idx)
        }

        effectiveSlaveID := probe.SlaveID
        if effectiveSlaveID == 0 && slave != nil && slave.SlaveID != 0 {
                effectiveSlaveID = slave.SlaveID
        }
        if effectiveSlaveID < 0 || effectiveSlaveID > 255 {
                log.Printf("Device %s: healthcheck %s invalid slave_id=%d", devName, probeName, effectiveSlaveID)
                return false
        }

        functionCode := probe.FunctionCode
        if functionCode == 0 {
                functionCode = 3
        }

        words := probe.Words
        if words <= 0 {
                words = 1
        }

        attempts := probe.Retries + 1
        if attempts < 1 {
                attempts = 1
        }

        defaultTimeout := handler.Timeout
        if probe.TimeoutMs > 0 {
                handler.Timeout = time.Duration(probe.TimeoutMs) * time.Millisecond
        }
        defer func() {
                handler.Timeout = defaultTimeout
        }()

        oldSlaveID := handler.SlaveId
        handler.SlaveId = byte(effectiveSlaveID)
        defer func() {
                handler.SlaveId = oldSlaveID
        }()

        reg := Register{
                Register:     probe.Register,
                FunctionCode: functionCode,
                Words:        words,
        }

        for attempt := 1; attempt <= attempts; attempt++ {
		resp, err := ReadRegisters(client, reg, probe.Offset)
		if err != nil {
			log.Printf("Device %s: healthcheck %s attempt=%d/%d failed: %v", devName, probeName, attempt, attempts, err)
			continue
		}
		want := ExpectedResponseBytes(reg)
		if len(resp) != want {
			log.Printf(
				"Device %s: healthcheck %s attempt=%d/%d unexpected length got=%d want=%d",
				devName,
				probeName,
				attempt,
				attempts,
				len(resp),
				want,
			)
			continue
		}
		log.Printf("Device %s: healthcheck %s passed at attempt=%d/%d", devName, probeName, attempt, attempts)
		return true
	}

	return false
}
