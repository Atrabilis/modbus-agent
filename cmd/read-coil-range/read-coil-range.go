package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/goburrow/modbus"
)

func main() {
	ip := flag.String("ip", "", "Modbus TCP device IP")
	port := flag.Int("port", 502, "Modbus TCP port")
	slaveID := flag.Int("slave-id", 1, "Modbus slave ID")
	startRegister := flag.Int("start-register", 0, "Starting coil register/address")
	quantity := flag.Int("quantity", 1, "Number of coils to read")
	timeout := flag.Duration("timeout", 5*time.Second, "TCP timeout")

	flag.Parse()

	if *ip == "" {
		log.Fatal("flag --ip is required")
	}
	if *startRegister < 0 {
		log.Fatal("flag --start-register must be >= 0")
	}
	if *quantity <= 0 {
		log.Fatal("flag --quantity must be > 0")
	}
	if *quantity > 2000 {
		log.Fatal("flag --quantity must be <= 2000 for FC1")
	}
	if *slaveID < 0 || *slaveID > 255 {
		log.Fatal("flag --slave-id must be between 0 and 255")
	}

	addr := fmt.Sprintf("%s:%d", *ip, *port)
	handler := modbus.NewTCPClientHandler(addr)
	handler.Timeout = *timeout
	handler.SlaveId = byte(*slaveID)

	if err := handler.Connect(); err != nil {
		log.Fatalf("Error connecting to Modbus (%s): %v", addr, err)
	}
	defer func() {
		if err := handler.Close(); err != nil {
			log.Printf("close error: %v", err)
		}
	}()

	client := modbus.NewClient(handler)

	ts := time.Now().UTC().Format("2006-01-02 15:04:05")
	begin := time.Now()

	resp, err := client.ReadCoils(uint16(*startRegister), uint16(*quantity))
	if err != nil {
		log.Fatalf("read err fc=1 addr=%d quantity=%d: %v", *startRegister, *quantity, err)
	}

	expectedBytes := (*quantity + 7) / 8
	if len(resp) != expectedBytes {
		log.Printf("unexpected response length: got=%d want=%d", len(resp), expectedBytes)
	}

	fmt.Printf("Time of execution: %s\n", ts)
	fmt.Printf("Device: %s\n", addr)
	fmt.Printf("Slave: %d\n", *slaveID)
	fmt.Printf("ReadCoils(start=%d, quantity=%d)\n", *startRegister, *quantity)
	fmt.Printf("Raw response bytes: % X\n", resp)
	fmt.Println(strings.Repeat("-", 72))

	for i := 0; i < *quantity; i++ {
		coilAddress := *startRegister + i
		byteIndex := i / 8
		bitIndex := uint(i % 8)

		var value uint8 = 0
		if byteIndex < len(resp) && (resp[byteIndex]&(1<<bitIndex)) != 0 {
			value = 1
		}

		m := coilAddress - 8256
		fmt.Printf("    [%s] coil_%-6d (M%-2d) -> %d\n", ts, coilAddress, m, value)
	}

	fmt.Println(strings.Repeat("-", 72))
	fmt.Printf("Time taken: %s\n", time.Since(begin))
}
