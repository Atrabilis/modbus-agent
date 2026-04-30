package internal

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/goburrow/modbus"
)

type PollSession interface {
	ReadCoils(address, quantity uint16) (results []byte, err error)
	ReadHoldingRegisters(address, quantity uint16) (results []byte, err error)
	ReadInputRegisters(address, quantity uint16) (results []byte, err error)
	SetSlaveID(id byte)
	SlaveID() byte
	SetTimeout(timeout time.Duration)
	Timeout() time.Duration
	Close() error
}

func NewPollSession(dev Device) (PollSession, error) {
	addr := net.JoinHostPort(dev.IP, strconv.Itoa(dev.Port))

	switch dev.TransportMode() {
	case "rtu_over_tcp":
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			return nil, fmt.Errorf("unable to connect to Modbus RTU-over-TCP endpoint %s: %w", addr, err)
		}
		return &rtuOverTCPSession{
			conn:    conn,
			timeout: 500 * time.Millisecond,
		}, nil
	default:
		handler := modbus.NewTCPClientHandler(addr)
		handler.Timeout = 500 * time.Millisecond
		if err := handler.Connect(); err != nil {
			return nil, fmt.Errorf("unable to connect to Modbus TCP endpoint %s: %w", addr, err)
		}
		return &tcpPollSession{
			handler: handler,
			client:  modbus.NewClient(handler),
		}, nil
	}
}

type tcpPollSession struct {
	handler *modbus.TCPClientHandler
	client  modbus.Client
}

func (s *tcpPollSession) ReadCoils(address, quantity uint16) ([]byte, error) {
	return s.client.ReadCoils(address, quantity)
}

func (s *tcpPollSession) ReadHoldingRegisters(address, quantity uint16) ([]byte, error) {
	return s.client.ReadHoldingRegisters(address, quantity)
}

func (s *tcpPollSession) ReadInputRegisters(address, quantity uint16) ([]byte, error) {
	return s.client.ReadInputRegisters(address, quantity)
}

func (s *tcpPollSession) SetSlaveID(id byte) {
	s.handler.SlaveId = id
}

func (s *tcpPollSession) SlaveID() byte {
	return s.handler.SlaveId
}

func (s *tcpPollSession) SetTimeout(timeout time.Duration) {
	s.handler.Timeout = timeout
}

func (s *tcpPollSession) Timeout() time.Duration {
	return s.handler.Timeout
}

func (s *tcpPollSession) Close() error {
	return s.handler.Close()
}

type rtuOverTCPSession struct {
	conn    net.Conn
	timeout time.Duration
	slaveID byte
}

func (s *rtuOverTCPSession) ReadCoils(address, quantity uint16) ([]byte, error) {
	return s.readFrame(1, address, quantity)
}

func (s *rtuOverTCPSession) ReadHoldingRegisters(address, quantity uint16) ([]byte, error) {
	return s.readFrame(3, address, quantity)
}

func (s *rtuOverTCPSession) ReadInputRegisters(address, quantity uint16) ([]byte, error) {
	return s.readFrame(4, address, quantity)
}

func (s *rtuOverTCPSession) SetSlaveID(id byte) {
	s.slaveID = id
}

func (s *rtuOverTCPSession) SlaveID() byte {
	return s.slaveID
}

func (s *rtuOverTCPSession) SetTimeout(timeout time.Duration) {
	s.timeout = timeout
}

func (s *rtuOverTCPSession) Timeout() time.Duration {
	return s.timeout
}

func (s *rtuOverTCPSession) Close() error {
	return s.conn.Close()
}

func (s *rtuOverTCPSession) readFrame(function byte, address, quantity uint16) ([]byte, error) {
	req := buildRTUReadFrame(s.slaveID, function, address, quantity)

	if err := s.conn.SetWriteDeadline(time.Now().Add(s.timeout)); err != nil {
		return nil, err
	}
	if _, err := s.conn.Write(req); err != nil {
		return nil, err
	}

	if err := s.conn.SetReadDeadline(time.Now().Add(s.timeout)); err != nil {
		return nil, err
	}

	header := make([]byte, 3)
	if _, err := io.ReadFull(s.conn, header); err != nil {
		return nil, err
	}
	if header[0] != s.slaveID {
		return nil, fmt.Errorf("unexpected slave id in response: got=%d want=%d", header[0], s.slaveID)
	}

	if header[1]&0x80 != 0 {
		tail := make([]byte, 2)
		if _, err := io.ReadFull(s.conn, tail); err != nil {
			return nil, err
		}
		frame := append(header, tail...)
		if err := validateRTUCRC(frame); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("modbus exception function=0x%02X code=0x%02X", header[1], header[2])
	}

	byteCount := int(header[2])
	payloadWithCRC := make([]byte, byteCount+2)
	if _, err := io.ReadFull(s.conn, payloadWithCRC); err != nil {
		return nil, err
	}

	frame := append(header, payloadWithCRC...)
	if err := validateRTUCRC(frame); err != nil {
		return nil, err
	}

	return payloadWithCRC[:byteCount], nil
}

func buildRTUReadFrame(slaveID, function byte, address, quantity uint16) []byte {
	frame := []byte{
		slaveID,
		function,
		byte(address >> 8),
		byte(address),
		byte(quantity >> 8),
		byte(quantity),
	}
	crc := crc16(frame)
	frame = append(frame, byte(crc), byte(crc>>8))
	return frame
}

func validateRTUCRC(frame []byte) error {
	if len(frame) < 4 {
		return fmt.Errorf("frame too short for crc validation")
	}
	payload := frame[:len(frame)-2]
	seen := binary.LittleEndian.Uint16(frame[len(frame)-2:])
	got := crc16(payload)
	if seen != got {
		return fmt.Errorf("crc mismatch exp=0x%04X got=0x%04X", got, seen)
	}
	return nil
}

func crc16(b []byte) uint16 {
	var crc uint16 = 0xFFFF
	for _, v := range b {
		crc ^= uint16(v)
		for i := 0; i < 8; i++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ 0xA001
			} else {
				crc >>= 1
			}
		}
	}
	return crc
}
