package internal

import "sort"

const (
	defaultTCPBlockWords        = 32
	defaultRTUOverTCPBlockWords = 8
	maxModbusRegisterBlockWords = 125
)

type PlannedRegisterRead struct {
	Slave            Slave
	Register         Register
	EffectiveAddress int
}

type ReadBlock struct {
	SlaveID      int
	FunctionCode int
	StartAddress int
	WordCount    int
	PlannedReads []PlannedRegisterRead
}

func PlanReadBlocks(dev Device) []ReadBlock {
	var planned []PlannedRegisterRead
	for _, slave := range dev.Slaves {
		for _, reg := range slave.Registers {
			effective := reg.Register - slave.Offset
			planned = append(planned, PlannedRegisterRead{
				Slave:            slave,
				Register:         reg,
				EffectiveAddress: effective,
			})
		}
	}

	sort.Slice(planned, func(i, j int) bool {
		a, b := planned[i], planned[j]
		if a.Slave.SlaveID != b.Slave.SlaveID {
			return a.Slave.SlaveID < b.Slave.SlaveID
		}
		if a.Register.FunctionCode != b.Register.FunctionCode {
			return a.Register.FunctionCode < b.Register.FunctionCode
		}
		if a.EffectiveAddress != b.EffectiveAddress {
			return a.EffectiveAddress < b.EffectiveAddress
		}
		return a.Register.Words < b.Register.Words
	})

	blocks := make([]ReadBlock, 0, len(planned))
	for _, item := range planned {
		if !blockReadsEnabled(dev) {
			blocks = append(blocks, newReadBlock(item))
			continue
		}

		if item.Register.FunctionCode == 1 {
			blocks = append(blocks, ReadBlock{
				SlaveID:      item.Slave.SlaveID,
				FunctionCode: item.Register.FunctionCode,
				StartAddress: item.EffectiveAddress,
				WordCount:    item.Register.Words,
				PlannedReads: []PlannedRegisterRead{item},
			})
			continue
		}

		maxWords := blockWordLimit(dev)
		maxGapWords := blockGapLimit(dev)
		if len(blocks) == 0 {
			blocks = append(blocks, newReadBlock(item))
			continue
		}

		last := &blocks[len(blocks)-1]
		if !canExtendBlock(*last, item, maxWords, maxGapWords) {
			blocks = append(blocks, newReadBlock(item))
			continue
		}

		last.PlannedReads = append(last.PlannedReads, item)
		blockEnd := item.EffectiveAddress + item.Register.Words
		last.WordCount = blockEnd - last.StartAddress
	}

	return blocks
}

func blockWordLimit(dev Device) int {
	if dev.ReadOptimization != nil && dev.ReadOptimization.MaxBlockWords > 0 {
		if dev.ReadOptimization.MaxBlockWords > maxModbusRegisterBlockWords {
			return maxModbusRegisterBlockWords
		}
		return dev.ReadOptimization.MaxBlockWords
	}

	switch dev.TransportMode() {
	case "rtu_over_tcp":
		return defaultRTUOverTCPBlockWords
	default:
		return defaultTCPBlockWords
	}
}

func blockGapLimit(dev Device) int {
	if dev.ReadOptimization != nil && dev.ReadOptimization.MaxGapWords > 0 {
		return dev.ReadOptimization.MaxGapWords
	}
	return 0
}

func blockReadsEnabled(dev Device) bool {
	if dev.ReadOptimization == nil || dev.ReadOptimization.Enabled == nil {
		return true
	}
	return *dev.ReadOptimization.Enabled
}

func newReadBlock(item PlannedRegisterRead) ReadBlock {
	return ReadBlock{
		SlaveID:      item.Slave.SlaveID,
		FunctionCode: item.Register.FunctionCode,
		StartAddress: item.EffectiveAddress,
		WordCount:    item.Register.Words,
		PlannedReads: []PlannedRegisterRead{item},
	}
}

func canExtendBlock(block ReadBlock, item PlannedRegisterRead, maxWords int, maxGapWords int) bool {
	if block.SlaveID != item.Slave.SlaveID {
		return false
	}
	if block.FunctionCode != item.Register.FunctionCode {
		return false
	}
	if maxWords < 1 {
		maxWords = 1
	}
	if maxWords > maxModbusRegisterBlockWords {
		maxWords = maxModbusRegisterBlockWords
	}

	blockEnd := block.StartAddress + block.WordCount
	itemEnd := item.EffectiveAddress + item.Register.Words
	if item.EffectiveAddress > blockEnd+maxGapWords {
		return false
	}

	newWords := itemEnd - block.StartAddress
	return newWords <= maxWords
}
