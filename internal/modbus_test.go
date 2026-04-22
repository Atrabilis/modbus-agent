package internal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRegistersParsesRootPlant(t *testing.T) {
	t.Parallel()

	yaml := `plant: lalcktur
devices:
  - device:
      name: "logo8"
      ip: "127.0.0.1"
      port: 502
      slaves: []
`

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatalf("write temp yaml: %v", err)
	}

	var cfg Devices
	if err := LoadRegisters(path, &cfg); err != nil {
		t.Fatalf("LoadRegisters failed: %v", err)
	}

	if cfg.Plant != "lalcktur" {
		t.Fatalf("expected plant lalcktur, got %q", cfg.Plant)
	}
}

func TestMergeTagsAddsPlantWhenMissing(t *testing.T) {
	t.Parallel()

	dev := &Device{Name: "dev1"}
	slave := &Slave{Name: "slave1", SlaveID: 3}
	reg := &Register{Name: "power"}

	tags := MergeTags("petorca", dev, slave, reg)

	if got := tags["plant"]; got != "petorca" {
		t.Fatalf("expected plant tag petorca, got %q", got)
	}
}

func TestMergeTagsDoesNotOverrideSpecificPlant(t *testing.T) {
	t.Parallel()

	dev := &Device{
		Name: "dev1",
		Tags: map[string]string{"plant": "override_plant"},
	}
	slave := &Slave{Name: "slave1", SlaveID: 3}
	reg := &Register{Name: "power"}

	tags := MergeTags("petorca", dev, slave, reg)

	if got := tags["plant"]; got != "override_plant" {
		t.Fatalf("expected plant override_plant, got %q", got)
	}
}
