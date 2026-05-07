# modbus-agent

Modbus polling agent with YAML-configured storage outputs.

## Overview

The agent performs the following steps for each run:

1. Load device, slave, register, and storage configuration from YAML.
2. Poll devices over Modbus.
3. Decode register values into samples.
4. Write samples to each enabled storage output unless `--interrogateOnly` is used.

The storage configuration is independent from the polling configuration. A single polling cycle can be written to multiple outputs.

## Supported storage backends

- `influxdb2`
- `timescaledb`
- `timescaledb_shadow`

## Polling model

The YAML configuration is logical. It defines devices, slaves, and registers as user-facing series definitions.

The polling engine may read registers in one of two ways:

- individual register reads
- block reads, where contiguous registers are grouped into a single Modbus request and then split back into the original logical registers

Block reads do not change:

- `device_name`
- `slave_name`
- tags
- field names
- storage output format

They only change how register bytes are acquired.

## Transport modes

Each device may define:

- `mode: tcp`
- `mode: rtu_over_tcp`

If `mode` is omitted, the default is `tcp`.

### `tcp`

Standard Modbus TCP.

### `rtu_over_tcp`

This mode is intended for endpoints such as NPort-style serial gateways. The agent opens a TCP socket and sends raw Modbus RTU frames with CRC over that socket.

This mode does not use local serial parameters such as baud rate or parity. Those parameters must already be configured on the remote gateway.

## Healthcheck

Each device may define an optional `healthcheck` block. Healthchecks run before normal register polling.

Example:

```yaml
healthcheck:
  enabled: true
  on_fail: "skip_device"
  success_policy: "any"
  probes:
    - function_code: 4
      register: 5036
      words: 1
      offset: 1
      timeout_ms: 500
```

### Supported fields

- `enabled`
  - optional
  - if the block exists and `enabled` is omitted, the healthcheck is considered enabled
- `on_fail`
  - `skip_device` (default)
  - `fail_run`
- `success_policy`
  - `any` (default)
  - `all`
- `probes`

Each probe supports:

- `slave_id`
  - optional
  - if omitted, the effective `slave_id` of the current slave is used
- `function_code`
- `register`
- `words`
- `offset`
- `timeout_ms`
- `retries`

### `skip_healthcheck` per slave

Each slave may define:

```yaml
skip_healthcheck: true
```

This is useful when multiple logical slaves share the same physical endpoint and a repeated probe is unnecessary.

## Read optimization

Each device may define an optional `read_optimization` block:

```yaml
read_optimization:
  enabled: true
  max_block_words: 125
  max_gap_words: 0
```

### Supported fields

- `enabled`
  - `true` enables block planning
  - `false` forces individual register reads
- `max_block_words`
  - maximum number of words in a planned block read
- `max_gap_words`
  - maximum allowed gap between registers when merging them into the same block
  - `0` means only contiguous ranges are merged

### Defaults

If `read_optimization` is omitted, the current defaults are:

- for `tcp`
  - `enabled: true`
  - `max_block_words: 32`
  - `max_gap_words: 0`
- for `rtu_over_tcp`
  - `enabled: true`
  - `max_block_words: 8`
  - `max_gap_words: 0`

### Protocol limits

For `function_code 3` and `function_code 4`, the Modbus protocol limits a single request to 125 registers. If a larger `max_block_words` value is configured, the planner clamps it to 125.

The current implementation does not perform shared block planning for `function_code 1` coils.

## YAML structure

The polling configuration has this high-level structure:

```yaml
plant: example_plant
devices:
  - device:
      name: example_device
      ip: 192.168.1.10
      port: 502
      mode: tcp
      healthcheck:
        enabled: true
        probes:
          - function_code: 4
            register: 5036
            words: 1
            offset: 1
            timeout_ms: 500
      read_optimization:
        enabled: true
        max_block_words: 32
        max_gap_words: 0
      slaves:
        - name: inverter_1
          slave_id: 1
          offset: 1
          modbus_registers:
            - register: 4950
              function_code: 4
              name: protocol_num
              description: Protocol number
              words: 2
              datatype: U32LE
              unit: ""
              gain: 1
              flags:
```

## Example: RTU over TCP

```yaml
plant: petorca
devices:
  - device:
      name: tracker
      ip: 192.168.1.23
      port: 4001
      mode: rtu_over_tcp
      healthcheck:
        enabled: true
        on_fail: "skip_device"
        success_policy: "any"
        probes:
          - function_code: 3
            register: 151
            words: 1
            offset: 0
            timeout_ms: 500
      read_optimization:
        enabled: true
        max_block_words: 125
        max_gap_words: 0
      slaves:
        - name: ncu_1
          slave_id: 1
          offset: 0
          modbus_registers:
            - register: 0
              function_code: 3
              name: tcu_num
              description: Number of active TCU devices reported by the NCU
              words: 1
              datatype: U8
              unit: ""
              gain: 1
              flags:
        - name: tcu_2
          slave_id: 1
          offset: -1
          skip_healthcheck: true
          modbus_registers:
            - register: 151
              function_code: 3
              name: actual_angle_deg
              description: Tracker actual angle
              words: 1
              datatype: S16
              unit: deg
              gain: 0.1
              flags:
```

## Storage behavior

### TimescaleDB

The TimescaleDB writer uses:

- `INSERT ... ON CONFLICT (ts, device_name, slave_name) DO UPDATE`

This means multiple fields for the same timestamp and logical series can be merged into a single row.

If a register read or decode fails:

- that field is not included in the sample
- if the row does not exist yet, the corresponding column remains `NULL`
- if the row already exists, the column is not overwritten

The writer does not discard the full row because of a single failed field.

If a field or tag does not have a matching column in the target table:

- the column is ignored
- a warning is emitted once per missing column
- the remaining columns are still written

For Timescale-oriented outputs:

- required tags are `device_name`, `slave_name`, and `slave_id`
- top-level `plant` is propagated as a `plant` tag unless overridden
- legacy aliases `device` and `slave` are still preserved
- `ip` is normalized to `ip_address`
- `unit` is not written as a SQL column
- `raw_*` fields are omitted

Column ordering in PostgreSQL is not relevant. Inserts are generated by column name.

### Timescale shadow

The `timescaledb_shadow` backend writes one row per:

- `plant`
- `device_name`
- `slave_name`
- `ts`

The row contains:

- fixed columns: `plant`, `ts`, `device_name`, `slave_name`, `series_key`
- a `payload` JSONB document with:
  - `slave_id`
  - `series_key`
  - `flags`
  - normalized non-raw fields

### Storage output example

```yaml
storage:
  outputs:
    - name: "local_shadow"
      type: "timescaledb_shadow"
      enabled: true
      timescaledb_shadow:
        host_env: "TIMESCALE_HOST_LOCAL"
        port_env: "TIMESCALE_PORT_LOCAL"
        user_env: "TIMESCALE_USER_LOCAL"
        password_env: "TIMESCALE_PASSWORD_LOCAL"
        database_env: "TIMESCALE_DB_LOCAL"
        schema: "landing"
        table: "diris_i35_shadow"
```

## Execution

Normal run:

```bash
go run ./cmd \
  --configPath /path/to/config.yml \
  --envPath /path/to/modbus-agent.env
```

Read-only run:

```bash
go run ./cmd \
  --configPath /path/to/config.yml \
  --envPath /path/to/modbus-agent.env \
  --interrogateOnly
```

When `--interrogateOnly` is used, the agent polls and decodes values but does not write to any configured storage output.

## End-of-run counters

At the end of each run, the agent prints:

- `Total Modbus requests`
- `Failed/timeout Modbus requests`
- `Time taken`

`Total Modbus requests` counts physical Modbus requests, including:

- healthcheck probes
- individual register reads
- block reads

It does not count logical fields or emitted samples.
