# modbus-agent

Agente de lectura Modbus con múltiples salidas de almacenamiento configurables por YAML (`storage.outputs`).

## Flujo de escritura

El agente:

1. Lee dispositivos/slaves/registros.
2. Genera samples (tags + fields).
3. Recorre `storage.outputs` y escribe en cada backend habilitado según `type`.

Esto permite enviar el mismo ciclo de lectura a varios destinos (por ejemplo, `influxdb2`, `timescaledb` y `timescaledb_shadow`) en paralelo lógico.

## Backends soportados

- `influxdb2`
- `timescaledb`
- `timescaledb_shadow`

## Comportamiento TimescaleDB

### Clave de fila (upsert)

La escritura usa:

- `INSERT ... ON CONFLICT (ts, device_name, slave_name) DO UPDATE`

Esto permite completar una misma fila por timestamp conforme llegan distintos registros del mismo equipo/slave.

### Registros que fallan lectura

Si un registro falla (read/decode), ese campo no se incluye en el sample.

- Si la fila aún no existe, esa columna queda `NULL`.
- Si la fila ya existe para esa PK, esa columna no se sobrescribe.

No se descarta toda la fila por un solo registro fallido.

### Columnas no existentes en tabla

Para soportar escenarios tipo PLC/Moxa donde se agregan sensores gradualmente:

- El writer consulta columnas existentes en `information_schema.columns`.
- Si llega un campo/tag cuya columna no existe en la tabla, se ignora.
- Se registra warning (una vez por columna) y se sigue escribiendo el resto de campos de la fila.

Resultado: agregar registros nuevos en YAML no rompe la ingesta mientras la migración SQL aún no se aplica.

### Orden de columnas

No depende del orden físico de columnas en PostgreSQL.

El writer siempre usa `INSERT INTO tabla (col1, col2, ...) VALUES (...)`, por lo que el mapeo es por nombre de columna.

### Campos/tags normalizados

- Tags requeridos para Timescale: `device_name`, `slave_name`, `slave_id`.
- Si existe `plant:` en la raíz del YAML, se propaga como tag `plant` (salvo override por tags más específicos).
- Compatibilidad legacy en tags: `device`, `slave` se mantienen como alias.
- `ip` se mapea a `ip_address`.
- `unit` se omite como columna SQL.
- Campos `raw_*` se omiten para Timescale.

## Comportamiento Timescale Shadow

El backend `timescaledb_shadow` escribe una fila por `plant + device_name + slave_name + ts` con:

- columnas fijas: `plant`, `ts`, `device_name`, `slave_name`, `series_key`
- `payload` JSONB con:
  - `slave_id`
  - `series_key`
  - `flags`
  - `fields` (campos no-raw normalizados)

La idea es usar esta tabla como transporte compacto del payload que luego se proyecta a la tabla estructurada.

## Configuración YAML (resumen)

Cada output en `storage.outputs` define:

- `name`
- `type`
- `enabled` (opcional, default `true`)

Cada `device` puede definir opcionalmente:

- `mode: tcp` (default)
- `mode: rtu_over_tcp`

`rtu_over_tcp` sirve para escenarios tipo NPort donde el agente abre un socket TCP y transmite frames Modbus RTU crudos con CRC hacia un bus serial remoto.

### Ejemplo `timescaledb`

```yaml
storage:
  outputs:
    - name: "local_timescale"
      type: "timescaledb"
      enabled: true
      timescaledb:
        host_env: "TIMESCALE_HOST_LOCAL"
        port_env: "TIMESCALE_PORT_LOCAL"
        user_env: "TIMESCALE_USER_LOCAL"
        password_env: "TIMESCALE_PASSWORD_LOCAL"
        database_env: "TIMESCALE_DB_LOCAL"
        schema: "lalcktur"
        table: "ktl_inverters"
```

### Ejemplo `timescaledb_shadow`

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

## Ejecución

```bash
go run ./cmd \
  --configPath /ruta/config.yml \
  --envPath /ruta/modbus-agent.env
```

Modo solo lectura (sin escribir outputs):

```bash
go run ./cmd \
  --configPath /ruta/config.yml \
  --envPath /ruta/modbus-agent.env \
  --interrogateOnly
```
