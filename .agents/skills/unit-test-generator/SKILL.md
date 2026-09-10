---
name: unit-test-generator
description: >-
  Generates comprehensive, isolated pytest unit tests for newly created or refactored
  Python modules, domain models, and service interfaces based on Uncle Bob's Clean Code principles.
---

# Clean Code Unit Test Generator

This skill guides the creation of clean, robust, and isolated unit tests following *Clean Code* / *The Clean Coder* standards.

## When to Use
Use this skill whenever:
* A new class, function, or domain model is created.
* Existing logic is refactored (e.g. moving logic out of monolithic scripts into modules).
* Boundary calculations (thermodynamics, dew point, route navigation) need regression prevention.

## Testing Guidelines (Uncle Bob's Standards)

### 1. The AAA Pattern (Arrange, Act, Assert)
Every test method must clearly separate:
```python
def test_cargo_cooling_decay_in_door_open_state():
    # Arrange: set up target object and parameters
    truck = TruckSimulator(
        vehicle_id="TRK-TEST-0001",
        origin="Base Warehouse",
        destination="Hub-Aurora",
        start_lat=41.88,
        start_lon=-87.65,
        dest_lat=41.76,
        dest_lon=-88.32
    )
    truck.state = "DOOR_OPEN"
    truck.ambient_temp = 30.0
    truck.cargo_temp = 4.0

    # Act: execute state update
    truck.update_telemetry()

    # Assert: temperature increased toward ambient
    assert truck.cargo_temp > 4.0
    assert truck.door_status == "OPEN"
```

### 2. FIRST Principles
* **Fast**: Run in milliseconds.
* **Independent**: No global state leakage between tests.
* **Repeatable**: Deterministic; mock `random` or use fixed seeds when asserting probabilistic ranges.
* **Self-Validating**: Clear boolean assertions.
* **Timely**: Written alongside code changes.

### 3. Mock External I/O
* Use `unittest.mock.patch` for:
  - `KafkaProducer`
  - `urllib.request.urlopen`
  - File reading (`builtins.open`) when testing missing/corrupt configs
  - `SparkSession`
