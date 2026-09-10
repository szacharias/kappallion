# Clean Coder Guidelines & Architecture Rules

> *"Clean code always looks like it was written by someone who cares."*  
> — Robert C. Martin (Uncle Bob), *Clean Code* & *The Clean Coder*

This document defines the craftsmanship, architectural abstraction, and coding standards for this repository. All assistants, developers, and subagents must adhere strictly to these rules when creating, modifying, or refactoring code.

---

## 1. Professionalism & The Boy Scout Rule
* **Leave the campground cleaner than you found it**: Whenever touching a file or module, eliminate dead code, extract tangled logic, fix typos, and improve naming.
* **Self-Documenting Code**: Code must express intent clearly through naming and structure. Comments should explain *why* non-obvious business decisions exist, never *what* the code is doing.

---

## 2. Architectural Boundaries & Separation of Concerns
Strictly isolate code into distinct conceptual layers:

1. **Domain Layer (`domain/`, `models/`)**:
   * Pure Python data classes and business rules.
   * Zero external dependencies on frameworks, databases, or I/O.
   * Examples: Vehicle thermodynamic models, cold-chain temperature thresholds, Dew Point calculators, anomaly state transitions.
2. **Service / Infrastructure Layer (`services/`, `infrastructure/`)**:
   * Concrete implementations of I/O, messaging, and data storage.
   * Examples: Kafka producer/consumer clients, OpenStreetMap route fetchers, Delta Lake readers/writers.
3. **Presentation Layer (`views/`, `ui/`)**:
   * User-facing visualization and interfaces.
   * Must contain **zero business logic**; delegates directly to domain models and services.
   * Examples: Streamlit dashboard layouts, Pydeck map layers, Plotly figures.

---

## 3. Function & Class Design Standards

### A. Functions: Do One Thing (SLAP)
* **Single Level of Abstraction Principle (SLAP)**: A function should do exactly one thing, at a single level of abstraction.
* **Length**: Keep functions small (typically under 25 lines).
* **Argument Arity**: Prefer 0 to 2 arguments. If a function requires $\ge 3$ arguments, group them into a typed `dataclass`.
* **Command-Query Separation (CQS)**: A function should either perform an action (command) or return data (query), never both unexpectedly.
* **Pure Functions**: Where possible, write functions that are deterministic and free of side effects.

### B. Naming Conventions
* **Intention-Revealing Names**: Names must answer why it exists, what it does, and how it is used (e.g., `calculate_dew_point()` instead of `calc_dp()`).
* **Pronounceable & Searchable**: Avoid single-letter variables except for standard loop indices (`i`, `j`).
* **Classes**: Nouns or noun phrases in PascalCase (`TruckSimulator`, `RouteManager`).
* **Functions & Methods**: Verbs or verb phrases in snake_case (`load_routes()`, `is_condensation_risk()`).

### C. Eliminate Magic Numbers & Strings
* **No Inline Literals**: Magic numbers (e.g., `41.8875`, `3.5`, `0.005`, `25.0`) must be defined as named module-level constants (e.g., `BASE_WAREHOUSE_LAT`, `DEFAULT_TARGET_TEMP_C`) or Enum classes (`VehicleState`, `DoorStatus`).

---

## 4. Type Annotations & Data Contracts
* **Strict Type Hints**: Use Python's `typing` module (`Optional`, `List`, `Dict`, `Tuple`, `Callable`, `Union`) on all function signatures, parameters, and return types.
* **Typed Data Contracts**: Replace raw, untyped nested dictionaries with typed `@dataclass` or Pydantic models with clear fields and validation.

```python
# PREFERRED
@dataclass(frozen=True)
class Coordinate:
    latitude: float
    longitude: float

# AVOID
coord = {"lat": 41.88, "lon": -87.65}
```

---

## 5. Error Handling & Resilience
* **Prefer Exceptions over Error Codes**: Raise domain-specific exceptions rather than returning `None`, `-1`, or error strings.
* **Specific Exception Handling**: Never use bare `except:`. Catch explicit exceptions (`urllib.error.URLError`, `json.JSONDecodeError`, `KafkaError`).
* **Graceful Degradation**: When optional external services fail (e.g., OSRM routing API), fail back to predictable local fallbacks and log the cause cleanly.

---

## 6. Testing as a First-Class Citizen (The Clean Coder Mandate)
Every new feature, refactored component, or mathematical calculation must be accompanied by unit tests:

* **FIRST Principles**:
  * **Fast**: Tests must execute in milliseconds.
  * **Independent**: Tests must not depend on external state or the execution order of other tests.
  * **Repeatable**: Tests must produce identical results across environments without flaky network/timing dependencies.
  * **Self-Validating**: Tests have binary pass/fail outcomes (`assert`).
  * **Timely**: Tests are written concurrently with or prior to production code.
* **AAA Pattern (Arrange, Act, Assert)**:
  * Each test method must clearly separate setup, execution, and verification.
* **Mock External I/O**:
  * Unit tests must never hit live Kafka brokers, remote S3/MinIO endpoints, or external web APIs. Use `unittest.mock` or test fixtures.
