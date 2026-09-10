# Coding Standards & Guidelines

This document outlines the minimal, essential coding standards for the Unified Streaming Lakehouse project. Adhering to these standards ensures code readability, reliability, and maintainability across all services (Simulator, Spark apps, Dashboards).

---

## 1. General Principles

* **Keep It Simple (KISS)**: Write straightforward code. Avoid over-engineering.
* **Don't Repeat Yourself (DRY)**: Abstract common logic into helper functions or modules, but avoid premature abstractions.
* **Readable > Clever**: Code is read much more often than it is written. Prioritize clarity over concise but cryptic code.

---

## 2. Python Standards

Since a significant portion of this stack (simulator, stream processors, APIs) is written in Python, follow these guidelines:

### Style and Formatting
* Use **PEP 8** style guidelines.
* **Formatting**: Standardize on `ruff` or `black` for auto-formatting.
* **Naming Conventions**:
  * Functions & Variables: `snake_case` (e.g., `calculate_metrics`)
  * Classes: `PascalCase` (e.g., `TelemetryConsumer`)
  * Constants: `UPPER_SNAKE_CASE` (e.g., `KAFKA_BOOTSTRAP_SERVERS`)

### Error Handling
* Avoid empty or generic `except Exception:` blocks unless explicitly logging and re-raising, or exiting. Always catch specific exceptions:
  ```python
  # Good
  try:
      connection.connect()
  except ConnectionRefusedError as e:
      logger.error(f"Failed to connect: {e}")
  ```

### Logging & Output
* Use Python's built-in `logging` module rather than raw `print()` statements for service logs.
* Ensure log messages are structured or contain relevant context (e.g., entity IDs, task names).
* Use a prefix to all logging that tells me where errors are occuring. For example "ERROR: Telemetry Service: ". 

---

## 3. Configuration & Secrets

* **No Hardcoded Configs**: Never hardcode connection strings, topic names, port numbers, or credentials.
* **Environment Variables**: Use environment variables to configure services dynamically (e.g., `os.environ.get("KAFKA_BOOTSTRAP_SERVERS")`). Use sensible defaults for local development.
* **Secrets**: Never commit passwords, API keys, or access tokens to the repository. Use environment files (`.env`) listed in `.gitignore` or secret managers.

---

## 4. Docker & Containerization

* **Use Specific Base Images**: Avoid using `latest` tags for base Docker images. Pin to stable, minimal versions (e.g., `python:3.11-slim` instead of `python:latest`).
* **Clean Layer Management**: Combine commands where possible (e.g., using `&&` in `RUN` instructions) and clean up package manager caches to minimize image size.
* **Environment Configuration**: Define default environment variables using `ENV` in Dockerfiles where appropriate.
