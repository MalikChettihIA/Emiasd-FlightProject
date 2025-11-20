# Repository Guidelines

## Project Structure & Module Organization
Sources live in `src/main/scala/com/flightdelay`: `app` contains `FlightDelayPredictionApp`, `data` handles ingestion, `features` prepares joins/PCA/balancing, `ml` manages training and evaluation, and `utils` centralizes helpers. Experiment YAMLs and logback files sit in `src/main/resources`, documentation in `docs/MD`, Docker assets in `docker/`, and produced jars plus unmanaged MLflow libs in `work/apps`. Root scripts (`start.sh`, `stop.sh`, `submit.sh`) wrap the cluster lifecycle.

## Build, Test, and Development Commands
- `sbt compile` — Compile and resolve dependencies.
- `sbt test` — Run ScalaTest/Mockito suites; required pre-PR.
- `sbt package` — Emit `work/apps/Emiasd-Flight-Data-Analysis.jar`; add `clean` after dependency changes.
- `./start.sh` / `./stop.sh` — Spin the Docker Spark + MLflow stack up or down via docker-compose.
- `./submit.sh` — Clean, rebuild, and execute `/scripts/spark-submit.sh` in the `spark-submit` container.
- `cd docker && ./setup.sh` — Initial image and shared-volume bootstrap.

## Coding Style & Naming Conventions
Use Scala 2.12 with two-space indentation, same-line braces, camelCase members, and PascalCase types. Keep package paths under `com.flightdelay.<domain>` and prefer explicit grouped imports (e.g., `import com.flightdelay.config.{AppConfiguration, ExperimentConfig}`). Default to immutable vals, descriptive logging via `DebugUtils`, and colocated configuration constants; verify formatting with `sbt compile` before commits.

## Testing Guidelines
Place specs in `src/test/scala/com/flightdelay` and name them `*Spec.scala` (e.g., `MLPipelineSpec`). ScalaTest matchers should verify schemas, metrics, and side effects, while Mockito isolates Spark or IO collaborators. Create deterministic Spark fixtures with `SparkSession.builder.master("local[*]").getOrCreate()`, set tight shuffle partitions, and run `sbt test` (or `sbt testOnly com.flightdelay.ml.MLPipelineSpec`) before pushing.

## Commit & Pull Request Guidelines
History favors imperative subjects such as “Update experiment configs and model hyperparameters”; keep them under ~72 characters and add explanatory bodies when configs or data move. Every PR must summarize scope, list touched configs/scripts, link issues, and capture MLflow run IDs or screenshots when behavior changes. Record the verification commands executed (`sbt test`, `./submit.sh`, `./start.sh`) in the PR template.

## Configuration & Environment Tips
Manage every toggle through the YAML files in `src/main/resources` (`local-config.yml`, `prod-config.yml`, etc.) and enable experiments via the `experiments` array. Ensure `configuration.common.output.basePath` points to a writable directory under `work` so Spark checkpoints and parquet exports succeed. Drop extra MLflow jars inside `work/apps` for unmanaged resolution, mount raw datasets through Docker volumes, and inject secrets via environment variables rather than committing them.
