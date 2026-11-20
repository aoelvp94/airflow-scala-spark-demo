# Reviewer demo for the Java SDK setXCom encoding fix (Airflow 3.4.0).
# See README.md for the full walk-through.

IMAGE ?= airflow-scala-docs-e2e:3.4.0
# AIRFLOW_SRC must point at an apache/airflow checkout with the fix applied.
# The scripts also auto-detect a few local worktrees if you leave it unset.

.PHONY: image bundle overlay baseline patched patched-scala all evidence clean sdk-check

all: image bundle overlay baseline patched patched-scala evidence

image:
	docker build -t $(IMAGE) -f Dockerfile .

bundle:
	./scripts/build-bundle.sh

overlay: bundle
	./scripts/build-overlay-sdk.sh

baseline: image bundle
	./scripts/run-airflow.sh baseline

patched: image overlay
	./scripts/run-airflow.sh patched

patched-scala: image overlay
	./scripts/run-airflow.sh patched-scala

evidence:
	python3 scripts/collect.py --write-evidence-md --demo-root .

# Unit tests for the fix live in the Airflow Java SDK, not in this demo.
# From a checkout with the setXCom patch applied:
#   cd $$AIRFLOW_SRC/java-sdk && ./gradlew :sdk:check
sdk-check:
	@if [ -z "$$AIRFLOW_SRC" ]; then echo "Set AIRFLOW_SRC to the airflow checkout"; exit 1; fi
	cd "$$AIRFLOW_SRC/java-sdk" && ./gradlew :sdk:check

clean:
	-docker rm -f airflow-reviewer-demo-baseline airflow-reviewer-demo-patched airflow-reviewer-demo-patched-scala >/dev/null 2>&1
	rm -rf bundle-baseline bundle-baseline-scala bundle-patched bundle-patched-scala build scala-src/target evidence/logs-*
	rm -f evidence/*-run-id.txt evidence/*-runs.json evidence/*-states.json evidence/*-xcom.json evidence/*-errors.json evidence/*-airflow.db evidence/EVIDENCE.md
