PYTHON ?= python3

.PHONY: test migrate smoke load ci

test:
	$(PYTHON) -m pytest -q

migrate:
	$(PYTHON) scripts/run_migrations.py

smoke:
	$(PYTHON) scripts/e2e_smoke.py

load:
	$(PYTHON) scripts/load_test.py --duration 10 --rps 2

ci:
	./scripts/ci_verify.sh
