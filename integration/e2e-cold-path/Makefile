# Cold-path e2e shorthand. See README.md for full docs.
#
# Targets:
#   test       — fixture-only run (no docker, no sweep coordination)
#   test-live  — live run (gated on COLD_E2E_LIVE=1; skips if sweep alive)
#   test-force — live run, ignore sweep busy check (use sparingly)
#   vet        — go vet
#   down       — tear down a stuck e2e compose stack

.PHONY: test test-live test-force vet down

test:
	go test -v ./...

test-live:
	COLD_E2E_LIVE=1 go test -v -run TestColdPathEnd2End ./...

test-force:
	COLD_E2E_LIVE=1 COLD_E2E_FORCE=1 go test -v -run TestColdPathEnd2End ./...

vet:
	go vet ./...

down:
	docker compose -p asap-cold-e2e \
		-f docker-compose/e2e-overlay.yml down -v
