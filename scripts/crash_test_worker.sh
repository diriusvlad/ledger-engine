#!/usr/bin/env bash
# Crash test #2 from the project brief: kill the webhook worker for real
# (SIGKILL, no cleanup) after it has already received a 200 from the
# webhook consumer but before it marks the outbox event 'sent', then
# restart it and prove the event gets redelivered and the consumer's
# dedupe catches it.
#
# Requires: a container runtime (docker by default; set
# CONTAINER_RUNTIME=podman to use podman instead — both are SIGKILL under
# the hood, so either demonstrates the same thing) and a Postgres instance
# already reachable at DB_URL (see README "Running everything").
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

RUNTIME="${CONTAINER_RUNTIME:-docker}"
DB_URL="${DATABASE_URL:-postgres://ledger:ledger@localhost:5433/ledger?sslmode=disable}"
WEBHOOK_SECRET="crash-test-secret"
CONSUMER_PORT="${CONSUMER_PORT:-9091}"
CRASH_DELAY="6s"
TAG_PREFIX="ledger-crashtest"

psql_query() {
	if command -v psql >/dev/null 2>&1; then
		psql "$DB_URL" -tAc "$1"
	else
		"$RUNTIME" exec ledger_postgres psql -U ledger -d ledger -tAc "$1"
	fi
}

cleanup() {
	"$RUNTIME" rm -f ledger_crashtest_worker ledger_crashtest_consumer >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> Building worker, webhookconsumer and seedcharge images (runtime: ${RUNTIME})"
"$RUNTIME" build -q -t "${TAG_PREFIX}-worker" --build-arg BIN=worker -f Dockerfile . >/dev/null
"$RUNTIME" build -q -t "${TAG_PREFIX}-webhookconsumer" --build-arg BIN=webhookconsumer -f Dockerfile . >/dev/null
"$RUNTIME" build -q -t "${TAG_PREFIX}-seedcharge" --build-arg BIN=seedcharge -f Dockerfile . >/dev/null

cleanup

echo "==> Resetting ledger tables so this run's event is the only thing in the outbox"
psql_query "TRUNCATE entries, transactions, accounts, idempotency_keys, outbox_events RESTART IDENTITY CASCADE;" >/dev/null

echo "==> Starting webhook consumer on :${CONSUMER_PORT}"
"$RUNTIME" run -d --name ledger_crashtest_consumer --network=host \
	-e LISTEN_ADDR=":${CONSUMER_PORT}" -e WEBHOOK_SECRET="${WEBHOOK_SECRET}" \
	"${TAG_PREFIX}-webhookconsumer" >/dev/null
sleep 1

echo "==> Starting worker with an injected post-send delay of ${CRASH_DELAY} (the crash window)"
"$RUNTIME" run -d --name ledger_crashtest_worker --network=host \
	-e DATABASE_URL="${DB_URL}" \
	-e WEBHOOK_URL="http://localhost:${CONSUMER_PORT}/webhook" \
	-e WEBHOOK_SECRET="${WEBHOOK_SECRET}" \
	-e WORKER_POLL_INTERVAL=200ms \
	-e WORKER_TEST_CRASH_DELAY_AFTER_SEND="${CRASH_DELAY}" \
	"${TAG_PREFIX}-worker" >/dev/null

echo "==> Seeding one charge (through the real ledger.Service, producing one outbox event)"
SEED_JSON=$("$RUNTIME" run --rm --network=host -e DATABASE_URL="${DB_URL}" "${TAG_PREFIX}-seedcharge")
EVENT_ID=$(echo "$SEED_JSON" | sed -n 's/.*"event_id":"\([^"]*\)".*/\1/p')
if [ -z "$EVENT_ID" ]; then
	echo "FAIL: could not seed a charge/outbox event: $SEED_JSON"
	exit 1
fi
echo "    event_id=${EVENT_ID}"

echo "==> Waiting for the consumer to receive the first delivery..."
COUNT=0
for _ in $(seq 1 50); do
	COUNT=$(curl -s "http://localhost:${CONSUMER_PORT}/_received" | grep -o "\"id\":\"${EVENT_ID}\"" | wc -l)
	[ "$COUNT" -ge 1 ] && break
	sleep 0.2
done
if [ "$COUNT" -lt 1 ]; then
	echo "FAIL: consumer never received the first delivery"
	exit 1
fi
echo "    confirmed: consumer received the delivery (worker is now in its post-send sleep)"

STATUS=$(psql_query "SELECT status FROM outbox_events WHERE id='${EVENT_ID}'")
echo "==> outbox_events.status = '${STATUS}' (expected: pending — mark-sent hasn't run yet)"
if [ "$STATUS" != "pending" ]; then
	echo "FAIL: expected status=pending before the kill, got '${STATUS}'"
	exit 1
fi

echo "==> SIGKILLing the worker mid-sleep (real kill -9, no cleanup, no cheating)"
"$RUNTIME" kill -s SIGKILL ledger_crashtest_worker >/dev/null

STATUS=$(psql_query "SELECT status FROM outbox_events WHERE id='${EVENT_ID}'")
SENT_AT=$(psql_query "SELECT COALESCE(sent_at::text, '') FROM outbox_events WHERE id='${EVENT_ID}'")
echo "==> after kill: status='${STATUS}' sent_at='${SENT_AT}'"
if [ "$STATUS" != "pending" ] || [ -n "$SENT_AT" ]; then
	echo "FAIL: expected pending/no sent_at after the kill (mark-sent must never have run)"
	exit 1
fi
echo "    confirmed: the POST succeeded but mark-sent never happened — exactly the crash we're testing"

echo "==> Restarting the worker (fresh container, no crash delay this time)"
"$RUNTIME" rm -f ledger_crashtest_worker >/dev/null 2>&1 || true
"$RUNTIME" run -d --name ledger_crashtest_worker --network=host \
	-e DATABASE_URL="${DB_URL}" \
	-e WEBHOOK_URL="http://localhost:${CONSUMER_PORT}/webhook" \
	-e WEBHOOK_SECRET="${WEBHOOK_SECRET}" \
	-e WORKER_POLL_INTERVAL=200ms \
	"${TAG_PREFIX}-worker" >/dev/null

echo "==> Waiting for redelivery of the same event..."
COUNT=0
for _ in $(seq 1 50); do
	COUNT=$(curl -s "http://localhost:${CONSUMER_PORT}/_received" | grep -o "\"id\":\"${EVENT_ID}\"" | wc -l)
	[ "$COUNT" -ge 2 ] && break
	sleep 0.2
done
if [ "$COUNT" -lt 2 ]; then
	echo "FAIL: event was never redelivered after the worker restarted"
	exit 1
fi

DUP=$(curl -s "http://localhost:${CONSUMER_PORT}/_received" | grep -c "\"duplicate\":true")
if [ "$DUP" -lt 1 ]; then
	echo "FAIL: redelivered event was not flagged as a duplicate by the consumer's dedupe"
	exit 1
fi
echo "    confirmed: event redelivered and flagged 'duplicate: true' by the consumer"

echo "==> Waiting for the DB row to finally reach 'sent'..."
for _ in $(seq 1 50); do
	STATUS=$(psql_query "SELECT status FROM outbox_events WHERE id='${EVENT_ID}'")
	[ "$STATUS" = "sent" ] && break
	sleep 0.2
done
if [ "$STATUS" != "sent" ]; then
	echo "FAIL: outbox row never reached 'sent' after the restart"
	exit 1
fi

echo
echo "PASS — crash test #2:"
echo "  1. worker POSTed the webhook successfully"
echo "  2. worker was SIGKILLed before it could mark the event 'sent'"
echo "  3. on restart, the still-'pending' event was redelivered"
echo "  4. the consumer's event-ID dedupe correctly flagged the redelivery as a duplicate"
