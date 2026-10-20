#!/usr/bin/env bash
# =====================================================================================
#  Ride-Hailing Surge Analytics - start everything with one command:   ./start.sh
#  Safe to run again at any time (after a reboot, after editing .env, after a failure).
#  The first run builds 4 images and downloads one month of NYC trips: about 30-45 minutes.
# =====================================================================================
set -uo pipefail
cd "$(dirname "$0")"
DC="docker compose --env-file .env -f docker/docker-compose.yml"
say()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m! %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31mx %s\033[0m\n' "$*"; exit 1; }
set_env() { if grep -q "^$1=" .env; then sed -i "s|^$1=.*|$1=$2|" .env; else echo "$1=$2" >> .env; fi; }

say "1/4 Checks"
docker info >/dev/null 2>&1 || die "Docker is not running (Docker Desktop on Windows, or: sudo systemctl start docker)"
docker compose version >/dev/null 2>&1 || die "Docker Compose v2 is missing"
[ -f .env ] || { cp .env.example .env; warn ".env created - fill in the AWS / Databricks / Groq keys later (optional)"; }
set_env DOCKER_GID "$(stat -c %g /var/run/docker.sock)"       # lets Airflow use the docker socket
# Docker Desktop on WSL sometimes sets a credential helper that cannot run from Ubuntu -> every image pull fails.
CFG="$HOME/.docker/config.json"
if [ -f "$CFG" ] && grep -q '"credsStore"' "$CFG" && ! command -v "docker-credential-$(sed -n 's/.*"credsStore": *"\([^"]*\)".*/\1/p' "$CFG")" >/dev/null 2>&1; then
  cp "$CFG" "$CFG.bak"; sed -i '/"credsStore"/d' "$CFG"; warn "removed a broken Docker credential helper from $CFG (backup: $CFG.bak)"
fi
# a host port that is already in use would stop a container from starting -> take the next free one
for pair in MYSQL_HOST_PORT:3307 AIRFLOW_PORT:8080 NAMENODE_UI_PORT:9870 HBASE_UI_PORT:16010 GENAI_PORT:8501 MAILHOG_UI_PORT:8026; do
  var=${pair%%:*}; port=$(grep "^$var=" .env | cut -d= -f2); port=${port:-${pair##*:}}
  docker ps --format '{{.Ports}}' | grep -q ":${port}->" && continue                 # already ours
  p=$port; while (exec 3<>/dev/tcp/127.0.0.1/"$p") 2>/dev/null; do p=$((p+1)); done
  [ "$p" != "$port" ] && warn "port $port is busy -> using $p for $var"; set_env "$var" "$p"
done

say "2/4 Building images (only the first time, 10-20 min)"
export COMPOSE_BAKE=false BUILDKIT_PROGRESS=plain
for svc in tools spark sqoop airflow-init; do
  $DC build "$svc" || die "image build failed: $svc (check the internet connection and run ./start.sh again)"
done

say "3/4 Starting the stack (first run: downloading NYC data + filling MySQL, 15-25 min - watch: docker logs -f rh-seed)"
# After a Windows/WSL restart Docker can start containers before the project folder is mounted (empty /app).
if docker ps --format '{{.Names}}' | grep -q '^rh-tools$' && ! docker exec rh-tools test -f /app/common/config.py; then
  warn "containers see an empty project folder (WSL restarted) - recreating them, data is kept"; $DC down
fi
$DC up -d || die "some services did not start - see: docker ps -a ; docker logs <container>"

say "4/4 Done"
# On its first start Airflow runs each DAG once by itself (latest schedule); later the schedules take over.
# Run a DAG again by hand with: make batch | make hourly | make payout (only when the previous run has finished).

port() { grep "^$1=" .env | cut -d= -f2; }
cat <<TXT

  Ops page (live data + assistant)  http://localhost:$(port GENAI_PORT)
  Airflow                           http://localhost:$(port AIRFLOW_PORT)    admin / admin
  MailHog (payout e-mails)          http://localhost:$(port MAILHOG_UI_PORT)
  HDFS / HBase web UI               http://localhost:$(port NAMENODE_UI_PORT)  /  http://localhost:$(port HBASE_UI_PORT)

  Every command:  make help          Status:  make status          Stop:  make down
TXT
