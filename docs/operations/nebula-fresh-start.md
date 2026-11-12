# Nebula fresh start after GitHub CI

This is a **full production reset**, not candle backfill. It removes every active
user, login session, balance, position, order, trade and candle, plus Redis streams
and engine snapshots. It starts a new history; future empty minutes are still
possible when no trades occur. Backups and archived historical exports are kept.
The frontend is on Vercel and is not wiped.

Do not execute until the application changes have been committed/pushed and all
six GitHub Docker CI jobs have succeeded. The release must include the
`20261112093000_realtime_candles` migration. Use that full commit SHA below. Local
Docker images are for testing only. This runbook has not been executed against
production.

Run the blocks in order in a **temporary Bash session**, rather than directly
in your login shell. Run this command **by itself** first and wait for the new
prompt:

```bash
bash --noprofile --norc
```

Then run the numbered blocks below. Their `set -euo pipefail` setting stops the
reset on a failed check; it exits only the temporary Bash session and returns
to your original terminal. Replace the `CI_SHA` placeholder before running step
1. A placeholder, wrong context, or missing image intentionally stops preflight.
Kubernetes commands use `command kubectl -n perps-app` explicitly to bypass
shell aliases and functions and keep the namespace visible. Stop on any failed command;
inspect the failed Job rather than continuing or blindly restarting the reset.
After step 3 the app is offline until the final startup. Keep the backup job
names and the saved CronJob configuration for recovery.

## 1. Select the release and check the cluster

```bash
set -euo pipefail
cd /Users/ashutosh/projects/ops
export CI_SHA='REPLACE_WITH_THE_FULL_40_CHARACTER_SUCCESSFUL_CI_COMMIT'
[[ "$CI_SHA" =~ ^[0-9a-f]{40}$ ]]
[[ "$(command kubectl config current-context)" == 'gke_project-4d058f50-15a8-46ac-aae_asia-south1-a_k8s-cluster' ]]
export RESET_ID="$(date -u +%Y%m%d%H%M%S)"
export RESET_DIR="$(mktemp -d /tmp/nebula-reset.XXXXXX)"
command kubectl -n perps-app get cronjobs -o json > "$RESET_DIR/cronjobs.json"
command kubectl -n perps-app get deployments,statefulsets,pvc
# Verify every image exists before stopping anything.
for service in api engine wss db-poller price-feeder market-maker; do
  docker buildx imagetools inspect "ashutoshsao/perps-app-${service}:${CI_SHA}" >/dev/null
done
```

## 2. Pin the ops source to the CI release

The feeder manifest must include the new port 8080 startup/readiness/liveness
probes before proceeding. These edits persist the release in the ops repo;
commit them after a successful reset. Do not apply the other manifests yet.

```bash
python3 - <<'PY'
import os, pathlib, re
root = pathlib.Path('perps-app-k8s')
files = {'api':'05-api.yml', 'engine':'04-engine.yml', 'wss':'07-wss.yml',
         'db-poller':'09-dbPoller.yml', 'price-feeder':'08-priceFeeder.yml',
         'market-maker':'10-marketMaker.yml'}
for service, filename in files.items():
    path = root / filename
    text, count = re.subn(r'image: ashutoshsao/perps-app-' + re.escape(service) + r':[^\s]+',
        'image: ashutoshsao/perps-app-' + service + ':' + os.environ['CI_SHA'], path.read_text())
    assert count == 1, (path, count)
    if service in ['api', 'wss']:
        text, replicas = re.subn(r'(?m)^  replicas: \d+$', '  replicas: 2', text, count=1)
        assert replicas == 1, (path, 'missing replicas field')
    path.write_text(text)
feeder = (root / files['price-feeder']).read_text()
assert all(key in feeder for key in ['startupProbe:', 'readinessProbe:', 'livenessProbe:', 'containerPort: 8080'])
PY
```

## 3. Stop writers and scheduled maintenance, then take backups

```bash
for job in postgres-backup redis-backup timescale-archive trade-history-archive; do
  command kubectl -n perps-app patch cronjob "$job" --type=merge -p '{"spec":{"suspend":true}}'
done
command kubectl -n perps-app scale deployment api engine wss db-poller price-feeder market-maker --replicas=0
if [[ -n "$(command kubectl -n perps-app get pods -l 'app in (api,engine,wss,db-poller,price-feeder,market-maker)' -o name)" ]]; then
  command kubectl -n perps-app wait --for=delete pod -l 'app in (api,engine,wss,db-poller,price-feeder,market-maker)' --timeout=180s
fi
# Let already-started scheduled jobs finish before resetting their storage.
for job in $(command kubectl -n perps-app get jobs -o json | python3 -c 'import json,sys; print(" ".join(x["metadata"]["name"] for x in json.load(sys.stdin)["items"] if x.get("status",{}).get("active",0)))'); do
  command kubectl -n perps-app wait --for=condition=complete "job/$job" --timeout=600s
done
command kubectl -n perps-app create job "pre-reset-postgres-${RESET_ID}" --from=cronjob/postgres-backup
command kubectl -n perps-app wait --for=condition=complete "job/pre-reset-postgres-${RESET_ID}" --timeout=600s
command kubectl -n perps-app create job "pre-reset-redis-${RESET_ID}" --from=cronjob/redis-backup
command kubectl -n perps-app wait --for=condition=complete "job/pre-reset-redis-${RESET_ID}" --timeout=600s
command kubectl -n perps-app logs "job/pre-reset-postgres-${RESET_ID}"
command kubectl -n perps-app logs "job/pre-reset-redis-${RESET_ID}"
# Confirm both logs show a successful upload to perps-db-backups before proceeding.
# These commands update scaled-down Deployments, without starting them.
for service in api engine wss db-poller price-feeder market-maker; do
  command kubectl -n perps-app set image "deployment/$service" "$service=ashutoshsao/perps-app-${service}:${CI_SHA}"
done
```

## 4. Define a one-off maintenance Job helper

It reuses only the selected Deployment's environment references and image. It
does not print secret values and refuses a Deployment that has not been stopped.

```bash
maintenance_job() {
  python3 - "$1" "$2" "$3" "$4" <<'PY'
import json, subprocess, sys
owner, name, directory, code = sys.argv[1:]
base = ['kubectl', '-n', 'perps-app']
deployment = json.loads(subprocess.check_output(base + ['get','deployment',owner,'-o','json']))
assert deployment['spec']['replicas'] == 0, 'Stop the Deployment first'
container = deployment['spec']['template']['spec']['containers'][0]
container = {key: container[key] for key in ['image','env','resources'] if key in container}
container.update(name='maintenance', workingDir=directory, command=['bun','-e',code])
container.setdefault('env', []).extend([
    {'name':'R2_BACKUP_BUCKET','valueFrom':{'configMapKeyRef':{'name':'app-config','key':'R2_BACKUP_BUCKET'}}}
])
job = {'apiVersion':'batch/v1','kind':'Job','metadata':{'name':name,'namespace':'perps-app'},
       'spec':{'backoffLimit':0,'activeDeadlineSeconds':600,
               'template':{'spec':{'restartPolicy':'Never','containers':[container]}}}}
subprocess.run(base + ['create','-f','-'], input=json.dumps(job).encode(), check=True)
PY
}
```

## 5. Archive and clear engine snapshots

Redis and PostgreSQL alone are insufficient: the engine would restore its old
markets, balances and positions from R2. This Job first copies every snapshot to
`perps-db-backups/engine-pre-reset/<job timestamp>/`, then clears the snapshot
bucket. It refuses unexpected bucket names or non-snapshot keys. If copying
fails, deletion never starts. Existing backup and archive buckets are not wiped.

```bash
maintenance_job engine "reset-snapshots-${RESET_ID}" /app/apps/engine '
import { S3Client, ListObjectsV2Command, CopyObjectCommand, DeleteObjectsCommand } from "@aws-sdk/client-s3";
const bucket = process.env.R2_BUCKET;
const backup = process.env.R2_BACKUP_BUCKET;
if (bucket !== "perps-engine-snapshots" || backup !== "perps-db-backups") throw new Error("Unexpected buckets");
const s3 = new S3Client({region:"auto", endpoint:`https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials:{accessKeyId:process.env.R2_ACCESS_KEY_ID,secretAccessKey:process.env.R2_SECRET_ACCESS_KEY}});
const keys = [];
let cursor;
do {
  const page = await s3.send(new ListObjectsV2Command({Bucket:bucket,ContinuationToken:cursor}));
  keys.push(...(page.Contents ?? []).map(x => x.Key));
  cursor = page.NextContinuationToken;
} while (cursor);
if (keys.some(key => !/^\d+-\d+\.json$/.test(key))) throw new Error("Unexpected snapshot key; inspect manually");
const prefix = `engine-pre-reset/${Date.now()}/`;
for (const key of keys) {
  await s3.send(new CopyObjectCommand({Bucket:backup,Key:prefix+key,CopySource:`${bucket}/${encodeURIComponent(key)}`}));
}
for (let i=0; i<keys.length; i+=1000) {
  const result = await s3.send(new DeleteObjectsCommand({Bucket:bucket,Delete:{Objects:keys.slice(i,i+1000).map(Key=>({Key}))}}));
  if (result.Errors?.length) throw new Error(JSON.stringify(result.Errors));
}
const remaining = await s3.send(new ListObjectsV2Command({Bucket:bucket,MaxKeys:1}));
if (remaining.Contents?.length) throw new Error("Snapshot bucket not empty");
console.log(`Archived and removed ${keys.length} engine snapshots; archive prefix ${prefix}`);
'
command kubectl -n perps-app wait --for=condition=complete "job/reset-snapshots-${RESET_ID}" --timeout=600s
command kubectl -n perps-app logs "job/reset-snapshots-${RESET_ID}"
```

## 6. Replace only Nebula's two data volumes and migrate

**This is the destructive storage step.** It deletes the two named PVCs in
`perps-app`; do not delete the namespace or PVCs from any other application.

```bash
command kubectl -n perps-app scale statefulset redis timescaledb --replicas=0
command kubectl -n perps-app wait --for=delete pod/redis-0 pod/timescaledb-0 --timeout=180s
command kubectl -n perps-app delete pvc redis-data-redis-0 timescaledb-data-timescaledb-0 --wait=true
command kubectl -n perps-app scale statefulset redis timescaledb --replicas=1
command kubectl -n perps-app rollout status statefulset/redis --timeout=600s
command kubectl -n perps-app rollout status statefulset/timescaledb --timeout=600s
maintenance_job api "reset-migrate-${RESET_ID}" /app/packages/db '
const migrate = Bun.spawn(["bunx","prisma","migrate","deploy"], {stdout:"inherit",stderr:"inherit"});
if (await migrate.exited !== 0) process.exit(1);
const seed = Bun.spawn(["bun","run","seed"], {stdout:"inherit",stderr:"inherit"});
process.exit(await seed.exited);
'
command kubectl -n perps-app wait --for=condition=complete "job/reset-migrate-${RESET_ID}" --timeout=600s
command kubectl -n perps-app logs "job/reset-migrate-${RESET_ID}"
```

## 7. Start consumers, then the feeder and market maker

```bash
command kubectl -n perps-app scale deployment engine db-poller --replicas=1
command kubectl -n perps-app scale deployment api wss --replicas=2
for service in engine api wss db-poller; do
  command kubectl -n perps-app rollout status "deployment/$service" --timeout=180s
done
# This applies the CI image plus new probes. Recreate keeps one feeder writer.
command kubectl -n perps-app apply -f perps-app-k8s/08-priceFeeder.yml
command kubectl -n perps-app scale deployment market-maker --replicas=1
command kubectl -n perps-app rollout status deployment/market-maker --timeout=180s
command kubectl -n perps-app rollout status deployment/price-feeder --timeout=180s
command kubectl -n perps-app logs deployment/price-feeder --tail=30
curl --fail --silent --show-error https://api.nebula.ashutoshsao.com/health
curl --fail --silent --show-error https://api.nebula.ashutoshsao.com/health/workers
```

Expect `BTC/ETH/SOL using backpack index` in feeder logs and all worker checks
healthy. Confirm new trades and candles. The real-time candle migration includes fresh
fills at every interval without waiting for scheduled materialization; a candle
still requires actual fills and db-poller ingestion. These checks may need a short
wait after bootstrap:

```bash
curl --fail --silent --show-error 'https://api.nebula.ashutoshsao.com/api/trades/BTC-PERP'
curl --fail --silent --show-error 'https://api.nebula.ashutoshsao.com/api/klines/BTC-PERP?interval=1m&limit=10'
```

## 8. Verify the reset and restore scheduled jobs

```bash
command kubectl -n perps-app get pods,pvc,cronjob
command kubectl -n perps-app exec timescaledb-0 -- psql -U postgres -d perp -c 'SELECT migration_name, finished_at FROM "_prisma_migrations" WHERE rolled_back_at IS NULL ORDER BY migration_name;'
command kubectl -n perps-app exec timescaledb-0 -- psql -U postgres -d perp -c 'SELECT username FROM "User" ORDER BY username;'
command kubectl -n perps-app exec timescaledb-0 -- psql -U postgres -d perp -c 'SELECT view_name, materialized_only FROM timescaledb_information.continuous_aggregates ORDER BY view_name;'
command kubectl -n perps-app logs deployment/market-maker --tail=30
command kubectl -n perps-app logs deployment/price-feeder --tail=30
command kubectl -n perps-app exec redis-0 -- sh -c 'export REDISCLI_AUTH="$REDIS_PASSWORD"; exec redis-cli --no-auth-warning CONFIG GET maxmemory maxmemory-policy'
```

Expect the real-time candle migration finished, `materialized_only = f` for all
six candle views, only `demo` plus the three bots as users, two ready API and two
ready WSS replicas, and one replica of each other application. Completed backup
and migration Job pods can remain in `Completed`; they need not be `Running`.
Feeder logs should say `using backpack index`, and Redis should report `209715200`
(200 MiB) and `noeviction`. Finally, place a test order on the site and confirm
the chart updates.

Restore each CronJob's original suspension setting:

Only do this once application checks are healthy.

```bash
python3 - <<'PY'
import json, os, subprocess
for job in json.load(open(os.path.join(os.environ['RESET_DIR'],'cronjobs.json')))['items']:
    name = job['metadata']['name']
    if name not in ['postgres-backup','redis-backup','timescale-archive','trade-history-archive']:
        continue
    patch = {'spec': {'suspend': job['spec'].get('suspend', False)}}
    subprocess.run(['kubectl','-n','perps-app','patch','cronjob',name,'--type=merge','-p',json.dumps(patch)], check=True)
PY
git diff -- perps-app-k8s
```

Commit the manifest changes in the ops repo. Keep pre-reset backups if the fresh
start is wrong: rollback requires restoring PostgreSQL, Redis, and the matching
engine snapshots together, not just rolling the image back.
