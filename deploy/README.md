# Running the engine

One Docker Compose stack, identical on every machine (ADR 0018). The image
carries the engine plus `tippecanoe` and `pmtiles`; the stack is a Prefect
server (state, UI) and a runner (executes the flows).

## Quickstart

```bash
cd deploy
cp .env.example .env          # fill in the R2 credentials and the webhook
docker compose up -d --build
```

The UI is at <http://127.0.0.1:4200> — localhost only, by design. On a remote
machine, tunnel it:

```bash
ssh -L 4200:127.0.0.1:4200 <host>
```

## Running a collection

Collections are deliberate campaigns: deployments carry no schedule, and a run
is triggered by hand —

```bash
docker compose exec runner prefect deployment run 'collect/ja'
```

— or from the UI (Deployments → collect/ja → Run). Progress, state and logs
live in the UI; the outcome lands on the Discord webhook if one is configured.

Provider traffic can only leave from the runner container: it is the one place
`ENGINE_ALLOW_OUTBOUND=1` is set. A `streetradar collect` in a host shell
refuses to make a request, which is the point.

## Moving to another machine

The stack's whole state is three things: `.env`, the `prefect-home` volume
(run history, deployments) and the `streetradar-data` volume (parquet parts,
resume snapshots). To move — say, to a VPS once collections outgrow this
machine:

```bash
# old machine: stop cleanly (a running shard finishes its batch and exits)
docker compose down

# copy .env and both volumes' contents to the new machine, then
docker compose up -d --build
```

Published tilesets are not part of the move: they live on R2.

## Updating

Locally, rebuild in place: `docker compose up -d --build`. The tag-and-pull
path (git tag → CI → GHCR `:stable` → a timer runs `compose pull && up -d`,
ADR 0010) is for the machine that runs unattended; the Ansible playbook sets
that timer up. Updating mid-collection is safe: the shard finishes its batch,
commits, and the next run resumes from durable state.
