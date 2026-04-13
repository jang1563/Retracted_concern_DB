# Handoff: Open-Data Collection And Finalize Chain

Date: 2026-04-13

## Summary

This project is currently running an `open-data-only` collection pipeline on Cayuga for:

- OpenAlex works
- Retraction Watch
- PubMed baseline + updatefiles
- Crossref is intentionally skipped in this mode

The active run root is:

`/athena/masonlab/scratch/users/jak4013/lsib/20260410-overnight-rerun1`

The current active jobs are:

- Public collection: `2788472`
- Open-data finalize dependency: `2788473`

At the latest confirmed check:

- `2788472 lsib-public-vendor`: `RUNNING`
- `2788473 lsib-open-data-finalize`: `PENDING (Dependency)`

This run has progressed farther than previous failed attempts.

## Most Important Current Status

The run is no longer failing at the earlier PubMed and Python-version issues.

Confirmed progress signals:

- `current_step=collect_open_data_freeze`
- `runtime_manifest=yes`
- `normalized_files=711`
- `release_files=0`
- `site_files=0`
- `raw_files=9`

This means the active job has already passed:

- vendor collection
- raw snapshot staging
- local raw snapshot readiness
- `register-snapshot`

And it is now producing normalized ingest artifacts inside the runtime root.

## Key Paths

Remote run root:

`/athena/masonlab/scratch/users/jak4013/lsib/20260410-overnight-rerun1`

Important subpaths:

- Repo: `/athena/masonlab/scratch/users/jak4013/lsib/20260410-overnight-rerun1/repo`
- Public collection log dir: `/athena/masonlab/scratch/users/jak4013/lsib/20260410-overnight-rerun1/logs`
- Public collection artifacts: `/athena/masonlab/scratch/users/jak4013/lsib/20260410-overnight-rerun1/artifacts/public_vendor_collection`
- Public collection runtime: `/athena/masonlab/scratch/users/jak4013/lsib/20260410-overnight-rerun1/artifacts/public_vendor_collection_work`
- Raw snapshot: `/athena/masonlab/scratch/users/jak4013/lsib/20260410-overnight-rerun1/raw/public_open_data_snapshot`
- Vendor archive: `/athena/masonlab/scratch/users/jak4013/lsib/20260410-overnight-rerun1/vendor_archive`
- Finalize runtime: `/athena/masonlab/scratch/users/jak4013/lsib/20260410-overnight-rerun1/artifacts/open_data_runtime`

## Important Fixes Already Applied

### 1. PubMed listing parser fixed

File:

- `scripts/raw_snapshot/collect_pubmed_baseline_updatefiles.sh`

What changed:

- The old listing parser incorrectly combined a heredoc with piped HTML, so the parser never received the directory HTML.
- The script now writes listing HTML to a temp file and parses it from disk.

Evidence that this is fixed:

- `baseline_xml_gz=1334`
- `update_xml_gz=77`

So PubMed is now downloading real `*.xml.gz` files.

### 2. Wrong Python interpreter in local runtime pipeline fixed

File:

- `scripts/cayuga/run_local_real_snapshot_pipeline.sh`

What changed:

- The script previously hardcoded `python3`, which picked up system Python 3.6 on Cayuga.
- It now uses `PYTHON_BIN`, matching the Conda/miniconda runtime configured elsewhere.

This fixed the previous error:

- `ModuleNotFoundError: No module named 'dataclasses'`

### 3. Space-saving mode already enabled

Relevant files:

- `scripts/raw_snapshot/collect_open_data_freeze.sh`
- `scripts/cayuga/templates/public_vendor_collection.sbatch.in`

Current behavior:

- Staging uses `symlink` mode by default for open-data collection
- This avoids duplicating the full vendor archive into raw snapshot

### 4. Retraction Watch collection reduced

File:

- `scripts/raw_snapshot/collect_retraction_watch_csv.sh`

What changed:

- Uses shallow clone/fetch
- Keeps CSV and manifest outputs
- Removes the repo clone after extracting the needed file

## Latest Confirmed Metrics

These were confirmed on the active run:

- `vendor_archive`: at least `652G`
- `raw/public_open_data_snapshot`: `16M`
- Raw snapshot bucket counts:
  - `openalex=4`
  - `official_notices=3`
  - `pubmed=2`
- Runtime artifact counts:
  - `normalized_files=711`
  - `release_files=0`
  - `site_files=0`

Interpretation:

- The active job is definitely inside the ingest/runtime phase.
- It has not yet finished materialization/build-core/build-report.

## Main Commands For Next Session

### 1. Check current queue state

```bash
ssh -o ConnectTimeout=10 cayuga-login1 \
  '/opt/ohpc/pub/software/slurm/25.05.0/bin/squeue -j 2788472,2788473 -o "%i %j %T %M %R"'
```

### 2. Check final Slurm state if jobs have left the queue

```bash
ssh -o ConnectTimeout=10 cayuga-login1 '
  /opt/ohpc/pub/software/slurm/25.05.0/bin/sacct \
    -j 2788472,2788473 \
    --format=JobID,JobName,State,Elapsed,ExitCode,Reason | sed -n "1,20p"
'
```

### 3. Check current step markers

```bash
ssh -o ConnectTimeout=10 cayuga-login1 '
  RUN_ROOT=/athena/masonlab/scratch/users/jak4013/lsib/20260410-overnight-rerun1
  test -f "$RUN_ROOT/artifacts/public_vendor_collection/current_step.txt" && \
    { echo -n "public_current_step="; cat "$RUN_ROOT/artifacts/public_vendor_collection/current_step.txt"; }
  test -f "$RUN_ROOT/artifacts/public_vendor_collection/failed_step.txt" && \
    { echo -n "public_failed_step="; cat "$RUN_ROOT/artifacts/public_vendor_collection/failed_step.txt"; }
  test -f "$RUN_ROOT/artifacts/open_data_runtime/current_step.txt" && \
    { echo -n "finalize_current_step="; cat "$RUN_ROOT/artifacts/open_data_runtime/current_step.txt"; }
  test -f "$RUN_ROOT/artifacts/open_data_runtime/failed_step.txt" && \
    { echo -n "finalize_failed_step="; cat "$RUN_ROOT/artifacts/open_data_runtime/failed_step.txt"; }
'
```

### 4. Check runtime progress

```bash
ssh -o ConnectTimeout=10 cayuga-login1 '
  RUN_ROOT=/athena/masonlab/scratch/users/jak4013/lsib/20260410-overnight-rerun1
  echo -n "runtime_manifest="
  test -f "$RUN_ROOT/artifacts/public_vendor_collection_work/runtime_root/data/manifests/ingest.sqlite3" && echo yes || echo no
  echo -n "normalized_files="
  find "$RUN_ROOT/artifacts/public_vendor_collection_work/runtime_root/data/normalized" -type f 2>/dev/null | wc -l
  echo -n "release_files="
  find "$RUN_ROOT/artifacts/public_vendor_collection_work/release" -type f 2>/dev/null | wc -l
  echo -n "site_files="
  find "$RUN_ROOT/artifacts/public_vendor_collection_work/site" -type f 2>/dev/null | wc -l
'
```

### 5. Check raw snapshot and PubMed file counts

```bash
ssh -o ConnectTimeout=10 cayuga-login1 '
  RUN_ROOT=/athena/masonlab/scratch/users/jak4013/lsib/20260410-overnight-rerun1
  for bucket in openalex official_notices pubmed; do
    dir="$RUN_ROOT/raw/public_open_data_snapshot/$bucket"
    if [ -d "$dir" ]; then
      printf "%s=" "$bucket"
      find "$dir" -maxdepth 1 \( -type f -o -type l \) | wc -l
    fi
  done
  echo -n "baseline_xml_gz="
  find "$RUN_ROOT/vendor_archive/pubmed/baseline/2026" -maxdepth 1 -name "*.xml.gz" | wc -l
  echo -n "update_xml_gz="
  find "$RUN_ROOT/vendor_archive/pubmed/updatefiles/2026-03" -maxdepth 1 -name "*.xml.gz" | wc -l
'
```

### 6. Check public collection log tail

```bash
ssh -o ConnectTimeout=10 cayuga-login1 '
  RUN_ROOT=/athena/masonlab/scratch/users/jak4013/lsib/20260410-overnight-rerun1
  tail -n 80 "$RUN_ROOT/logs/public-vendor-2788472.out"
'
```

## If The Current Run Succeeds

The expected next state is:

- `2788472` leaves queue with `COMPLETED`
- `2788473` starts automatically
- `release_files` rises above zero
- `site_files` rises above zero

Then check:

```bash
ssh -o ConnectTimeout=10 cayuga-login1 '
  RUN_ROOT=/athena/masonlab/scratch/users/jak4013/lsib/20260410-overnight-rerun1
  find "$RUN_ROOT/artifacts/public_vendor_collection_work/release" -type f | sed -n "1,80p"
  find "$RUN_ROOT/artifacts/public_vendor_collection_work/site" -type f | sed -n "1,80p"
'
```

If finalize completes, inspect:

- `artifacts/open_data_runtime`
- `artifacts/open_data_release`
- `artifacts/open_data_site`

and then package the results into a cleaner release summary.

## If The Current Run Fails

The first places to inspect are:

1. Slurm status
2. `public_failed_step.txt`
3. `tail -n 80 logs/public-vendor-2788472.out`

The previous failure classes were:

- PubMed HTML listing parse bug
- wrong Python path inside `run_local_real_snapshot_pipeline.sh`

Both of those are already fixed in the current codebase, so if the next failure happens it is likely to be a different downstream ingest/materialization issue.

## Re-Submit Workflow If Needed

Only do this if `2788472` has actually failed.

1. Sync latest repo:

```bash
./scripts/cayuga/sync_repo_to_cayuga.sh \
  cayuga-login1 \
  /athena/masonlab/scratch/users/jak4013/lsib/20260410-overnight-rerun1
```

2. Submit public collection:

```bash
ssh -o ConnectTimeout=10 cayuga-login1 '
  RUN_ROOT=/athena/masonlab/scratch/users/jak4013/lsib/20260410-overnight-rerun1
  cd "$RUN_ROOT/repo"
  ./scripts/cayuga/submit_public_vendor_collection.sh "$RUN_ROOT"
'
```

3. Submit finalize with dependency:

```bash
ssh -o ConnectTimeout=10 cayuga-login1 '
  RUN_ROOT=/athena/masonlab/scratch/users/jak4013/lsib/20260410-overnight-rerun1
  cd "$RUN_ROOT/repo"
  bash ./scripts/cayuga/submit_open_data_finalize.sh "$RUN_ROOT" afterany:<new_public_jobid>
'
```

## Local Files Most Relevant To Continue

- `scripts/raw_snapshot/collect_pubmed_baseline_updatefiles.sh`
- `scripts/raw_snapshot/collect_open_data_freeze.sh`
- `scripts/raw_snapshot/collect_monthly_freeze.sh`
- `scripts/raw_snapshot/run_vendor_archive_pipeline.sh`
- `scripts/cayuga/run_local_real_snapshot_pipeline.sh`
- `scripts/cayuga/submit_public_vendor_collection.sh`
- `scripts/cayuga/submit_open_data_finalize.sh`
- `scripts/cayuga/templates/public_vendor_collection.sbatch.in`
- `scripts/cayuga/templates/open_data_finalize_template.sbatch.in`

## Practical Interpretation

The current run looks promising.

It is not stuck at the old failure points anymore.
It has progressed into runtime normalization, and the main question for the next session is:

- does `2788472` finish the ingest/build steps cleanly,
- or does it fail later in the downstream pipeline?

That is the main continuation point.
