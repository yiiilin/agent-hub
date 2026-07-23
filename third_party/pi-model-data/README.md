# Pi Model Data Snapshot

`v0.81.1/` is the generated `packages/ai/src/providers/data/` directory used by
the pinned Pi `v0.81.1` submodule. Pi excludes this directory from its source
repository and otherwise requires `npm run hydrate:model-data`, which fetches
multiple external model catalogs during a build.

Agent Hub commits this small snapshot so `scripts/build-pi-standalone.sh` can
run Pi's `build:offline` deterministically after dependency installation.

- Pi commit: `20be4b18d4c57487f8993d2762bace129f0cf7c6`
- Snapshot tree SHA-256: `27928526a62db7d9f808b9efebe1d2529d782ace46c9e9dacc327c7dfb2a261e`
- Source command: `npm --prefix third_party/pi run hydrate:model-data`

When updating Pi, regenerate the model data from the new pinned source in an
auditable build, update this directory and checksum together, then review the
model catalog diff as part of that version update.
