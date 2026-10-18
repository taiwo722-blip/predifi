# Local Soroban wallet E2E

This test runs against a private Stellar Quickstart ledger in `--local` mode.
It uses the repository's `stellar` Docker Compose service with an E2E-only
override that gives it a separate project and volume. RPC is at
`http://localhost:8000/rpc`, local Friendbot at
`http://localhost:8000/friendbot`, and the local passphrase
`Standalone Network ; February 2017`. It never uses public testnet.

The override sets Quickstart's `--limits unlimited` for this private fixture.
The current PrediFi core contract builds to about 204,000 WASM bytes, above
the documented 128 KiB public-network contract limit. This local E2E checks
wallet flow behavior; it does not prove the current contract can be deployed
under public-network resource limits.

## Run from a clean checkout

On Linux or macOS, install Node.js 22, Docker Engine with Compose v2, Rust
with the `wasm32v1-none` target, and Stellar CLI 28.1.0. Docker must be
running and port 8000 must be free. The dedicated GitHub Actions job installs
these tools on an Ubuntu runner. Then run from the repository root:

```bash
npm ci --prefix e2e/wallet --ignore-scripts --no-audit
npm --prefix e2e/wallet run test:local
```

The test command starts only the isolated `stellar` service, waits for RPC
health, and verifies the local network passphrase. It builds the two project
contracts, deploys them with Stellar CLI, deploys the native asset contract if
needed, initializes access control and PrediFi, whitelists XLM, and creates a
fresh two-outcome pool. It generates a temporary admin CLI identity and an
in-memory prediction wallet, then funds both through **local** Friendbot.
The admin configuration is deleted after the test. No private key is committed
or printed. Repeated runs create fresh accounts and contracts; the E2E Compose
volume can remain in place. Port 8000 must be free, so stop any other local
Stellar service before starting the test.

The scripted flow checks:

1. A rejected signer callback prevents submission and leaves pool stakes at zero.
2. A valid transaction is built, prepared through RPC simulation, signed with
   an ephemeral key, submitted, confirmed as `SUCCESS` in a ledger, and shown
   to increase outcome 0 stake by 10,000,000 stroops. The test prints the
   confirmed transaction hash and ledger number.
3. An invalid outcome (`2` for a two-outcome pool) produces contract error
   `InvalidOutcome` (code 25) during real RPC simulation, with no submission
   or state change.
4. An injected RPC response that never confirms produces a bounded timeout
   error. This last check is a mocked confirmation boundary, not a ledger test.

The test calls a signer callback representing wallet approval or rejection.
It does **not** drive the Freighter browser extension. The current frontend has
wallet connection handling but no prediction transaction builder or Freighter
`signTransaction` adapter to invoke from this test.

## Run without local Docker

Push an `issue-127-*` branch to your fork, then open **Actions → Soroban Wallet
E2E** in that fork to inspect the push-triggered run. GitHub requires a
workflow file on the fork's default branch before its manual **Run workflow**
button works; a new workflow on an issue branch can run on `push` without that
button. After the workflow reaches your fork's default branch, you can also
select the issue branch with **Run workflow**. The hosted Ubuntu runner starts
the same Compose service and invokes the same
`npm --prefix e2e/wallet run test:local` command. The workflow also runs on
pull requests to `main` that touch the contract, Compose fixture, E2E test, or
workflow. No local Docker installation is needed to use the hosted run.

The Actions test step shows the four scenario results and the confirmed
transaction hash. A passing job is the on-ledger proof; syntax checks on a
machine without Docker are not. The job summary reports pass or fail. If the
run fails, inspect the first failing step and the scenario in its test output.
For an RPC startup failure, check `docker compose ps stellar` in the final
diagnostic step. For build failures, check the Stellar CLI and Rust versions
printed by the tooling step. The job stops the local service afterward.
