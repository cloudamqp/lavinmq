Decide if this PR's code changes require updates to `docs/`. Publish the result as a check run.

Verdicts:
- `success`: no user-facing changes, OR docs are updated sufficiently
- `neutral`: user-facing changes exist, docs were touched but are incomplete
- `failure`: user-facing changes exist, no `docs/` files touched

Publish with:
`gh api -X POST /repos/$REPO/check-runs -f name="Docs check" -f head_sha=$HEAD_SHA -f status=completed -f conclusion=<verdict> -f "output[title]=Docs check" -f "output[summary]=<one sentence naming the specific flag, feature, or file>"`
