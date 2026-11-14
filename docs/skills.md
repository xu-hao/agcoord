# Native installation of the agcoord Agent Skill

The `skills/agcoord/SKILL.md` Agent Skill gives supported coding hosts concise guidance for
resource claims, portable overflow checks, and guarded pull request landing.

## First-party plugin installation

| Host | Register/install | Explicit invocation |
| --- | --- | --- |
| Codex | `codex plugin marketplace add xu-hao/agcoord`, then `codex plugin add agcoord@agcoord` | `$agcoord` or `/skills` |
| Claude Code | `claude plugin marketplace add xu-hao/agcoord`, then `claude plugin install agcoord@agcoord` | `/agcoord:agcoord` |
| Antigravity (`agy`) | `agy plugin install https://github.com/xu-hao/agcoord` | `/agcoord` |

For a local checkout, substitute `/path/to/agcoord` for `xu-hao/agcoord`. The root
`plugin.json` and `.agents/plugins/marketplace.json` support Codex;
`.claude-plugin/plugin.json` and `.claude-plugin/marketplace.json` support Claude Code;
`.antigravity-plugin/plugin.json` supports Antigravity.

## Persistent agent instructions

After installing the skill, add this trigger to the repository's `AGENTS.md` or
`CLAUDE.md` so agents use it for relevant work:

```text
When running checks or landing pull requests, follow the agcoord skill: declare resources
with agc run, keep checks portable for overflow, and use one agc land request for gate and
publication. Never run agc inside an admitted job or merge a pull request directly.
```

## Contributor validation

Validate the native manifests without installing plugins globally or making live agent calls:

```sh
agc run --no-overflow --label "native plugin validation" --resource cpu=1 -- \
  .venv/bin/python scripts/check-plugins.py
```
