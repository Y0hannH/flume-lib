# flume-lib documentation

Technical documentation for [flume-lib](../README.md). Start from the document that matches what you are doing.

| Document | Audience | Content |
|---|---|---|
| [agent-cookbook.md](agent-cookbook.md) | AI agents | Everything needed to write a Fabric notebook with the library: rules, public API, decision procedure from an API's documentation to a config, validated recipes, error catalogue, generated key reference. |
| [cookbook.md](cookbook.md) | People writing configs | "The vendor's API does X → write Y" tables for auth, pagination and refresh strategy; failure triage; watermark operations; a generated index of every key. |
| [configuration.md](configuration.md) | Reference | Every option of every block, with defaults, stop conditions and the reasoning behind each behaviour. |
| [install.md](install.md) | Operators | Requirements, offline wheel install, GitHub install, OneLake specifics. |
| [security.md](security.md) | Security reviewers | Threat model, supply chain, secrets, network, resource limits, how to report a vulnerability. |
| [development.md](development.md) | Contributors | Checks to run, what CI does, release procedure. |
| [`../examples/`](../examples/) | Everyone | Complete Fabric notebooks: every auth type, every pagination strategy, every write mode, date windows and merge, limits and proxy, the `run_source` parameters and the functions around it. |
| [`../CHANGELOG.md`](../CHANGELOG.md) | Everyone | Version history, breaking changes first. |

`agent-cookbook.md` and the key index of `cookbook.md` are checked by CI against the code: a config in the agent cookbook that stopped validating, or a key that drifted, fails the build. So are the examples: every config they build validates, and every configuration key and public function appears in at least one of them.
