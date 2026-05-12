# XAgent

XAgent is a physical-execution agent runtime built around a closed-loop tool-calling cycle. It is designed for tasks that must touch the real environment: reading and editing files, running code, inspecting browser state, asking the user for clarification, and maintaining task memory.

The project is intentionally not a generic chat wrapper. Its core design goal is: observe first, act with minimal side effects, surface failures clearly, and keep looping until the task reaches a concrete terminal state.

## Highlights

- **Tool-call loop with explicit control flow**: every tool returns an `ActionResult` with data, next prompt, exit intent, and loop flags.
- **Multiple model protocols**: text-based tool protocol plus native OpenAI/Claude tool-calling clients.
- **Session-managed history**: model history, trimming, stream parsing, and failover stay inside the LLM layer.
- **Physical tools**: file read/write/patch, Python and shell execution, browser scan/JavaScript execution, user interruption, short-term checkpoints, long-term memory settlement, local skill activation, and plan tracking.
- **Streaming frontends**: CLI, Gradio UI, and a modern FastAPI + React UI with SSE updates.
- **Observability hooks**: structured event sinks, JSONL logging, and optional Langfuse integration.
- **Local-first workspace model**: relative paths resolve under an agent workspace, while project assets and memory stay under the repository.

## Project Status

XAgent is an early-stage research and engineering project. It has a real architecture, tests, and runnable frontends, but the public API and configuration format may still evolve. Treat it as experimental software, especially because it can execute code and modify files.

## Architecture

```text
┌──────────────────────────────────┐
│ Frontends                        │
│ CLI / Gradio / FastAPI + React   │
├──────────────────────────────────┤
│ Agent Core                       │
│ XAgent → agent_loop → handler    │
├──────────────────────────────────┤
│ LLM Layer                        │
│ Session / ToolClient / SSE       │
├──────────────────────────────────┤
│ Tool Layer                       │
│ code_run / file_* / web_* / ...  │
├──────────────────────────────────┤
│ Infrastructure                   │
│ BrowserDriver / memory / SOP     │
└──────────────────────────────────┘
```

The main runtime path is:

```text
user task
  → XAgent.run_task()
  → run_agent_loop()
  → client.chat(messages, tools)
  → handler.dispatch(tool_call)
  → tool returns ActionResult
  → next prompt or terminal exit
```

Design details live in [`docs/`](docs/):

- [`docs/architecture.md`](docs/architecture.md): system layers, module boundaries, workspace model
- [`docs/agent-loop.md`](docs/agent-loop.md): `ActionResult`, `AgentContext`, handler dispatch, turn-end hooks
- [`docs/llm-layer.md`](docs/llm-layer.md): sessions, protocol adapters, history trimming, failover
- [`docs/tool-layer.md`](docs/tool-layer.md): tool isolation, truncation, browser contract, dual history alignment
- [`docs/observability.md`](docs/observability.md): event model and telemetry sinks
- [`docs/outlines.md`](docs/outlines.md): high-level technical outline

## Repository Layout

```text
XAgent/
├── src/
│   ├── core/                 # agent loop, LLM clients, telemetry, skills
│   ├── handler/              # XAgentHandler tool dispatch methods
│   ├── tools/                # stateless tool implementations
│   ├── assets/               # system prompt, tool schema, code-run header
│   ├── main.py               # CLI entrypoint
│   ├── web_ui.py             # Gradio frontend
│   └── web_ui_new.py         # FastAPI backend for React frontend
├── frontends/web/            # React + Vite UI
├── docs/                     # design documents
├── memory/                   # persistent memory and SOP files
├── reflect/                  # reflection utilities
├── skills/                   # local prompt skill packs
├── tests/                    # unittest-based coverage
├── pyproject.toml
└── uv.lock
```

Runtime output is expected under `workspace/`, `temp/`, and logs. These are ignored by Git.

## Requirements

- Python `>=3.12,<3.13`
- [`uv`](https://docs.astral.sh/uv/) for Python environment management
- Node.js and npm for the React frontend
- Chrome or Chromium if you use Selenium-backed browser tools
- An OpenAI-compatible or Claude-compatible model endpoint

## Installation

Create the Python environment:

```bash
uv sync
```

Install optional web/browser dependencies:

```bash
uv sync --extra web
```

Install the React frontend dependencies:

```bash
cd frontends/web
npm install
```

## Configuration

The simplest setup uses environment variables:

```bash
export OPENAI_API_KEY="..."
export OPENAI_BASE_URL="https://api.openai.com/v1/chat/completions"
export OPENAI_MODEL="gpt-4o"
```

`OPENAI_BASE_URL` defaults to `https://api.openai.com/v1/chat/completions`, and `OPENAI_MODEL` defaults to `gpt-4o`.

You can also provide a JSON config file with one or more named sessions. The loader supports OpenAI text, Claude text, OpenAI native tools, Claude native tools, and mixin failover sessions. A minimal shape looks like:

```json
{
  "openai_main": {
    "apikey": "sk-...",
    "apibase": "https://api.openai.com/v1/chat/completions",
    "model": "gpt-4o",
    "temperature": 0.2,
    "max_tokens": 4096,
    "timeout": 120,
    "max_retries": 2
  }
}
```

Local configuration and secret files are ignored by default, including `config.json`, `mykey*`, `observability*.json`, `.env*`, and `*.secret.json`. Keep shareable examples under names such as `config.example.json` or `observability.example.json`.

## Running

### CLI

```bash
.venv/bin/python -m src.main
```

Useful CLI commands:

```text
/help
/session.temperature=0.5
/session.max_tokens=8192
/verbose on
/stop
/exit
```

### FastAPI + React UI

For a single backend-served UI:

```bash
cd frontends/web
npm run build
cd ../..
.venv/bin/python -m src.web_ui_new --host 127.0.0.1 --port 7861
```

Open:

```text
http://127.0.0.1:7861/
```

For frontend development with Vite:

```bash
.venv/bin/python -m src.web_ui_new --host 127.0.0.1 --port 7861
cd frontends/web
VITE_API_BASE=http://127.0.0.1:7861 npm run dev -- --host 127.0.0.1 --port 5173
```

### Gradio UI

```bash
.venv/bin/python -m src.web_ui --host 127.0.0.1 --port 7860
```

## Tools

XAgent exposes these tool domains to the model:

| Domain | Tools |
| --- | --- |
| Code execution | `code_run` |
| File operations | `file_read`, `file_write`, `file_patch` |
| Browser operations | `web_scan`, `web_execute_js` |
| User interaction | `ask_user` |
| Memory and planning | `update_working_checkpoint`, `start_long_term_update`, `plan_update` |
| Prompt skills | `skill_activate` |

Tool behavior is declared in [`src/assets/tools_schema.json`](src/assets/tools_schema.json). Tool implementations should remain stateless where possible; handler state belongs in `AgentContext`.

## Workspace and Safety Model

By default, XAgent creates and uses `<project_root>/workspace` as the physical workspace. Relative file paths resolve under that workspace. This limits accidental repository or system-wide edits.

Important safety constraints:

- File operations validate workspace boundaries.
- `file_patch` requires exactly one match for `old_content`.
- Code execution runs in a subprocess with timeout handling.
- Browser execution uses an isolated driver abstraction.
- Tool outputs are truncated before they re-enter the model context.
- Long-running tasks can be interrupted by `/stop`, UI stop buttons, or runtime stop signals.

XAgent can still execute shell code and write files. Review configuration, prompts, and workspace contents before running it against sensitive systems.

## Observability

Event sinks can be enabled through environment variables or an observability config file.

```bash
export XAGENT_LOG_DIR=logs
export XAGENT_LOG_STDERR=1
```

Optional Langfuse integration is available through the `observability` extra and `observability.example.json`.

```bash
uv sync --extra observability
.venv/bin/python -m src.main --observability-config observability.example.json
```

## Skills

Local skills are prompt instruction packs. They do not register new executable tools and do not run code by themselves.

The default skill root is [`skills/`](skills/). Each skill should include:

```text
SKILL.md
_meta.json
```

You can pass a custom skill directory:

```bash
.venv/bin/python -m src.main --skills-dir skills
```

The agent can auto-select relevant skills based on the task, or activate them explicitly with the `skill_activate` tool.

## Testing

Run the Python test suite:

```bash
.venv/bin/python -m unittest discover -s tests
```

Run frontend checks:

```bash
cd frontends/web
npm run build
npm run lint
```

The project currently uses `unittest` for Python tests. If you add pytest-only tests, add pytest to the development dependencies first.

## Development Guidelines

- Read the relevant document in [`docs/`](docs/) before changing architecture, loop behavior, LLM protocol code, or tool behavior.
- Keep diffs small and focused.
- Preserve module boundaries:
  - `core/agent_loop.py` should not depend on concrete tools.
  - `core/llm.py` owns protocol translation and history management.
  - `tools/` functions should stay stateless.
  - `handler/` owns stateful orchestration through `AgentContext`.
- Add tests for changes that affect loop exits, history, tool contracts, path safety, streaming, or frontend event delivery.

## Roadmap Ideas

- First-class config examples for OpenAI, Claude, and failover setups
- A stricter permission model for high-risk tools
- More complete frontend testing
- CI for Python and frontend checks
- Packaged release workflows
- A formal plugin extension story

## License

No license file is currently included. Until a license is added, all rights are reserved by default. Add a `LICENSE` file before encouraging external reuse or contributions.
