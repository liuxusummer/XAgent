from __future__ import annotations

import json
import socket
import tempfile
import threading
import unittest
import urllib.request
from pathlib import Path
from unittest.mock import patch

from src.core.eval import (
    _DatasetRedirectHandler,
    _NoBypassProxyHandler,
    _validate_dataset_url,
    EvalError,
    create_eval_run,
    download_dataset,
    evaluate_assertions,
    execute_eval_run,
    get_dataset_detail,
    import_dataset_content,
    import_dataset_path,
    list_datasets,
    parse_dataset,
    read_eval_run,
    summarize_cases,
)


class _FakeAgent:
    def __init__(self, result: dict) -> None:
        self.result = result
        self.closed = False

    def run_task(self, _task: str) -> dict:
        return self.result

    def close(self) -> None:
        self.closed = True


class _FakeResponse:
    def __init__(self, data: bytes, url: str = "https://example.test/data.jsonl") -> None:
        self._data = data
        self._offset = 0
        self.url = url
        self.headers = {"Content-Length": str(len(data))}

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self._data) - self._offset
        chunk = self._data[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        return None


class _FakeEgressProxy:
    url = "http://127.0.0.1:43210"

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        return None


class EvalDatasetTests(unittest.TestCase):
    def test_parse_jsonl_json_and_csv_datasets(self) -> None:
        jsonl_cases = parse_dataset(
            '{"id":"a","task":"say hello","assertions":{"contains":["hello"]}}\n',
            "jsonl",
        )
        json_cases = parse_dataset(
            json.dumps({"cases": [{"task": "read file", "assertions": {"max_turns": 3}}]}),
            "json",
        )
        csv_cases = parse_dataset(
            'id,task,assertions\nc1,"call tool","{""tool_called"": [""file_read""]}"\n',
            "csv",
        )

        self.assertEqual(jsonl_cases[0].id, "a")
        self.assertEqual(json_cases[0].assertions["max_turns"], 3)
        self.assertEqual(csv_cases[0].assertions["tool_called"], ["file_read"])

    def test_parse_rejects_case_without_task(self) -> None:
        with self.assertRaises(EvalError):
            parse_dataset('[{"id": "bad"}]', "json")

    def test_parse_rejects_non_positive_or_non_finite_limits(self) -> None:
        invalid_assertions = [
            '{"max_turns":0}',
            '{"max_duration_sec":0}',
            '{"max_duration_sec":NaN}',
        ]
        for assertions in invalid_assertions:
            with self.subTest(assertions=assertions), self.assertRaises(EvalError):
                parse_dataset(
                    f'[{{"task":"bad","assertions":{assertions}}}]',
                    "json",
                )

    def test_parse_rejects_duplicate_ids_and_deduplicates_tags(self) -> None:
        with self.assertRaises(EvalError):
            parse_dataset(
                '[{"id":"same","task":"one"},{"id":"same","task":"two"}]',
                "json",
            )

        cases = parse_dataset(
            '[{"id":"one","task":"one","tags":["recovery","recovery"]}]',
            "json",
        )
        self.assertEqual(cases[0].tags, ["recovery"])

    def test_import_path_stays_inside_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            data_file = root / "business" / "eval.jsonl"
            data_file.parent.mkdir()
            data_file.write_text('{"task": "hello"}\n', encoding="utf-8")

            metadata = import_dataset_path(root, rel_path="business/eval.jsonl")

            detail = get_dataset_detail(root, metadata["id"])
            self.assertEqual(detail["case_count"], 1)
            self.assertEqual(detail["cases"][0]["task"], "hello")

            with self.assertRaises(EvalError):
                import_dataset_path(root, rel_path="../outside.jsonl")

    def test_download_dataset_limits_protocol_and_saves_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            payload = b'{"task": "downloaded", "assertions": {"contains": ["ok"]}}\n'
            public_dns = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))]
            with (
                patch("src.core.network_guard.socket.getaddrinfo", return_value=public_dns),
                patch(
                    "src.core.eval._open_dataset_url",
                    return_value=_FakeResponse(payload),
                ) as open_url,
                patch(
                    "src.core.eval.PublicEgressProxy",
                    return_value=_FakeEgressProxy(),
                ),
            ):
                metadata = download_dataset(tmp_dir, url="https://example.test/data.jsonl")

            self.assertEqual(metadata["source"]["type"], "url")
            self.assertTrue(open_url.call_args.args[2].startswith("http://127.0.0.1:"))
            detail = get_dataset_detail(tmp_dir, metadata["id"])
            self.assertEqual(detail["cases"][0]["task"], "downloaded")

            with self.assertRaises(EvalError):
                download_dataset(tmp_dir, url="file:///tmp/data.jsonl")

    def test_dataset_url_rejects_private_and_credentialed_hosts(self) -> None:
        blocked_urls = [
            "http://0.0.0.0/data.jsonl",
            "http://127.0.0.1/data.jsonl",
            "http://[::1]/data.jsonl",
            "http://[fc00::1]/data.jsonl",
            "http://[fe80::1]/data.jsonl",
            "http://100.64.0.1/data.jsonl",
            "http://169.254.169.254/latest/meta-data",
            "http://10.0.0.1/data.jsonl",
            "http://192.0.2.1/data.jsonl",
            "http://224.0.0.1/data.jsonl",
            "https://user:password@example.test/data.jsonl",
        ]

        for url in blocked_urls:
            with self.subTest(url=url), self.assertRaises(EvalError):
                _validate_dataset_url(url)

    def test_dataset_url_rejects_dns_answers_containing_private_address(self) -> None:
        mixed_dns = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.5", 443)),
        ]

        with (
            patch("src.core.network_guard.socket.getaddrinfo", return_value=mixed_dns),
            self.assertRaises(EvalError),
        ):
            _validate_dataset_url("https://example.test/data.jsonl")

    def test_download_revalidates_final_response_url(self) -> None:
        public_dns = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))]
        response = _FakeResponse(b'{"task":"blocked"}', url="http://127.0.0.1/private.jsonl")

        with (
            patch("src.core.network_guard.socket.getaddrinfo", return_value=public_dns),
            patch("src.core.eval._open_dataset_url", return_value=response),
            patch("src.core.eval.PublicEgressProxy", return_value=_FakeEgressProxy()),
            tempfile.TemporaryDirectory() as tmp_dir,
            self.assertRaises(EvalError),
        ):
            download_dataset(tmp_dir, url="https://example.test/data.jsonl")

    def test_dataset_redirect_revalidates_destination(self) -> None:
        handler = _DatasetRedirectHandler()
        request = urllib.request.Request("https://example.test/data.jsonl")

        with self.assertRaises(EvalError):
            handler.redirect_request(
                request,
                None,
                302,
                "Found",
                {},
                "http://127.0.0.1/private.jsonl",
            )

        public_dns = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))]
        with patch("src.core.network_guard.socket.getaddrinfo", return_value=public_dns):
            redirected = handler.redirect_request(
                request,
                None,
                302,
                "Found",
                {},
                "https://cdn.example.test/data.jsonl",
            )

        self.assertEqual(redirected.full_url, "https://cdn.example.test/data.jsonl")

    def test_dataset_proxy_ignores_process_no_proxy_bypass(self) -> None:
        handler = _NoBypassProxyHandler(
            {
                "http": "http://127.0.0.1:43210",
                "https": "http://127.0.0.1:43210",
            }
        )
        request = urllib.request.Request("https://example.test/data.jsonl")

        with patch("urllib.request.proxy_bypass", return_value=True) as proxy_bypass:
            handler.proxy_open(request, "http://127.0.0.1:43210", "https")

        proxy_bypass.assert_not_called()
        self.assertEqual(request.host, "127.0.0.1:43210")
        self.assertEqual(request._tunnel_host, "example.test")


class EvalAssertionTests(unittest.TestCase):
    def test_assertions_cover_response_tools_and_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            (root / "business").mkdir()
            (root / "business" / "answer.txt").write_text("saved text", encoding="utf-8")
            case = {
                "assertions": {
                    "contains": ["hello"],
                    "contains_any": ["absent", "world"],
                    "not_contains": ["forbidden"],
                    "exit_reason": ["CURRENT_TASK_DONE"],
                    "tool_called": ["file_read"],
                    "file_exists": ["business/answer.txt"],
                    "file_contains": {"business/answer.txt": "saved"},
                    "max_turns": 5,
                    "max_duration_sec": 10,
                }
            }
            result = {
                "response": "hello world",
                "exit_reason": "CURRENT_TASK_DONE",
                "tool_results": [{"tool_name": "file_read"}],
                "turns": 2,
            }

            status, failures = evaluate_assertions(case, result, workspace_root=root, duration_sec=0.1)

            self.assertEqual(status, "passed")
            self.assertEqual(failures, [])

    def test_assertions_report_failures(self) -> None:
        case = {
            "assertions": {
                "contains": ["needle"],
                "contains_any": ["missing-a", "missing-b"],
                "tool_called": ["file_read"],
                "max_turns": 1,
            }
        }
        result = {"response": "haystack", "exit_reason": "CURRENT_TASK_DONE", "tool_results": [], "turns": 3}

        status, failures = evaluate_assertions(case, result, workspace_root=".", duration_sec=0.1)

        self.assertEqual(status, "failed")
        self.assertGreaterEqual(len(failures), 4)

    def test_agent_error_is_reported_as_error_without_assertion_noise(self) -> None:
        case = {"assertions": {"contains": ["needle"], "tool_called": ["file_search"]}}
        result = {"response": "[error] LLM request failed: HTTP Error 401: Unauthorized", "exit_reason": "ERROR"}

        status, failures = evaluate_assertions(case, result, workspace_root=".", duration_sec=0.1)

        self.assertEqual(status, "error")
        self.assertEqual(failures, ["[error] LLM request failed: HTTP Error 401: Unauthorized"])

    def test_summary_records_recovery_tokens_policy_and_tags(self) -> None:
        summary = summarize_cases(
            [
                {
                    "status": "passed",
                    "duration_sec": 1,
                    "turns": 2,
                    "tags": ["recovery"],
                    "tool_attempts": 2,
                    "successful_tool_attempts": 1,
                    "failed_tool_attempts": 1,
                    "unknown_tool_attempts": 0,
                    "policy_outcomes": {
                        "allow": 1,
                        "deny": 0,
                        "require_approval": 1,
                    },
                    "token_usage": {"input_tokens": 7, "total_tokens": 10},
                },
                {
                    "status": "failed",
                    "duration_sec": 3,
                    "turns": 4,
                    "tags": ["recovery"],
                    "tool_attempts": 1,
                    "successful_tool_attempts": 0,
                    "failed_tool_attempts": 1,
                    "unknown_tool_attempts": 0,
                    "policy_outcomes": {
                        "allow": 0,
                        "deny": 1,
                        "require_approval": 0,
                    },
                    "token_usage": {"input_tokens": 13, "total_tokens": 20},
                },
            ]
        )

        self.assertEqual(summary["pass_rate"], 0.5)
        self.assertEqual(summary["p95_duration"], 2.9)
        self.assertEqual(summary["tool_attempts"], 3)
        self.assertAlmostEqual(summary["tool_success_rate"], 1 / 3)
        self.assertEqual(summary["recovery_opportunities"], 2)
        self.assertEqual(summary["recovery_rate"], 0.5)
        self.assertEqual(summary["policy_outcomes"]["require_approval"], 1)
        self.assertEqual(summary["token_usage"]["total_tokens"], 30)
        self.assertEqual(summary["avg_total_tokens"], 15)
        self.assertEqual(summary["total_token_coverage"], 1)
        self.assertEqual(summary["tags"]["recovery"]["completed"], 2)

    def test_total_token_average_excludes_partial_usage(self) -> None:
        summary = summarize_cases(
            [
                {
                    "status": "passed",
                    "token_usage": {"input_tokens": 8},
                },
                {
                    "status": "passed",
                    "token_usage": {"input_tokens": 5, "total_tokens": 7},
                },
            ]
        )

        self.assertEqual(summary["token_coverage"], 1)
        self.assertEqual(summary["total_token_coverage"], 0.5)
        self.assertEqual(summary["avg_total_tokens"], 7)


class EvalRunTests(unittest.TestCase):
    def test_run_digest_covers_only_the_evaluated_case_subset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            metadata = import_dataset_content(
                tmp_dir,
                name="suite.jsonl",
                fmt="jsonl",
                content='{"task":"one"}\n{"task":"two"}\n',
            )
            full = create_eval_run(
                tmp_dir,
                workspace="default.ws",
                dataset_id=metadata["id"],
                agent="main",
            )
            limited = create_eval_run(
                tmp_dir,
                workspace="default.ws",
                dataset_id=metadata["id"],
                agent="main",
                case_limit=1,
            )

            self.assertEqual(metadata["schema_version"], 1)
            self.assertEqual(len(metadata["content_sha256"]), 64)
            self.assertEqual(full["dataset_case_count"], 2)
            self.assertEqual(limited["dataset_case_count"], 1)
            self.assertNotEqual(full["dataset_digest"], limited["dataset_digest"])
            self.assertEqual(
                full["dataset_source_digest"],
                limited["dataset_source_digest"],
            )
            with self.assertRaises(EvalError):
                create_eval_run(
                    tmp_dir,
                    workspace="default.ws",
                    dataset_id=metadata["id"],
                    agent="main",
                    case_limit=501,
                )

    def test_explicit_storage_root_physically_partitions_owner_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            storage_root = Path(tmp_dir) / "private-owner"
            owner_digest = "a" * 64
            metadata = import_dataset_content(
                tmp_dir,
                name="private.jsonl",
                fmt="jsonl",
                content='{"task":"private"}\n',
                storage_root=storage_root,
                owner_digest=owner_digest,
            )
            run = create_eval_run(
                tmp_dir,
                workspace="default.ws",
                dataset_id=metadata["id"],
                agent="main",
                storage_root=storage_root,
                owner_digest=owner_digest,
            )

            self.assertEqual(list_datasets(tmp_dir), [])
            self.assertEqual(
                get_dataset_detail(
                    tmp_dir,
                    metadata["id"],
                    storage_root=storage_root,
                )["owner_digest"],
                owner_digest,
            )
            self.assertEqual(
                read_eval_run(
                    tmp_dir,
                    run["id"],
                    storage_root=storage_root,
                )["owner_digest"],
                owner_digest,
            )
            with self.assertRaises(EvalError):
                read_eval_run(tmp_dir, run["id"])

    def test_secure_eval_error_redaction_omits_exception_details(self) -> None:
        class _FailingAgent:
            def run_task(self, _task: str) -> dict:
                raise RuntimeError("/private/host/credential.txt")

            def close(self) -> None:
                return None

        with tempfile.TemporaryDirectory() as tmp_dir:
            metadata = import_dataset_content(
                tmp_dir,
                name="suite.jsonl",
                fmt="jsonl",
                content='{"task":"fail"}\n',
            )
            run = create_eval_run(
                tmp_dir,
                workspace="default.ws",
                dataset_id=metadata["id"],
                agent="main",
            )

            result = execute_eval_run(
                tmp_dir,
                run["id"],
                agent_factory=_FailingAgent,
                redact_errors=True,
            )

            rendered = json.dumps(result)
            self.assertNotIn("/private/host", rendered)
            self.assertIn("evaluation case failed", rendered)

    def test_execute_eval_run_serially_with_fresh_agents(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            metadata = import_dataset_content(
                tmp_dir,
                name="suite.jsonl",
                fmt="jsonl",
                content=(
                    '{"id":"one","task":"first","assertions":{"contains":["ok"]}}\n'
                    '{"id":"two","task":"second","assertions":{"contains":["ok"]}}\n'
                ),
            )
            run = create_eval_run(tmp_dir, workspace="default.ws", dataset_id=metadata["id"], agent="main")
            built: list[_FakeAgent] = []

            def factory() -> _FakeAgent:
                agent = _FakeAgent(
                    {
                        "response": "ok",
                        "exit_reason": "CURRENT_TASK_DONE",
                        "tool_results": [],
                        "turns": 1,
                    }
                )
                built.append(agent)
                return agent

            result = execute_eval_run(tmp_dir, run["id"], agent_factory=factory)

            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["summary"]["passed"], 2)
            self.assertEqual(len(built), 2)
            self.assertTrue(all(agent.closed for agent in built))
            stored = read_eval_run(tmp_dir, run["id"])
            self.assertEqual(stored["cases"][0]["status"], "passed")

    def test_execute_eval_run_captures_bounded_case_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            metadata = import_dataset_content(
                tmp_dir,
                name="suite.jsonl",
                fmt="jsonl",
                content=(
                    '{"task":"recover","tags":["capability"],'
                    '"assertions":{"contains":["ok"]}}\n'
                    '{"task":"deny","tags":["capability"],'
                    '"assertions":{"contains":["ok"]}}\n'
                ),
            )
            run = create_eval_run(
                tmp_dir,
                workspace="default.ws",
                dataset_id=metadata["id"],
                agent="main",
            )
            raw_results = iter(
                [
                    {
                        "response": "ok",
                        "exit_reason": "CURRENT_TASK_DONE",
                        "turns": 2,
                        "usage": {"input_tokens": 7, "total_tokens": 10},
                        "tool_results": [
                            {
                                "tool_name": "file_read",
                                "data": {"status": "ERROR"},
                                "policy": {
                                    "outcomes": ["require_approval", "allow"]
                                },
                            },
                            {
                                "tool_name": "file_search",
                                "data": {"status": "OK"},
                                "policy": {"outcome": "allow"},
                            },
                        ],
                    },
                    {
                        "response": "not the expected response",
                        "exit_reason": "CURRENT_TASK_DONE",
                        "turns": 1,
                        "usage": {"input_tokens": 13, "total_tokens": 20},
                        "tool_results": [
                            {
                                "tool_name": "file_write",
                                "data": {"status": "SKIP"},
                                "policy": {"outcome": "deny"},
                            }
                        ],
                    },
                ]
            )

            result = execute_eval_run(
                tmp_dir,
                run["id"],
                agent_factory=lambda: _FakeAgent(next(raw_results)),
            )

            self.assertTrue(result["cases"][0]["recovered"])
            self.assertEqual(result["summary"]["tool_attempts"], 3)
            self.assertAlmostEqual(result["summary"]["tool_success_rate"], 1 / 3)
            self.assertEqual(result["summary"]["recovery_rate"], 0.5)
            self.assertEqual(
                result["summary"]["policy_outcomes"],
                {"allow": 2, "deny": 1, "require_approval": 1},
            )
            self.assertEqual(result["summary"]["token_usage"]["total_tokens"], 30)
            self.assertEqual(
                result["summary"]["tags"]["capability"]["pass_rate"],
                0.5,
            )

    def test_cancel_stops_unstarted_cases(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            metadata = import_dataset_content(
                tmp_dir,
                name="suite.jsonl",
                fmt="jsonl",
                content='{"task":"first"}\n{"task":"second"}\n',
            )
            run = create_eval_run(tmp_dir, workspace="default.ws", dataset_id=metadata["id"], agent="main")
            cancel_event = threading.Event()
            calls = 0

            def factory() -> _FakeAgent:
                nonlocal calls
                calls += 1
                cancel_event.set()
                return _FakeAgent({"response": "", "exit_reason": "CURRENT_TASK_DONE", "tool_results": [], "turns": 1})

            result = execute_eval_run(tmp_dir, run["id"], agent_factory=factory, cancel_event=cancel_event)

            self.assertEqual(result["status"], "canceled")
            self.assertEqual(calls, 1)
            self.assertEqual(result["summary"]["completed"], 1)

    def test_cancel_stops_the_current_long_running_agent(self) -> None:
        class _BlockingAgent:
            def __init__(self) -> None:
                self.started = threading.Event()
                self.released = threading.Event()
                self.stopped = False
                self.closed = False

            def run_task(self, _task: str) -> dict:
                self.started.set()
                if not self.released.wait(2):
                    raise RuntimeError("agent was not stopped")
                return {
                    "response": "",
                    "exit_reason": "EXITED",
                    "tool_results": [],
                    "turns": 1,
                }

            def stop(self) -> None:
                self.stopped = True
                self.released.set()

            def close(self) -> None:
                self.closed = True

        with tempfile.TemporaryDirectory() as tmp_dir:
            metadata = import_dataset_content(
                tmp_dir,
                name="suite.jsonl",
                fmt="jsonl",
                content='{"task":"long"}\n',
            )
            run = create_eval_run(
                tmp_dir,
                workspace="default.ws",
                dataset_id=metadata["id"],
                agent="main",
            )
            cancel_event = threading.Event()
            agent = _BlockingAgent()
            outcome: list[dict] = []
            worker = threading.Thread(
                target=lambda: outcome.append(
                    execute_eval_run(
                        tmp_dir,
                        run["id"],
                        agent_factory=lambda: agent,
                        cancel_event=cancel_event,
                    )
                )
            )
            worker.start()
            self.assertTrue(agent.started.wait(1))
            cancel_event.set()
            worker.join(timeout=2)

            self.assertFalse(worker.is_alive())
            self.assertTrue(agent.stopped)
            self.assertTrue(agent.closed)
            self.assertEqual(outcome[0]["status"], "canceled")


if __name__ == "__main__":
    unittest.main()
