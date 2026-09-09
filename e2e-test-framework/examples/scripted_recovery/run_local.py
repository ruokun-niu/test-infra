#!/usr/bin/env python3
"""Run the scripted pause scenario with retained, isolated local artifacts."""

import argparse
import collections
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


HERE = Path(__file__).resolve().parent
FRAMEWORK = HERE.parent.parent
RUN_ID = "local.scripted_recovery.run"


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def request(url, method="GET"):
    with urllib.request.urlopen(urllib.request.Request(url, method=method), timeout=3) as response:
        body = response.read()
        return json.loads(body) if body else None


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def check_ports(ports):
    require(len(ports) == len(set(ports)), "Ports must be distinct")
    for port in ports:
        with socket.socket() as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                listener.bind(("127.0.0.1", port))
            except OSError as error:
                raise RuntimeError(f"Port {port} unavailable; choose another port: {error}") from error


def binary_path(value):
    binary = Path(value).expanduser().resolve()
    require(binary.is_file() and os.access(binary, os.X_OK), f"Executable not found: {binary}")
    return binary


def fingerprint(binary):
    digest = hashlib.sha256()
    with binary.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"path": str(binary), "sha256": digest.hexdigest()}


class Scenario:
    def __init__(self, args, work):
        self.args = args
        self.work = work
        self.processes = []
        self.logs = []
        self.service_url = f"http://127.0.0.1:{args.service_port}/api/test_runs/{RUN_ID}"
        self.source_url = self.service_url + "/sources/script-db"
        self.admin_url = f"http://127.0.0.1:{args.admin_port}"
        self.server = None
        self.run_storage = work / "cache" / "test_runs" / RUN_ID

    def launch(self, command, logfile, log_filter):
        stream = (self.work / logfile).open("ab")
        self.logs.append(stream)
        environment = os.environ.copy()
        environment["RUST_LOG"] = log_filter
        process = subprocess.Popen(command, cwd=self.work, env=environment,
                                   stdout=stream, stderr=subprocess.STDOUT)
        self.processes.append(process)
        return process

    def wait(self, description, probe):
        deadline = time.monotonic() + self.args.timeout
        last_error = None
        while time.monotonic() < deadline:
            for process in self.processes:
                require(process.poll() is None, f"Process {process.pid} exited: {process.returncode}")
            try:
                value = probe()
                if value:
                    return value
            except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
                last_error = str(error)
            time.sleep(0.1)
        raise RuntimeError(f"Timed out waiting for {description}; last network error: {last_error}")

    def generator(self):
        source = request(self.source_url)
        generator = source["source_change_generator"]
        require(generator["status"] not in ("Error", "Stopped"), f"Generator failed: {generator}")
        return generator["state"]

    def pause(self, label):
        def probe():
            state = self.generator()
            previous = state.get("previous_record") or {}
            record = previous.get("scripted", {}).get("record", {})
            return state if state["status"] == "Paused" and record.get("label") == label else None

        state = self.wait(f"script pause {label}", probe)
        write_json(self.work / f"generator-{label}.json", state)
        return state

    def source_ordinals(self):
        events = []
        for filename in sorted((self.run_storage / "sources" / "script-db").rglob("*.jsonl")):
            for line in filename.read_text().splitlines():
                event = json.loads(line)
                events.append(event["payload"]["after"]["properties"]["ordinal"])
        return events

    def snapshot(self, expected):
        response = request(self.admin_url + "/api/v1/queries/items/results")
        require(response.get("success") is True, f"Snapshot request failed: {response}")
        rows = response["data"]
        require(isinstance(rows, list), f"Invalid snapshot: {response}")
        ordinals = sorted(row["Ordinal"] for row in rows)
        if ordinals != expected:
            return None
        return response

    def deliveries(self):
        ordinals = []
        for filename in sorted((self.run_storage / "reactions" / "items").rglob("outputs*.jsonl")):
            for line in filename.read_text().splitlines():
                record = json.loads(line)
                body = record["payload"]["request_body"]
                if "result" not in body:
                    continue
                result = body["result"]
                require(result["type"] == "ADD", f"Unexpected result for insert-only query: {result}")
                ordinals.append(result["after"]["Ordinal"])
        return ordinals

    def wait_deliveries(self, expected):
        def probe():
            received = self.deliveries()
            require(set(received) <= set(expected), f"Unexpected deliveries: {received}")
            return received if set(received) == set(expected) else None
        return self.wait(f"reaction deliveries {expected}", probe)

    def start_server(self, binary):
        self.server = self.launch(
            [str(binary), "--config", str(self.work / "server.yaml")], "drasi-server.log",
            os.environ.get("DRASI_RUST_LOG", "info,drasi_lib::queries=debug,drasi_lib::reactions=debug"),
        )
        self.wait("Drasi health", lambda: request(self.admin_url + "/health"))

    def close(self):
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        for stream in self.logs:
            stream.close()

    def run(self):
        args = self.args
        service = binary_path(args.test_service_bin)
        server = binary_path(args.drasi_server_bin) if args.mode != "framework" else None
        ports = [args.service_port]
        if server:
            ports.extend([args.admin_port, args.source_port, args.reaction_port])
        check_ports(ports)
        metadata = {"mode": args.mode, "test_service": fingerprint(service),
                    "verify_plugins": not args.allow_local_plugins,
                    "ports": ports, "started_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        if server:
            metadata["drasi_server"] = fingerprint(server)
        write_json(self.work / "run.json", metadata)

        config = json.loads((HERE / "config.json").read_text())
        repo = config["data_store"]["test_repos"][0]
        repo["source_path"] = str(HERE / "dev_repo")
        if server:
            test = repo["local_tests"][0]
            test["sources"][0]["source_change_dispatchers"].insert(0, {
                "kind": "Grpc", "host": "127.0.0.1", "port": args.source_port,
                "source_id": "script-db", "tls": False, "batch_events": False,
                "timeout_seconds": 10,
            })
            test["reactions"] = [{
                "test_reaction_id": "items", "stop_triggers": [],
                "output_handler": {
                    "kind": "Grpc", "host": "127.0.0.1", "port": args.reaction_port,
                    "correlation_metadata_key": "x-query-sequence", "query_ids": ["items"],
                    "include_initial_state": False,
                },
            }]
            config["test_run_host"]["test_runs"][0]["reactions"] = [{
                "test_reaction_id": "items", "start_immediately": True,
                "output_loggers": [{"kind": "JsonlFile", "max_lines_per_file": 1}],
            }]
            server_config = {
                "id": "scripted-recovery", "host": "127.0.0.1", "port": args.admin_port,
                "persistConfig": True, "persistIndex": True, "enableUi": False,
                "stateStore": {"kind": "redb", "path": "./data/state.redb"},
                "autoInstallPlugins": False,
                "verifyPlugins": not args.allow_local_plugins,
                "plugins": [{"ref": "source/grpc"}, {"ref": "reaction/grpc"}],
                "sources": [{"kind": "grpc", "id": "script-db", "autoStart": True,
                             "host": "127.0.0.1", "port": args.source_port,
                             "durability": {"enabled": True, "max_events": 1000}}],
                "queries": [{"id": "items", "autoStart": True,
                             "query": "MATCH (item:Item) RETURN item.ordinal AS Ordinal",
                             "queryLanguage": "Cypher", "enableBootstrap": False,
                             "outboxCapacity": 1000, "sources": [{"sourceId": "script-db"}]}],
                "reactions": [{"kind": "grpc", "id": "items-out", "queries": ["items"],
                               "autoStart": True, "batchSize": 1, "batchFlushTimeoutMs": 100,
                               "endpoint": f"grpc://127.0.0.1:{args.reaction_port}",
                               "metadata": {"x-query-sequence": "items"}}],
            }
            (self.work / "data").mkdir()
            write_json(self.work / "server.yaml", server_config)
            self.start_server(server)
        write_json(self.work / "config.json", config)
        self.launch([str(service), "--config", str(self.work / "config.json"),
                     "--port", str(args.service_port)], "test-service.log",
                    os.environ.get("TEST_SERVICE_RUST_LOG", "info"))
        self.wait("generator API", self.generator)
        request(self.source_url + "/start", "POST")
        before = self.pause("before-crash")
        require(before["next_record"]["record"]["source_change_event"]["payload"]["source"]["lsn"] == 3,
                "Pause did not retain change 3 as the next record")
        require(self.source_ordinals() == [1, 2], "Unexpected inputs before pause")
        print("Paused after inputs 1, 2; next input is 3.", flush=True)
        if server:
            snapshot = self.wait("first two query rows", lambda: self.snapshot([1, 2]))
            write_json(self.work / "snapshot-before.json", snapshot)
            self.wait_deliveries([1, 2])
        if args.mode == "crash":
            print(f"SIGKILL Drasi pid={self.server.pid}; generator remains paused.", flush=True)
            self.server.kill()
            self.server.wait(timeout=10)
            self.processes.remove(self.server)
            self.start_server(server)
            after = self.generator()
            write_json(self.work / "generator-after-restart.json", after)
            require(after == before, "Generator state changed during server restart")
            require(self.source_ordinals() == [1, 2], "Generator sent inputs while paused")
            snapshot = self.wait("restored first two rows", lambda: self.snapshot([1, 2]))
            write_json(self.work / "snapshot-restored.json", snapshot)
        request(self.source_url + "/start", "POST")
        self.pause("after-resume")
        require(self.source_ordinals() == [1, 2, 3, 4], "Inputs missing, duplicated, or reordered")
        print("Resumed at input 3; dispatched inputs 1..4 exactly once.", flush=True)
        received = None
        if server:
            snapshot = self.wait("all four query rows", lambda: self.snapshot([1, 2, 3, 4]))
            write_json(self.work / "snapshot-final.json", snapshot)
            self.wait_deliveries([1, 2, 3, 4])
        request(self.source_url + "/start", "POST")
        self.wait("script finish", lambda: self.generator()["status"] == "Finished")
        if server:
            request(self.service_url + "/reactions/items/stop", "POST")
            received = self.deliveries()
            require(set(received) == {1, 2, 3, 4}, f"Incomplete deliveries: {received}")
            if args.mode == "clean":
                require(received == [1, 2, 3, 4], f"Unexpected clean delivery order/count: {received}")
        verdict = {"passed": True, "scope": args.mode, "input_ordinals": self.source_ordinals(),
                   "delivered_ordinals": received,
                   "delivery_counts": dict(collections.Counter(received or [])),
                   "note": "Bounded scripted scenario; not proof of general exactly-once recovery."}
        write_json(self.work / "verdict.json", verdict)
        print(f"PASS ({args.mode}). Artifacts: {self.work}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["framework", "clean", "crash"], default="framework")
    parser.add_argument("--test-service-bin", default=os.environ.get(
        "TEST_SERVICE_BIN", str(FRAMEWORK / "target" / "debug" / "test-service")))
    parser.add_argument("--drasi-server-bin", default=os.environ.get("DRASI_SERVER_BIN", ""))
    parser.add_argument("--allow-local-plugins", action="store_true",
                        help="Disable plugin verification for trusted locally rebuilt plugins only")
    parser.add_argument("--service-port", type=int, default=63124)
    parser.add_argument("--admin-port", type=int, default=8091)
    parser.add_argument("--source-port", type=int, default=50061)
    parser.add_argument("--reaction-port", type=int, default=50062)
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()
    work = Path(tempfile.mkdtemp(prefix=f"scripted-recovery-{args.mode}-"))
    print(f"Artifacts: {work}", flush=True)
    scenario = Scenario(args, work)
    try:
        scenario.run()
        return 0
    except (Exception, KeyboardInterrupt) as error:
        write_json(work / "verdict.json", {"passed": False, "scope": args.mode, "error": str(error)})
        print(f"FAIL: {error}. Logs and state: {work}", flush=True)
        return 1
    finally:
        scenario.close()


if __name__ == "__main__":
    raise SystemExit(main())