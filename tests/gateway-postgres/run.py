#!/usr/bin/env python3
"""Owned ephemeral primary/physical-standby fixture; no shared DB access."""
from pathlib import Path
import json, os, socketserver, subprocess, sys, threading, time, uuid
ROOT = Path(__file__).resolve().parents[2]
IMAGE = "postgres@sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685"
prefix = "gateway-replay-" + uuid.uuid4().hex[:10]
primary, standby = prefix + "-primary", prefix + "-standby"

def docker(*args):
    try:
        return subprocess.check_output(["docker", *args], text=True, stderr=subprocess.STDOUT).strip()
    except subprocess.CalledProcessError as error:
        print(error.output[-3000:], file=sys.stderr)
        raise

def ready(container):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        result = subprocess.run(["docker", "exec", container, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"], capture_output=True)
        if result.returncode == 0: return
        time.sleep(.2)
    raise RuntimeError("fixture database failed readiness: " + docker("logs", container)[-3000:])

bridges = []
def url(container):
    # Docker stdio avoids dependence on desktop VM published-port forwarding.
    # Every connection still terminates at the actual PostgreSQL backend.
    class Tunnel(socketserver.BaseRequestHandler):
        def handle(self):
            process = subprocess.Popen(["docker", "exec", "-i", container, "busybox", "nc", "127.0.0.1", "5432"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0)
            def upload():
                try:
                    while chunk := self.request.recv(65536): process.stdin.write(chunk)
                except (OSError, BrokenPipeError): pass
                finally:
                    try: process.stdin.close()
                    except OSError: pass
            sender = threading.Thread(target=upload, daemon=True); sender.start()
            try:
                while chunk := process.stdout.read(65536): self.request.sendall(chunk)
            except OSError: pass
            finally:
                process.terminate()
                try: process.wait(timeout=2)
                except subprocess.TimeoutExpired: process.kill(); process.wait()
                try: self.request.shutdown(2)
                except OSError: pass
                sender.join(timeout=2)
    class Server(socketserver.ThreadingTCPServer):
        daemon_threads = True
    server = Server(("127.0.0.1", 0), Tunnel)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    bridges.append(server)
    return "postgres://postgres@127.0.0.1:" + str(server.server_address[1]) + "/postgres?sslmode=disable"

try:
    docker("network", "create", prefix)
    docker("run", "-d", "--name", primary, "--network", prefix, "--network-alias", "primary", "--tmpfs", "/var/lib/postgresql/data", "-e", "POSTGRES_HOST_AUTH_METHOD=trust", IMAGE, "postgres", "-c", "wal_level=replica", "-c", "max_wal_senders=4")
    ready(primary)
    docker("exec", primary, "sh", "-c", "printf 'host replication postgres all trust\n' >> /var/lib/postgresql/data/pg_hba.conf")
    docker("exec", primary, "psql", "-U", "postgres", "-c", "SELECT pg_reload_conf()")
    docker("run", "-d", "--name", standby, "--network", prefix, "--user", "postgres", "--tmpfs", "/replica:uid=70,gid=70,mode=0700", "--entrypoint", "sh", IMAGE, "-c", "pg_basebackup -h primary -U postgres -D /replica -R -X stream && exec postgres -D /replica")
    ready(standby)
    print("Fixture PostgreSQL " + docker("exec", primary, "postgres", "--version"), flush=True)
    env = {**os.environ, "GATEWAY_TEST_PRIMARY_URL": url(primary), "GATEWAY_TEST_REPLICA_URL": url(standby), "GATEWAY_TEST_PRIMARY_CONTAINER": primary}
    subprocess.run(["cargo", "test", "-p", "distributed", "--no-default-features", "--features", "gateway-delivery,graphql,postgres", "--test", "edge_query_delivery_postgres", "--", "--nocapture"], cwd=ROOT, env=env, check=True)
    subprocess.run(["cargo", "test", "-p", "distributed", "--no-default-features", "--features", "gateway-delivery,graphql,postgres", "--lib", "graphql::delivery::versions::tests", "--", "--nocapture"], cwd=ROOT, env=env, check=True)
    subprocess.run(["cargo", "test", "-p", "distributed", "--no-default-features", "--features", "gateway-delivery,graphql,postgres", "--test", "postgres_repository", "projected_command_ledger_rows_upgrade_to_atomic_and_preserve_checks", "--", "--nocapture"], cwd=ROOT, env={**env, "DATABASE_URL": env["GATEWAY_TEST_PRIMARY_URL"]}, check=True)
finally:
    for bridge in bridges:
        bridge.shutdown(); bridge.server_close()
    for container in [standby, primary]:
        subprocess.run(["docker", "rm", "-f", container], capture_output=True)
    subprocess.run(["docker", "network", "rm", prefix], capture_output=True)
