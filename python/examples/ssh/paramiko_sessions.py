"""SSH sessions via paramiko (`pip install paramiko`; illustration only)."""
import paramiko

from connpool import ConnectionPool, PoolConfig, RetryPolicy


def connect():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.load_system_host_keys()
    client.connect("bastion.internal", username="deploy", timeout=10, banner_timeout=10)
    return client


def alive(client):
    transport = client.get_transport()
    return bool(transport and transport.is_active())


pool = ConnectionPool.from_callables(
    connect, validate=alive,
    config=PoolConfig(
        max_size=3, max_lifetime=3600,
        retry=RetryPolicy(max_attempts=4, base_delay=1, retry_on=(OSError, paramiko.SSHException)),
    ),
)

if __name__ == "__main__":
    with pool, pool.connection() as ssh:
        _, out, _ = ssh.exec_command("uptime")
        print(out.read().decode())
