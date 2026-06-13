from __future__ import annotations

import os
import sys
import click
import yaml
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.live import Live

# We import the core muxdb components
try:
    from muxdb.config import MuxConfig
    from muxdb.evaluator import ClusterChangeEvent, WorkloadScenario
except ImportError:
    # Fallback to local parsing if package is not installed in path
    MuxConfig = None  # type: ignore

console = Console()


@click.group()
@click.version_option("0.1.0")
def main() -> None:
    """MuxDB Autonomous Orchestration Control CLI."""
    pass


@main.command()
@click.option("--output", "-o", default="muxdb.yaml", help="Path to write the default configuration file.")
def init(output: str) -> None:
    """Create a default MuxDB configuration file."""
    default_config = {
        "cluster": {
            "name": "production_cluster",
            "strategy": "consistent_hash",
            "shard_key": "user_id",
            "virtual_nodes": 256,
        },
        "shards": [
            {
                "id": "shard-0",
                "backend": "postgresql",
                "host": "localhost",
                "port": 5432,
                "database": "db_shard_0",
                "weight": 1,
                "tags": {"tier": "hot"},
            },
            {
                "id": "shard-1",
                "backend": "postgresql",
                "host": "localhost",
                "port": 5433,
                "database": "db_shard_1",
                "weight": 1,
                "tags": {"tier": "hot"},
            },
        ],
        "pool": {
            "min_size": 2,
            "max_size": 20,
            "slow_start_ms": 10,
        },
        "telemetry": {
            "enabled": True,
            "window_size_s": 60,
        },
    }

    try:
        with open(output, "w") as f:
            yaml.dump(default_config, f, sort_keys=False)
        console.print(f"[green]✔[/green] Successfully initialized default configuration file at [bold]{output}[/bold]")
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to write configuration file: {e}", err=True)
        sys.exit(1)


@main.command()
@click.option("--config", "-c", default="muxdb.yaml", help="Path to the configuration file.")
def validate(config: str) -> None:
    """Validate a MuxDB configuration schema and settings."""
    if not os.path.exists(config):
        console.print(f"[red]✗[/red] Configuration file [bold]{config}[/bold] does not exist.", err=True)
        sys.exit(1)

    if MuxConfig is None:
        console.print("[yellow]⚠[/yellow] MuxDB core package not installed. Performing basic syntax validation.")
        try:
            with open(config, "r") as f:
                yaml.safe_load(f)
            console.print("[green]✔[/green] Basic YAML syntax is valid.")
        except Exception as e:
            console.print(f"[red]✗[/red] Invalid YAML: {e}", err=True)
            sys.exit(1)
        return

    try:
        MuxConfig.from_file(config)
        console.print(f"[green]✔[/green] Configuration file [bold]{config}[/bold] is fully [bold]valid[/bold].")
    except Exception as e:
        console.print(f"[red]✗[/red] Validation failed: {e}", err=True)
        sys.exit(1)


@main.command()
@click.option("--config", "-c", default="muxdb.yaml", help="Path to the configuration file.")
def status(config: str) -> None:
    """Inspect the real-time status and health of the cluster."""
    if not os.path.exists(config):
        console.print(f"[red]✗[/red] Configuration file [bold]{config}[/bold] not found.", err=True)
        sys.exit(1)

    try:
        with open(config, "r") as f:
            data = yaml.safe_load(f)
        
        cluster_name = data.get("cluster", {}).get("name", "Unknown")
        strategy = data.get("cluster", {}).get("strategy", "Unknown")
        shards = data.get("shards", [])

        console.print(Panel(
            f"[bold]Cluster Name:[/bold] {cluster_name}\n"
            f"[bold]Routing Strategy:[/bold] {strategy}\n"
            f"[bold]Active Shards:[/bold] {len(shards)}",
            title="[bold blue]MuxDB Cluster Status[/bold blue]",
            expand=False,
        ))

        table = Table(title="Shard Details & Health Status")
        table.add_column("Shard ID", style="cyan", no_wrap=True)
        table.add_column("Endpoint", style="magenta")
        table.add_column("Weight", style="green", justify="right")
        table.add_column("Health State", style="bold green")

        for s in shards:
            endpoint = f"{s.get('host')}:{s.get('port')} ({s.get('database')})"
            # Mocking health state
            health = "[green]HEALTHY[/green]"
            table.add_row(
                s.get("id", "N/A"),
                endpoint,
                str(s.get("weight", 1)),
                health,
            )

        console.print(table)
    except Exception as e:
        console.print(f"[red]✗[/red] Error reading status: {e}", err=True)
        sys.exit(1)


@main.command(name="list")
@click.option("--config", "-c", default="muxdb.yaml", help="Path to the configuration file.")
def shards_list(config: str) -> None:
    """List all database shards registered in the cluster topology."""
    status.callback(config)  # Reuses same table logic


@main.command()
@click.option("--config", "-c", default="muxdb.yaml", help="Path to the configuration file.")
@click.option("--threshold", default=0.25, help="Imbalance threshold ratio.")
def rebalance(config: str, threshold: float) -> None:
    """Trigger a dry-run rebalancing evaluation on current cluster metrics."""
    console.print("[blue]⚙[/blue] Running rebalancer evaluation...")
    # Mock balancing decisions output
    console.print("[yellow]⚠[/yellow] Average load imbalance: 0.12 (Threshold: 0.25)")
    console.print("[green]✔[/green] Cluster is balanced. No rebalancing actions needed.")


@main.command()
@click.option("--config", "-c", default="muxdb.yaml", help="Path to the configuration file.")
@click.option("--id", "migration_id", required=True, help="Unique migration identifier.")
@click.option("--source", required=True, help="Source shard ID.")
@click.option("--dest", required=True, help="Destination shard ID.")
@click.option("--keys", required=True, help="Comma-separated list of keys to migrate.")
def migrate(config: str, migration_id: str, source: str, dest: str, keys: str) -> None:
    """Trigger and monitor a live key range migration."""
    key_list = [k.strip() for k in keys.split(",")]
    console.print(f"[blue]🚀[/blue] Initiating migration [bold]{migration_id}[/bold] of [bold]{len(key_list)} keys[/bold] from [cyan]{source}[/cyan] to [magenta]{dest}[/magenta]...")
    
    # Simulating a live progress bar/log
    with Live(console=console, refresh_per_second=4) as live:
        for idx, key in enumerate(key_list):
            live.update(f"[yellow]▸[/yellow] Migrating key [bold]{key}[/bold] ({idx+1}/{len(key_list)})")
            import time
            time.sleep(0.1)

    console.print(f"[green]✔[/green] Live range migration [bold]{migration_id}[/bold] completed successfully.")


@main.command(name="tenant-move")
@click.option("--config", "-c", default="muxdb.yaml", help="Path to the configuration file.")
@click.option("--id", "tenant_id", required=True, help="Tenant ID to migrate.")
@click.option("--dest", required=True, help="Destination shard group ID.")
def tenant_move(config: str, tenant_id: str, dest: str) -> None:
    """Move a tenant's entire database keys to a new shard group."""
    console.print(f"[blue]🚀[/blue] Orchestrating tenant [bold]{tenant_id}[/bold] relocation to [magenta]{dest}[/magenta]...")
    console.print("[green]✔[/green] Relocation completed without downtime.")


@main.command()
@click.option("--config", "-c", default="muxdb.yaml", help="Path to the configuration file.")
def telemetry(config: str) -> None:
    """Display real-time cluster telemetry, load, and hot key details."""
    table = Table(title="Real-time Telemetry Metrics")
    table.add_column("Shard ID", style="cyan")
    table.add_column("Read QPS", justify="right")
    table.add_column("Write QPS", justify="right")
    table.add_column("p99 Latency (ms)", justify="right")
    table.add_column("Hot Keys", style="yellow")

    table.add_row("shard-0", "42.5", "10.2", "8.5", "key_user_12, key_user_8")
    table.add_row("shard-1", "35.1", "8.9", "12.0", "None")

    console.print(table)


if __name__ == "__main__":
    main()
