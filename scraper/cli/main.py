from __future__ import annotations

import csv
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Optional

import click
from rich.console import Console
from rich.table import Table

console = Console()
logger = logging.getLogger(__name__)


def _setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def _load_sources(name: Optional[str] = None):
    from scraper.scheduler.jobs import load_sources
    sources = load_sources()
    if name:
        sources = [s for s in sources if s.name == name]
        if not sources:
            click.echo(f"Unknown source: {name}", err=True)
            sys.exit(1)
    return sources


# ─────────────────────────────────────────────────────────────────────────────
# Root
# ─────────────────────────────────────────────────────────────────────────────
@click.group()
@click.option("--log-level", default="INFO", help="Logging level")
def cli(log_level: str):
    """Financial news scraper CLI."""
    _setup_logging(log_level)


# ─────────────────────────────────────────────────────────────────────────────
# db
# ─────────────────────────────────────────────────────────────────────────────
@cli.group()
def db():
    """Database management commands."""


@db.command("init")
def db_init():
    """Create all tables (skips existing)."""
    from scraper.db import Base, engine
    Base.metadata.create_all(engine)
    console.print("[green]✓[/green] Database tables created.")


@db.command("migrate")
def db_migrate():
    """Run alembic upgrade head."""
    import subprocess
    result = subprocess.run(["alembic", "upgrade", "head"], capture_output=True, text=True)
    if result.returncode == 0:
        console.print("[green]✓[/green] Migrations applied.")
        console.print(result.stdout)
    else:
        console.print("[red]Migration failed:[/red]")
        console.print(result.stderr)
        sys.exit(1)


@db.command("stats")
def db_stats():
    """Show article counts and quality summary."""
    from scraper.db import ArticleRepository, get_session
    with get_session() as session:
        repo = ArticleRepository(session)
        stats = repo.stats()

    table = Table(title="Database Stats")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    for k, v in stats.items():
        table.add_row(k.replace("_", " ").title(), str(v))
    console.print(table)


# ─────────────────────────────────────────────────────────────────────────────
# scrape
# ─────────────────────────────────────────────────────────────────────────────
@cli.command()
@click.option("--all", "all_sources", is_flag=True, help="Run all enabled real-time sources")
@click.option("--source", default=None, help="Run a specific source by name")
def scrape(all_sources: bool, source: Optional[str]):
    """Run real-time scraping for one or all sources."""
    from scraper.pipeline.orchestrator import Orchestrator

    if not all_sources and not source:
        click.echo("Specify --all or --source <name>", err=True)
        sys.exit(1)

    sources = _load_sources(source)
    sources = [s for s in sources if not s.backfill_only]

    orchestrator = Orchestrator()
    for config in sources:
        if not config.enabled:
            console.print(f"[yellow]Skipping disabled source:[/yellow] {config.name}")
            continue
        console.print(f"[cyan]Scraping:[/cyan] {config.name}")
        stats = orchestrator.run(config, run_type="realtime")
        console.print(
            f"  found={stats.found} stored=[green]{stats.stored}[/green] "
            f"duped={stats.duped} failed=[red]{stats.failed}[/red]"
        )


# ─────────────────────────────────────────────────────────────────────────────
# backfill
# ─────────────────────────────────────────────────────────────────────────────
@cli.command()
@click.option("--source", required=True, help="Source name (e.g. gdelt, finnhub)")
@click.option("--start", required=True, help="Start date YYYY-MM-DD")
@click.option("--end", required=True, help="End date YYYY-MM-DD")
@click.option("--workers", default=1, show_default=True, help="Parallel workers")
@click.option("--ticker", default=None, help="Ticker symbol (Finnhub only)")
def backfill(source: str, start: str, end: str, workers: int, ticker: Optional[str]):
    """Run historical backfill for a source."""
    from scraper.pipeline.orchestrator import BackfillOrchestrator

    sources = _load_sources(source)
    config = sources[0]

    from_dt = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    to_dt = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    console.print(
        f"[cyan]Backfill:[/cyan] {config.name} | {start} → {end} | workers={workers}"
    )
    orchestrator = BackfillOrchestrator()
    stats = orchestrator.run_backfill(config, from_dt, to_dt, workers=workers)
    console.print(
        f"  found={stats.found} stored=[green]{stats.stored}[/green] "
        f"duped={stats.duped} failed=[red]{stats.failed}[/red]"
    )


# ─────────────────────────────────────────────────────────────────────────────
# status
# ─────────────────────────────────────────────────────────────────────────────
@cli.command()
@click.option("--source", default=None, help="Filter by source name")
@click.option("--last", default="20", help="Number of recent runs to show")
def status(source: Optional[str], last: str):
    """Show recent scrape run status."""
    from scraper.db import ScrapeRunRepository, get_session

    limit = int(last.replace("d", ""))  # accepts "7d" or "20"

    with get_session() as session:
        repo = ScrapeRunRepository(session)
        runs = repo.recent(source_name=source, limit=limit)

    table = Table(title="Recent Scrape Runs")
    for col in ["ID", "Source", "Type", "Status", "Started", "Found", "Stored", "Duped", "Failed"]:
        table.add_column(col)

    for run in runs:
        status_color = "green" if run.status == "completed" else "red"
        table.add_row(
            str(run.id),
            run.source_name,
            run.run_type,
            f"[{status_color}]{run.status}[/{status_color}]",
            run.started_at.strftime("%Y-%m-%d %H:%M"),
            str(run.articles_found),
            str(run.articles_stored),
            str(run.articles_duped),
            str(run.articles_failed),
        )
    console.print(table)


# ─────────────────────────────────────────────────────────────────────────────
# query
# ─────────────────────────────────────────────────────────────────────────────
@cli.command()
@click.option("--ticker", default=None, help="Filter by ticker symbol")
@click.option("--from", "from_dt", default=None, help="Start date YYYY-MM-DD")
@click.option("--to", "to_dt", default=None, help="End date YYYY-MM-DD")
@click.option("--source", default=None, help="Filter by source name")
@click.option("--min-quality", default=None, type=float, help="Minimum quality score")
@click.option("--limit", default=100, show_default=True)
@click.option("--format", "fmt", default="table", type=click.Choice(["table", "csv", "json"]))
def query(
    ticker: Optional[str],
    from_dt: Optional[str],
    to_dt: Optional[str],
    source: Optional[str],
    min_quality: Optional[float],
    limit: int,
    fmt: str,
):
    """Query stored articles with optional filters."""
    from scraper.db import ArticleRepository, get_session

    _from = datetime.strptime(from_dt, "%Y-%m-%d").replace(tzinfo=timezone.utc) if from_dt else None
    _to = datetime.strptime(to_dt, "%Y-%m-%d").replace(tzinfo=timezone.utc) if to_dt else None

    with get_session() as session:
        repo = ArticleRepository(session)
        articles = repo.query_articles(
            ticker=ticker,
            from_dt=_from,
            to_dt=_to,
            source_name=source,
            min_quality=min_quality,
            limit=limit,
        )

    if fmt == "json":
        rows = [
            {
                "id": a.id,
                "url": a.url,
                "title": a.title,
                "published_at": a.published_at.isoformat(),
                "source_name": a.source_name,
                "tickers": a.get_tickers(),
                "quality_score": a.quality_score,
                "word_count": a.word_count,
            }
            for a in articles
        ]
        click.echo(json.dumps(rows, indent=2))

    elif fmt == "csv":
        writer = csv.DictWriter(
            sys.stdout,
            fieldnames=["id", "url", "title", "published_at", "source_name",
                        "tickers", "quality_score", "word_count", "body"],
        )
        writer.writeheader()
        for a in articles:
            writer.writerow({
                "id": a.id,
                "url": a.url,
                "title": a.title,
                "published_at": a.published_at.isoformat(),
                "source_name": a.source_name,
                "tickers": json.dumps(a.get_tickers()),
                "quality_score": a.quality_score,
                "word_count": a.word_count,
                "body": a.body,
            })

    else:
        table = Table(title=f"Articles ({len(articles)} results)")
        for col in ["ID", "Source", "Published", "Tickers", "QScore", "Words", "Title"]:
            table.add_column(col)
        for a in articles:
            table.add_row(
                str(a.id),
                a.source_name,
                a.published_at.strftime("%Y-%m-%d"),
                ", ".join(a.get_tickers()) or "-",
                f"{a.quality_score:.2f}",
                str(a.word_count),
                a.title[:60] + ("…" if len(a.title) > 60 else ""),
            )
        console.print(table)


# ─────────────────────────────────────────────────────────────────────────────
# scheduler
# ─────────────────────────────────────────────────────────────────────────────
@cli.group()
def scheduler():
    """Manage the APScheduler real-time scraping daemon."""


@scheduler.command("start")
@click.option("--daemon", is_flag=True, help="Run in foreground (block until Ctrl-C)")
def scheduler_start(daemon: bool):
    """Start the real-time scraping scheduler."""
    import signal
    import time
    from scraper.scheduler.jobs import start_scheduler

    sched = start_scheduler()
    console.print("[green]✓[/green] Scheduler started. Press Ctrl-C to stop.")
    if daemon:
        try:
            while True:
                time.sleep(60)
        except (KeyboardInterrupt, SystemExit):
            sched.shutdown()
            console.print("[yellow]Scheduler stopped.[/yellow]")


@scheduler.command("stop")
def scheduler_stop():
    """Stop the scheduler (no-op if not running in this process)."""
    from scraper.scheduler.jobs import stop_scheduler
    stop_scheduler()
    console.print("[yellow]Scheduler stopped (if it was running).[/yellow]")


if __name__ == "__main__":
    cli()
