import typer
from typing import Optional
import subprocess

app = typer.Typer()

def _dbt(
  cmd: str, 
  select: Optional[str], 
  exclude: Optional[str], 
  target: Optional[str], 
  full_refresh: bool, 
  project_dir: Optional[str], 
  profiles_dir: Optional[str]
):
  args = ["dbt", cmd]
  if select:
    args += ["--select", select]
  if exclude:
    args += ["--exclude", exclude]
  if target:
    args += ["--target", target]
  if full_refresh:
    args += ["--full-refresh"]
  if project_dir:
    args += ["--project-dir", project_dir]
  if profiles_dir:
    args += ["--profiles-dir", profiles_dir]
  subprocess.run(args, check=True)

@app.command()
def run(
  select: Optional[str] = None,
  exclude: Optional[str] = None,
  target: Optional[str] = None,
  full_refresh: bool = False,
  project_dir: Optional[str] = None,
  profiles_dir: Optional[str] = None,
):
  _dbt("run", select, exclude, target, full_refresh, project_dir, profiles_dir)

@app.command()
def build(
    select: Optional[str] = None,
    exclude: Optional[str] = None,
    target: Optional[str] = None,
    full_refresh: bool = False,
    project_dir: Optional[str] = None,
    profiles_dir: Optional[str] = None,
):
  _dbt("build", select, exclude, target, full_refresh, project_dir, profiles_dir)

@app.command()
def test(
    select: Optional[str] = None,
    exclude: Optional[str] = None,
    target: Optional[str] = None,
    project_dir: Optional[str] = None,
    profiles_dir: Optional[str] = None,
):
    _dbt("test", select, exclude, target, False, project_dir, profiles_dir)

@app.command()
def compile(
    select: Optional[str] = None,
    exclude: Optional[str] = None,
    target: Optional[str] = None,
    project_dir: Optional[str] = None,
    profiles_dir: Optional[str] = None,
):
    _dbt("compile", select, exclude, target, False, project_dir, profiles_dir)


if __name__ == "__main__":
    app()