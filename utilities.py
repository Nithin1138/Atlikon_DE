"""Root utilities module supporting `%run ./utilities` across Databricks and local IDE."""
import sys
import os

repo_root = None

# Strategy A: Databricks notebook context
try:
    nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    full_path = f"/Workspace{nb_path}" if not nb_path.startswith("/Workspace") else nb_path
    parts = full_path.split("/")
    if "Atlikon_DE" in parts:
        idx = parts.index("Atlikon_DE")
        repo_root = "/".join(parts[:idx+1])
except Exception:
    pass

# Strategy B: Local file or CWD inspection
if not repo_root or not os.path.exists(os.path.join(repo_root, "src")):
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        current_dir = os.getcwd()
    candidate = current_dir
    while candidate and candidate != "/":
        if os.path.exists(os.path.join(candidate, "src")):
            repo_root = candidate
            break
        candidate = os.path.dirname(candidate)

# Strategy C: Databricks Workspace fallback paths
if not repo_root or not os.path.exists(os.path.join(repo_root, "src")):
    for candidate in [
        "/Workspace/Users/veeranithin9@gmail.com/Atlikon_DE",
        "/Workspace/Repos/veeranithin9@gmail.com/Atlikon_DE",
    ]:
        if os.path.exists(os.path.join(candidate, "src")):
            repo_root = candidate
            break

if repo_root and repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# Expose Config, Schemas, Transformations, Data Quality & Auditing
try:
    env = dbutils.widgets.get("env").lower()
except Exception:
    env = os.getenv("ENVIRONMENT", "prod").lower()

CONFIG = {
    "dev": {"catalog": "fmcg_dev", "bucket": "spartsbar-2355-dev"},
    "prod": {"catalog": "fmcg", "bucket": "spartsbar-2355"},
}
active_config = CONFIG.get(env, CONFIG["prod"])
catalog = active_config["catalog"]
bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"
s3_bucket = active_config["bucket"]

def base_path_for(data_source: str) -> str:
    return f"s3://{s3_bucket}/{data_source}/*.csv"

from src.schemas import *
from src.transformations import *
from src.data_quality import *
from src.audit import *
from src.config import *
