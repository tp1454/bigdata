import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "flink-jobs" / "src"))
sys.path.append(str(Path(__file__).resolve().parents[1] / "producers" / "src"))
