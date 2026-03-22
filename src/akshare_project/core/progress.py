from .paths import get_state_path


class ProgressStore:
    def __init__(self, name: str, suffix: str = "progress"):
        self.path = get_state_path(name, suffix=suffix)

    def load(self) -> set[str]:
        if not self.path.exists():
            return set()
        with self.path.open("r", encoding="utf-8") as file:
            return {line.strip() for line in file if line.strip()}

    def append(self, line: str) -> None:
        if not line:
            return
        with self.path.open("a", encoding="utf-8") as file:
            file.write(f"{line}\n")

    def append_lines(self, lines) -> None:
        normalized_lines = [str(line).rstrip("\n") for line in lines if str(line).strip()]
        if not normalized_lines:
            return
        with self.path.open("a", encoding="utf-8") as file:
            for line in normalized_lines:
                file.write(f"{line}\n")
