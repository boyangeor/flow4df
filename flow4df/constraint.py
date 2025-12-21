from dataclasses import dataclass


@dataclass(frozen=True, kw_only=True)
class Constraint:
    name: str
    expression: str
