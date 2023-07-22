import dataclasses

@dataclasses.dataclass(frozen=True)
class UcdpEvent:
	name: str
	params: dict
