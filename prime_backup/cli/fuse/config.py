import dataclasses


@dataclasses.dataclass
class FuseConfig:
	log_call: bool = False
	no_cache: bool = False
	no_meta: bool = False

	@classmethod
	def get(cls) -> 'FuseConfig':
		return _config


_config = FuseConfig()
