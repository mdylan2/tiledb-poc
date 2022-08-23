class BaseConfig:
    """Base configuration class for methods to inherit. Assumes name of config is
    organized like {name}__config_key
    """

    def __init__(self, name, config):
        self.config = config
        self.name = name

    def _generate_cfg_dict(self):
        cfg = self.__dict__.copy()
        del cfg["config"]
        del cfg["name"]

        return cfg

    def clean_cfg(self):
        return self._clean_cfg()

    def _clean_cfg(self):
        cfg = self._generate_cfg_dict()
        cfg = {k[len(self.name) + 2 :]: v for k, v in cfg.items()}

        return cfg
