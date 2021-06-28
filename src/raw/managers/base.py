from abc import abstractmethod
import json
import os

from src.utils.logger import BaseModuleWithLogging


class BaseManagerModule(BaseModuleWithLogging):

    def __init__(self, name: str, manifest_file: str):
        super().__init__(name)

        # extract manifest file
        self.manifest_file = manifest_file
        if os.path.exists(manifest_file): 
            f = open(manifest_file, 'r')
            self.manifest = json.load(f)
            f.close()
        else:
            self.manifest = None

    @abstractmethod
    def update(self) -> None:
        raise NotImplemented

    def _save_manifest(self, manifest) -> None:
        f = open(self.manifest_file, 'w+')
        json.dump(manifest, f)
        f.close()