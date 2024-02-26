import configparser
import os


class CSConfig:
    def __init__(self, file_config: str, class_name: str, class_identity):
        self.file_config = file_config
        root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        conf_path = root_path + "/config/" + self.file_config + ".config"
        config = configparser.RawConfigParser()
        config.read(conf_path)
        self.parser = dict(config.items(class_name + "@" + class_identity))

    def read_parameter(self, params):
        return self.parser[params]


if __name__ == "__main__":
    print(CSConfig("production", "cryptoquant", "config").read_parameter("url_mpi"))

