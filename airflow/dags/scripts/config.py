import os
import yaml


class Config:
    def __init__(self, config_file=None):
        if config_file is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            config_file = os.path.join(base_dir, 'config.yaml')
        self.config = self.load_config(config_file)

    @staticmethod
    def load_config(config_file):
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
        return config

    def get_database_config(self):
        return self.config['database']

    def get_api_config(self):
        return self.config['api']

    def get_chembl_config(self):
        return self.config['chembl']

    def get_model_mapping(self):
        return self.config['model_mapping']

    def get_aws_config(self):
        return self.config['aws']

    def get_fingerprint_similarity_config(self):
        return self.config['fingerprint_similarity']


CONFIG = Config()
