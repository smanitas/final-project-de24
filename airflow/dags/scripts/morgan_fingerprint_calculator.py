import logging

import pandas as pd
from rdkit.Chem import AllChem
from rdkit.Chem import MolFromSmiles
from tqdm import tqdm

from config import CONFIG
from exceptions import EmptySMILESError
from exceptions import InvalidSMILESError
from exceptions import SMILESParsingError


class MorganFingerprintCalculator:
    config = CONFIG.get_fingerprint_similarity_config()
    FPS_BITS = config['fingerprints']['fps_bits']
    FPS_MOL_RADIUS = config['fingerprints']['fps_mol_radius']

    @staticmethod
    def validate_smiles(smiles):
        if pd.isna(smiles) or smiles.strip() == '':
            raise EmptySMILESError("Empty SMILES string!")
        mol = MolFromSmiles(smiles)
        if mol is None:
            raise InvalidSMILESError(f"Invalid SMILES string: {smiles}")
        return mol

    @classmethod
    def calculate_morgan_fingerprint(cls, smiles):
        try:
            mol = cls.validate_smiles(smiles)
            fps = AllChem.GetMorganFingerprintAsBitVect(mol, cls.FPS_MOL_RADIUS, nBits=cls.FPS_BITS)
            return fps.ToBitString()
        except (SMILESParsingError, InvalidSMILESError, EmptySMILESError) as e:
            logging.warning(f"Warning: {e}")
            return None

    @classmethod
    def process_fingerprints(cls, df_part):
        logging.info('Processing fingerprints...')
        if 'canonical_smiles' not in df_part.columns:
            logging.error("The 'canonical_smiles' column is missing from the DataFrame.")
            raise ValueError("The 'canonical_smiles' column is missing from the DataFrame.")

        try:
            smiles_list = df_part['canonical_smiles'].tolist()
            df_part['morgan_fingerprint'] = [cls.calculate_morgan_fingerprint(smiles) for smiles in tqdm(smiles_list)]
            df_filtered = df_part[df_part['morgan_fingerprint'].notnull()][['chembl_id', 'morgan_fingerprint']]
            return df_filtered
        except Exception as e:
            logging.error(f"An error occurred during fingerprint processing: {e}")
            raise
