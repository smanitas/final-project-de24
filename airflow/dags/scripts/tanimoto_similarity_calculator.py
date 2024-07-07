import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from rdkit.Chem import AllChem
from rdkit.DataStructs import CreateFromBitString
from rdkit.DataStructs import TanimotoSimilarity

from aws import AWS
from config import CONFIG
from exceptions import SMILESParsingError
from morgan_fingerprint_calculator import MorganFingerprintCalculator

aws = AWS()
s3 = aws.boto_client
config = CONFIG.get_fingerprint_similarity_config()
bucket_name = config['bucket_name']


class TanimotoSimilarityCalculator:

    @staticmethod
    def calculate_tanimoto_similarity(fps1, fps2):
        return TanimotoSimilarity(fps1, fps2)

    @classmethod
    def process_tanimoto_similarity(cls, args):
        parquet_file, target_df, bucket_name = args
        results = []

        try:
            logging.info(f'Reading fingerprint file {parquet_file}')
            fp_obj = s3.get_object(Bucket=bucket_name, Key=parquet_file)
            fp_df = pq.read_table(pa.BufferReader(fp_obj['Body'].read())).to_pandas()
            fp_df['morgan_fingerprint'] = fp_df['morgan_fingerprint'].apply(CreateFromBitString)
        except Exception as e:
            logging.error(f"Error reading parquet file {parquet_file}: {e}")
            return pd.DataFrame()

        for idx, row in target_df.iterrows():
            try:
                target_fps = AllChem.GetMorganFingerprintAsBitVect(
                    MorganFingerprintCalculator.validate_smiles(row['smiles']),
                    MorganFingerprintCalculator.FPS_MOL_RADIUS, nBits=MorganFingerprintCalculator.FPS_BITS)
                similarity_scores = fp_df.apply(
                    lambda x: cls.calculate_tanimoto_similarity(target_fps, x['morgan_fingerprint']), axis=1)
                temp_df = fp_df[['chembl_id']].copy()
                temp_df['tanimoto_similarity_score'] = similarity_scores
                temp_df['target_chembl_id'] = row['molecule name']
                results.append(temp_df)
            except SMILESParsingError as e:
                logging.warning(f"Error processing row {idx}: {e}")
                continue

        return pd.concat(results, ignore_index=True) if results else pd.DataFrame()
