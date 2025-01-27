import gc
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlmodel import Session
from sqlmodel import select

from aws import AWS
from config import CONFIG
from db import Database
from models import (
    CompoundProperties,
    DimMolecules,
    FactMoleculeSimilarities,
    MoleculeDictionary
)
from tanimoto_similarity_calculator import TanimotoSimilarityCalculator

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')


class TanimotoSimilarityProcessor:
    def __init__(self):
        db = Database()
        aws = AWS()
        self.engine = db.engine
        self.s3 = aws.boto_client
        config = CONFIG.get_fingerprint_similarity_config()
        self.bucket_name = config['bucket_name']
        self.input_prefix = config['input_prefix']
        self.similarities_prefix = config['similarities']['similarities_prefix']
        self.fingerprints_prefix = config['fingerprints']['fingerprints_prefix']

    def compute_and_store_similarity(self, file_key):
        try:
            logging.info(f'Processing file {file_key}')
            try:
                file_obj = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)
                df = pd.read_csv(file_obj['Body'], encoding='utf-8', on_bad_lines='skip')
                df.columns = [col.lower() for col in df.columns]
            except (pd.errors.ParserError, UnicodeDecodeError) as e:
                logging.error(f"Error reading file {file_key}: {e}")
                return

            df = df[df['molecule name'].apply(lambda x: x.startswith('CHEMBL'))]

            parquet_files = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=self.fingerprints_prefix).get(
                'Contents', [])
            parquet_files = [file['Key'] for file in parquet_files if file['Key'].endswith('.parquet')]

            combined_results = []
            num_cores = 4

            with ThreadPoolExecutor(max_workers=num_cores) as executor:
                futures = [executor.submit(TanimotoSimilarityCalculator.process_tanimoto_similarity,
                                           (pf, df, self.bucket_name)) for pf in parquet_files]

                for future in as_completed(futures):
                    try:
                        batch_result = future.result()
                        combined_results.append(batch_result)
                    except Exception as e:
                        logging.error(f"Error processing batch: {e}")

            combined_results = pd.concat(combined_results, ignore_index=True)
            logging.info(f'Number of records in combined results: {len(combined_results)}')

            if 'target_chembl_id' in combined_results.columns:
                top_10_df_union = pd.DataFrame()
                for molecule_name, group in combined_results.groupby('target_chembl_id'):
                    logging.info(f'Processing and saving molecule {molecule_name}')
                    output_file_name = f"similarity_{molecule_name}.parquet"
                    output_file_path = f'{self.similarities_prefix}{output_file_name}'
                    pq.write_table(pa.Table.from_pandas(group), output_file_name, compression='zstd')
                    self.s3.upload_file(output_file_name, self.bucket_name, output_file_path)
                    logging.info(f'File uploaded to S3: {output_file_path}')
                    os.remove(output_file_name)

                    top_10_df = group.nlargest(10, 'tanimoto_similarity_score').reset_index(drop=True)
                    top_10_df['has_duplicates_of_last_largest_score'] = top_10_df.duplicated(
                        subset=['tanimoto_similarity_score'], keep=False)
                    top_10_df.rename(columns={'chembl_id': 'source_chembl_id'}, inplace=True)
                    top_10_df_union = pd.concat([top_10_df_union, top_10_df])

                top_10_df_union.drop_duplicates(inplace=True)
                self.insert_to_data_mart(top_10_df_union)
            else:
                logging.warning(f"'target_chembl_id' column missing in combined results for file {file_key}.")

            logging.info(f'File {file_key} processed.')
            logging.info('All data processed and saved.')
        except Exception as e:
            logging.error(f"An error occurred during the similarity computation: {e}")
        finally:
            gc.collect()

    def insert_to_data_mart(self, top_10_df):
        try:
            with Session(self.engine) as session:
                logging.info("Fetching existing chembl_id values from fact_molecule_similarities table...")
                existing_chembl_ids = session.exec(select(DimMolecules.chembl_id)).all()
                existing_chembl_ids_set = {row for row in existing_chembl_ids}

                logging.info("Filtering top_10_df to exclude existing chembl_id")
                top_10_df = top_10_df[~top_10_df['source_chembl_id'].isin(existing_chembl_ids_set)]

                logging.info("Inserting data into dim_molecules table")
                unique_source_chembl_id = set(top_10_df['source_chembl_id'])
                unique_target_chembl_id = set(top_10_df['target_chembl_id'])
                unique_chembl_id = unique_source_chembl_id.union(unique_target_chembl_id)

                stmt = (
                    select(
                        MoleculeDictionary.chembl_id,
                        MoleculeDictionary.molecule_type,
                        CompoundProperties.mw_freebase,
                        CompoundProperties.alogp,
                        CompoundProperties.psa,
                        CompoundProperties.cx_logp,
                        CompoundProperties.molecular_species,
                        CompoundProperties.full_mwt,
                        CompoundProperties.aromatic_rings,
                        CompoundProperties.heavy_atoms
                    )
                    .where(MoleculeDictionary.chembl_id.in_(unique_chembl_id))
                    .where(~MoleculeDictionary.chembl_id.in_(existing_chembl_ids_set))
                    .join(CompoundProperties, CompoundProperties.chembl_id == MoleculeDictionary.chembl_id)
                )
                results = session.exec(stmt).all()

                dim_molecules_df = pd.DataFrame([row._asdict() for row in results], columns=[
                    'chembl_id', 'molecule_type', 'mw_freebase', 'alogp', 'psa', 'cx_logp',
                    'molecular_species', 'full_mwt', 'aromatic_rings', 'heavy_atoms'
                ])
                dim_molecules_df.drop_duplicates(subset=['chembl_id'], inplace=True)
                dim_molecules_df.to_sql(DimMolecules.__tablename__, con=self.engine, if_exists='append', index=False)

                logging.info("Inserting data into fact_molecule_similarities table")
                top_10_df.to_sql(FactMoleculeSimilarities.__tablename__, con=self.engine, if_exists='append',
                                 index=False)

                logging.info("Data successfully inserted into dim_molecules and fact_molecule_similarities tables.")
        except Exception as e:
            logging.error(f"An error occurred during insertion: {e}")
        finally:
            gc.collect()
