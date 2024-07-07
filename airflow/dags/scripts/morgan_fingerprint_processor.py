import gc
import logging
import os
from multiprocessing import Pool
from multiprocessing import cpu_count

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlmodel import Session
from sqlmodel import select

from aws import AWS
from config import CONFIG
from db import Database
from models import CompoundStructures
from morgan_fingerprint_calculator import MorganFingerprintCalculator


class MorganFingerprintProcessor:
    def __init__(self):
        self.database = Database()
        self.engine = self.database.engine
        self.aws = AWS()
        self.fingerprints_config = CONFIG.get_fingerprint_similarity_config()
        self.bucket_name = self.fingerprints_config['bucket_name']
        self.fingerprints_prefix = self.fingerprints_config['fingerprints']['fingerprints_prefix']
        self.chunk_size = self.fingerprints_config['fingerprints']['chunk_size']

    def compute_and_store_fingerprints(self):
        try:
            logging.info('Connecting to the database...')
            with Session(self.engine) as session:
                logging.info('Fetching data from stg_compound_structures table...')
                statement = select(CompoundStructures).limit(400000)
                results = session.exec(statement).all()

                if not results:
                    logging.error("No data fetched from the database.")
                    return

                # Convert SQLModel objects to dictionaries
                data = [row.model_dump() for row in results]

                df = pd.DataFrame(data)
                logging.info(f'Total records read: {len(df)}')

                num_batches = (len(df) + self.chunk_size - 1) // self.chunk_size
                df_splits = [df[i * self.chunk_size:(i + 1) * self.chunk_size] for i in range(num_batches)]

                with Pool(cpu_count() // 2) as pool:
                    for batch_num, df_part in enumerate(
                            pool.imap(MorganFingerprintCalculator.process_fingerprints, df_splits)):
                        logging.info(f'Processing and saving batch {batch_num}...')

                        # Save to S3
                        try:
                            table = pa.Table.from_pandas(df_part)
                            file_name = f'compound_fingerprints_{batch_num}.parquet'
                            pq.write_table(table, file_name, compression='zstd')

                            s3_path = f'{self.fingerprints_prefix}compound_fingerprints_{batch_num}.parquet'
                            self.aws.boto_client.upload_file(file_name, self.bucket_name, s3_path)
                            logging.info(f'File uploaded to S3: {s3_path}')

                            os.remove(file_name)
                            del df_part
                            del table
                        except Exception as e:
                            logging.error(f'An error occurred while saving batch {batch_num}: {e}')
        except Exception as e:
            logging.error(f"An error occurred while computing and storing fingerprints: {e}")
        finally:
            gc.collect()
