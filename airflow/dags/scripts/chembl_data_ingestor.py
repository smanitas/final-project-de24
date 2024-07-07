import asyncio
import gc
import logging
from typing import List
from typing import Type

import aiohttp
import pandas as pd
from pydantic import ValidationError
from sqlmodel import SQLModel
from sqlmodel import text
from tqdm.asyncio import tqdm

from config import CONFIG
from db import Database
from models import ChemblIdLookup
from models import CompoundProperties
from models import CompoundStructures
from models import Molecule
from models import MoleculeDictionary

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')


class ChemblDataIngestor:
    def __init__(self):
        self.api_config = CONFIG.get_api_config()
        self.semaphore = asyncio.Semaphore(self.api_config['concurrent_requests'])
        self.database = Database()
        self.engine = self.database.engine
        self.model_mapping = CONFIG.get_model_mapping()

    async def load_and_validate_data(self, session, url, json, model):
        retries = self.api_config['retries']
        delay = self.api_config['delay']

        async with self.semaphore:
            for attempt in range(retries):
                try:
                    async with session.get(url) as response:
                        response.raise_for_status()
                        data = await response.json()
                        validated_data = [model(**item) for item in data[json]]
                        logging.info(f"Validated {len(validated_data)} records from URL: {url}")
                        return validated_data, data['page_meta']['total_count']
                except (aiohttp.ClientError, ValidationError) as e:
                    logging.error(f"Load attempt {attempt + 1} failed for URL: {url} with error: {e}")
                    if attempt < retries - 1:
                        await asyncio.sleep(delay)
                    else:
                        raise

    async def load_all_data(self, file: str, params: str, json: str, model: Type[SQLModel]):
        logging.info(f"Starting to load all data for {model.__name__}")
        async with aiohttp.ClientSession() as session:
            try:
                init_url = f"{self.api_config['base_url']}/{file}{self.api_config['page_params'].format(0)}{params}"
                initial_data, total_records = await self.load_and_validate_data(session=session, url=init_url, json=json, model=model)
                all_data = initial_data

                batch_size = 1000
                tasks = []

                for i in range(batch_size, total_records, batch_size):
                    url = f"{self.api_config['base_url']}/{file}{self.api_config['page_params'].format(i)}{params}"
                    tasks.append(self.load_and_validate_data(session=session, url=url, json=json, model=model))

                    if len(tasks) == self.api_config['concurrent_requests'] or i + batch_size >= total_records:
                        chunk = tasks[:self.api_config['concurrent_requests']]
                        with tqdm(total=len(chunk), desc=f"Loading and validating data batches for {model.__name__}") as pbar:
                            for task in asyncio.as_completed(chunk):
                                try:
                                    result, _ = await task
                                    all_data.extend(result)
                                    pbar.update(1)
                                except Exception as e:
                                    logging.error(f"Error loading and validating data: {e}")

                        await self.process_and_insert_data(all_data, model)
                        all_data = []
                        tasks = []

                if all_data:
                    await self.process_and_insert_data(all_data, model)
            except Exception as e:
                logging.error(f"An error occurred while loading data for {model.__name__}: {e}")
            finally:
                await session.close()

    async def process_and_insert_data(self, data: List[SQLModel], model: Type[SQLModel]):
        if not data:
            return

        logging.info(f"Starting data processing and insertion for {model.__tablename__}")

        try:
            if model == Molecule:
                molecule_dicts = []
                compound_properties = []
                compound_structures = []

                async def process_item(item):
                    molecule_dict, properties, structures = item.to_models()
                    molecule_dicts.append(molecule_dict.model_dump())
                    if properties:
                        compound_properties.append(properties.model_dump())
                    if structures:
                        compound_structures.append(structures.model_dump())

                tasks = [process_item(item) for item in data]
                await asyncio.gather(*tasks)

                await self.insert_individual_data(molecule_dicts, MoleculeDictionary)
                await self.insert_individual_data(compound_properties, CompoundProperties)
                await self.insert_individual_data(compound_structures, CompoundStructures)
            else:
                await self.insert_individual_data([item.model_dump() for item in data], model)
        except Exception as e:
            logging.error(f"An error occurred during data processing and insertion for {model.__tablename__}: {e}")
        finally:
            del data
            gc.collect()

    async def insert_individual_data(self, data: List[dict], model: Type[SQLModel]):
        if not data:
            return

        logging.info(f"Inserting data into {model.__tablename__}")

        try:
            df = pd.DataFrame(data)
            logging.info(f"Data converted to DataFrame for {model.__tablename__}")

            df.to_sql(f"{model.__tablename__}", con=self.engine, if_exists='append', index=False)
            logging.info(f"Data successfully inserted into the database for {model.__tablename__}")
        except Exception as e:
            logging.error(f"An error occurred while inserting data into {model.__tablename__}: {e}")
        finally:
            del df
            del data
            gc.collect()

    async def truncate_table(self, model: Type[SQLModel]):
        logging.info(f"Truncating table {model.__tablename__}")
        with self.engine.begin() as conn:
            try:
                conn.execute(text(f"TRUNCATE TABLE {model.__tablename__} CASCADE"))
            except Exception as e:
                logging.error(f"An error occurred while truncating table {model.__tablename__}: {e}")

    async def run(self):
        logging.info("Starting the main function")

        try:
            await self.truncate_table(ChemblIdLookup)
            await self.truncate_table(MoleculeDictionary)
            await self.truncate_table(CompoundProperties)
            await self.truncate_table(CompoundStructures)

            for key, value in CONFIG.get_chembl_config().items():
                file = value['file']
                params = value['params']
                json = value['json']
                model_name = self.model_mapping[key]
                model = globals()[model_name]
                await self.load_all_data(file, params, json, model)
        except Exception as e:
            logging.error(f"An error occurred in the run method: {e}")
        finally:
            logging.info("Finished the main function")


if __name__ == "__main__":
    try:
        ingestor = ChemblDataIngestor()
        asyncio.run(ingestor.run())
    except RuntimeError as e:
        if str(e) != "Event loop is closed":
            raise
