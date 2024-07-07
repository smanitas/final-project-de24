import asyncio
from chembl_data_ingestor import ChemblDataIngestor


async def main():
    ingestor = ChemblDataIngestor()
    await ingestor.run()


if __name__ == "__main__":
    asyncio.run(main())
