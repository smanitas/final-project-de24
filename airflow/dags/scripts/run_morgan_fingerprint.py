import logging
from morgan_fingerprint_processor import MorganFingerprintProcessor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')


def main():
    processor = MorganFingerprintProcessor()
    processor.compute_and_store_fingerprints()


if __name__ == "__main__":
    main()
