from morgan_fingerprint_processor import FingerprintProcessor
from tanimoto_similarity_processor import SimilarityProcessor
from config import engine, s3, BUCKET_NAME, INPUT_PREFIX, FINGERPRINTS_PREFIX, SIMILARITIES_PREFIX


def main():
    fingerprint_processor = FingerprintProcessor(engine, s3, BUCKET_NAME, INPUT_PREFIX, FINGERPRINTS_PREFIX)
    fingerprint_processor.compute_and_store_fingerprints()

    similarity_processor = SimilarityProcessor(engine, s3, BUCKET_NAME, INPUT_PREFIX, SIMILARITIES_PREFIX,
                                               FINGERPRINTS_PREFIX)
    similarity_processor.compute_and_store_similarity()


if __name__ == '__main__':
    main()
