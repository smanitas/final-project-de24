from tanimoto_similarity_processor import TanimotoSimilarityProcessor

if __name__ == "__main__":
    processor = TanimotoSimilarityProcessor()
    processor.compute_and_store_similarity("final_task/input_files/file.csv")
