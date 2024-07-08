CREATE TABLE IF NOT EXISTS dim_molecules (
    chembl_id VARCHAR(20) PRIMARY KEY,
    molecule_type VARCHAR(30),
    mw_freebase NUMERIC(9, 2),
    alogp NUMERIC(9, 2),
    psa NUMERIC(9, 2),
    cx_logp NUMERIC(9, 2),
    molecular_species VARCHAR(50),
    full_mwt NUMERIC(9, 2),
    aromatic_rings INT4,
    heavy_atoms INT4
);

CREATE TABLE IF NOT EXISTS fact_molecule_similarities (
    source_chembl_id VARCHAR(20) REFERENCES dim_molecules(chembl_id),
    target_chembl_id VARCHAR(20) REFERENCES dim_molecules(chembl_id),
    tanimoto_similarity_score NUMERIC(9, 6),
    has_duplicates_of_last_largest_score BOOLEAN,
    PRIMARY KEY (source_chembl_id, target_chembl_id)
);

CREATE OR REPLACE VIEW avg_similarity_per_source AS
SELECT
    fms.source_chembl_id,
    AVG(fms.tanimoto_similarity_score) AS avg_similarity_score
FROM
    fact_molecule_similarities fms
GROUP BY
    fms.source_chembl_id;

CREATE OR REPLACE VIEW avg_alogp_deviation AS
SELECT
    fms.source_chembl_id,
    AVG(ABS(dm1.alogp - dm2.alogp)) AS avg_alogp_deviation
FROM
    fact_molecule_similarities fms
JOIN
    dim_molecules dm1 ON fms.source_chembl_id = dm1.chembl_id
JOIN
    dim_molecules dm2 ON fms.target_chembl_id = dm2.chembl_id
GROUP BY
    fms.source_chembl_id;

CREATE OR REPLACE VIEW next_most_similar AS
WITH ranked_similarities AS (
    SELECT
        fms.source_chembl_id,
        fms.target_chembl_id,
        fms.tanimoto_similarity_score,
        LEAD(fms.target_chembl_id, 1) OVER (PARTITION BY fms.source_chembl_id ORDER BY fms.tanimoto_similarity_score DESC) AS next_most_similar,
        LEAD(fms.target_chembl_id, 2) OVER (PARTITION BY fms.source_chembl_id ORDER BY fms.tanimoto_similarity_score DESC) AS second_most_similar
    FROM
        fact_molecule_similarities fms
)
SELECT
    source_chembl_id,
    target_chembl_id,
    tanimoto_similarity_score,
    next_most_similar,
    second_most_similar
FROM
    ranked_similarities;

CREATE OR REPLACE VIEW avg_similarity_score_categories AS
SELECT
    CASE
        WHEN GROUPING(fms.source_chembl_id) = 1 AND GROUPING(dm.aromatic_rings) = 1 AND GROUPING(dm.heavy_atoms) = 1 THEN 'TOTAL'
        ELSE fms.source_chembl_id
    END AS source_chembl_id,
    CASE
        WHEN GROUPING(fms.source_chembl_id) = 1 AND GROUPING(dm.aromatic_rings) = 1 AND GROUPING(dm.heavy_atoms) = 1 THEN 'TOTAL'
        ELSE CAST(dm.aromatic_rings AS VARCHAR)
    END AS aromatic_rings,
    CASE
        WHEN GROUPING(fms.source_chembl_id) = 1 AND GROUPING(dm.aromatic_rings) = 1 AND GROUPING(dm.heavy_atoms) = 1 THEN 'TOTAL'
        ELSE CAST(dm.heavy_atoms AS VARCHAR)
    END AS heavy_atoms,
    AVG(fms.tanimoto_similarity_score) AS avg_similarity_score
FROM
    fact_molecule_similarities fms
JOIN
    dim_molecules dm ON fms.source_chembl_id = dm.chembl_id
GROUP BY
    GROUPING SETS (
        (fms.source_chembl_id),
        (dm.aromatic_rings, dm.heavy_atoms),
        (dm.heavy_atoms),
        ()
    );
