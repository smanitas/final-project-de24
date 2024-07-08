CREATE TABLE IF NOT EXISTS stg_chembl_id_lookup (
	chembl_id VARCHAR(20) PRIMARY KEY,
	entity_type VARCHAR(50) NOT NULL,
	status VARCHAR(10) NOT NULL,
	last_active INT4 NULL,
	resource_url VARCHAR(1000) NULL
);

CREATE TABLE IF NOT EXISTS stg_molecule_dictionary (
	chembl_id VARCHAR(20) PRIMARY KEY,
	molecule_type VARCHAR(30) NULL
);

CREATE TABLE IF NOT EXISTS stg_compound_properties (
	chembl_id VARCHAR(20) PRIMARY KEY,
	mw_freebase NUMERIC(9, 2) NULL,
	alogp NUMERIC(9, 2) NULL,
	psa NUMERIC(9, 2) NULL,
	cx_logp NUMERIC(9, 2) NULL,
	molecular_species VARCHAR(50) NULL,
	full_mwt NUMERIC(9, 2) NULL,
	aromatic_rings INT4 NULL,
	heavy_atoms INT4 NULL
);

CREATE TABLE IF NOT EXISTS stg_compound_structures (
	chembl_id VARCHAR(20) PRIMARY KEY,
	canonical_smiles VARCHAR(4000) NULL
);

