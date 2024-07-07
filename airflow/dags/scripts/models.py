from typing import Optional
from sqlmodel import SQLModel, Field


class ChemblIdLookup(SQLModel, table=True):
    __tablename__ = "stg_chembl_id_lookup"
    chembl_id: str = Field(primary_key=True, max_length=20)
    entity_type: str = Field(max_length=50)
    status: str = Field(max_length=10)
    last_active: Optional[int] = Field(default=None)
    resource_url: Optional[str] = Field(default=None, max_length=1000)


class MoleculeDictionary(SQLModel, table=True):
    __tablename__ = 'stg_molecule_dictionary'
    chembl_id: str = Field(primary_key=True, max_length=20)
    molecule_type: Optional[str] = Field(default=None, max_length=30)


class CompoundProperties(SQLModel, table=True):
    __tablename__ = 'stg_compound_properties'
    chembl_id: str = Field(primary_key=True, max_length=20)
    mw_freebase: Optional[float] = Field(default=None)
    alogp: Optional[float] = Field(default=None)
    psa: Optional[float] = Field(default=None)
    cx_logp: Optional[float] = Field(default=None)
    molecular_species: Optional[str] = Field(default=None, max_length=50)
    full_mwt: Optional[float] = Field(default=None)
    aromatic_rings: Optional[int] = Field(default=None)
    heavy_atoms: Optional[int] = Field(default=None)


class CompoundStructures(SQLModel, table=True):
    __tablename__ = 'stg_compound_structures'
    chembl_id: str = Field(primary_key=True, max_length=20)
    canonical_smiles: Optional[str] = Field(default=None, max_length=4000)


class Molecule(SQLModel):
    molecule_chembl_id: str
    molecule_type: Optional[str]
    molecule_properties: Optional[CompoundProperties]
    molecule_structures: Optional[CompoundStructures]

    def to_models(self):
        molecule_dict = MoleculeDictionary(
            chembl_id=self.molecule_chembl_id,
            molecule_type=self.molecule_type
        )

        compound_properties = None
        if self.molecule_properties:
            compound_properties = CompoundProperties(
                chembl_id=self.molecule_chembl_id,
                mw_freebase=float(
                    self.molecule_properties.mw_freebase) if self.molecule_properties.mw_freebase else None,
                alogp=float(self.molecule_properties.alogp) if self.molecule_properties.alogp else None,
                psa=float(self.molecule_properties.psa) if self.molecule_properties.psa else None,
                cx_logp=float(self.molecule_properties.cx_logp) if self.molecule_properties.cx_logp else None,
                molecular_species=self.molecule_properties.molecular_species,
                full_mwt=float(self.molecule_properties.full_mwt) if self.molecule_properties.full_mwt else None,
                aromatic_rings=self.molecule_properties.aromatic_rings,
                heavy_atoms=self.molecule_properties.heavy_atoms
            )

        compound_structures = None
        if self.molecule_structures:
            compound_structures = CompoundStructures(
                chembl_id=self.molecule_chembl_id,
                canonical_smiles=self.molecule_structures.canonical_smiles
            )

        return molecule_dict, compound_properties, compound_structures


class DimMolecules(SQLModel, table=True):
    __tablename__ = 'dim_molecules'
    chembl_id: str = Field(primary_key=True, max_length=20)
    molecule_type: Optional[str] = Field(default=None, max_length=30)
    mw_freebase: Optional[float] = Field(default=None)
    alogp: Optional[float] = Field(default=None)
    psa: Optional[float] = Field(default=None)
    cx_logp: Optional[float] = Field(default=None)
    molecular_species: Optional[str] = Field(default=None, max_length=50)
    full_mwt: Optional[float] = Field(default=None)
    aromatic_rings: Optional[int] = Field(default=None)
    heavy_atoms: Optional[int] = Field(default=None)


class FactMoleculeSimilarities(SQLModel, table=True):
    __tablename__ = 'fact_molecule_similarities'
    source_chembl_id: str = Field(foreign_key="dim_molecules.chembl_id", max_length=20, primary_key=True)
    target_chembl_id: str = Field(foreign_key="dim_molecules.chembl_id", max_length=20, primary_key=True)
    tanimoto_similarity_score: Optional[float] = Field(default=None)
    has_duplicates_of_last_largest_score: Optional[bool] = Field(default=None)
