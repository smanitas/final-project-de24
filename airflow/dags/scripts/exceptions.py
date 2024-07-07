# Custom exceptions for SMILES validation
class SMILESParsingError(Exception):
    """Base exception for SMILES parsing errors"""
    pass


class EmptySMILESError(SMILESParsingError):
    """Raised for empty SMILES strings"""
    pass


class InvalidSMILESError(SMILESParsingError):
    """Raised for invalid SMILES strings"""
    pass
