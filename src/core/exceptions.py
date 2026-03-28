class ASTError(Exception):
    """Base class for exceptions related to AST parsing and evaluation."""

class ASTParsingError(ASTError):
    """Exception raised for errors during AST parsing."""

class ASTValidationError(ASTError):
    """Exception raised for errors during AST validation."""

class ASTEvaluationError(ASTError):
    """Exception raised for errors during AST evaluation."""