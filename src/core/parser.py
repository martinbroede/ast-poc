import ast

from core.exceptions import ASTParsingError, ASTValidationError


def __validate_expr(node: ast.AST) -> None:
    """
    Allows for a very limited subset of expressions, specifically:
    - Variables: A, my_var...
    - Operators: '&' as TriState AND, '|' as TriState OR, '~' as TriState NOT
    - Parentheses: (...)
    """
    if isinstance(node, ast.Name):
        return

    if isinstance(node, ast.BinOp):
        if not isinstance(node.op, (ast.BitAnd, ast.BitOr)):
            raise ASTValidationError(f"Unsupported binary operator: {type(node.op).__name__}")
        __validate_expr(node.left)
        __validate_expr(node.right)
        return

    if isinstance(node, ast.UnaryOp):
        if not isinstance(node.op, ast.Invert):
            raise ASTValidationError(f"Unsupported unary operator: {type(node.op).__name__}")
        __validate_expr(node.operand)
        return

    raise ASTValidationError(f"Unsupported syntax node: {type(node).__name__}")


def parse_expr(expr_str: str) -> ast.expr:
    """
    Parse an input expression into a validated built-in Python AST node.

    This is the single public parser entry point.

    Supported syntax:
    - Variables: A, my_var...
    - Operators: '&' as TriState AND, '|' as TriState OR
    - Parentheses: (...)

    Returns:
    - ast.expr (aliased as ast.expr)
    """
    if not expr_str.strip():
        raise ASTParsingError("Input expression cannot be empty or whitespace.")
    try:
        parsed = ast.parse(expr_str, mode="eval")
    except SyntaxError as e:
        raise ASTParsingError(f"Syntax error in expression: '{e.msg}'") from e
    except Exception as e:
        raise ASTParsingError(f"Unexpected error during parsing: {str(e)}") from e
    expr = parsed.body
    __validate_expr(expr)
    return expr
