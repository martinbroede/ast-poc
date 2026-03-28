# AST POC

A proof-of-concept for evaluating logical expressions with three-valued logic using the built-in Python AST module.

- `true`
- `false`
- `unknown`

The project parses expressions into Python built-in AST nodes and evaluates them with custom `TriState` semantics.

## Input syntax

Supported tokens:

- Variables: `A`, `my_var`, ...
- Operators: `&`, `|`
- Parentheses: `(` and `)`

## Output

`parse_expr` returns a validated Python AST expression node (`ast.expr`).

Typical node kinds are:

- `ast.Name`
- `ast.BinOp` with `ast.BitAnd` or `ast.BitOr` treated as TriState AND/OR

## Validation rules

The parser rejects unsupported syntax and operators.

Examples of rejected inputs:

- Empty expressions
- Incomplete expressions (for example `A &`)
- Mismatched parentheses
- Unexpected tokens
- Unsupported operators

On invalid input, `parse_expr` raises `ValueError`.

## Evaluation Contract

`evaluate(expr, context)` evaluates an AST expression using `TriState` values from context.

- Missing variables cause an exception.
- `&` uses `TriState.__and__`
- `|` uses `TriState.__or__`

## Running tests

```bash
cd src
python -m pytest -q test.py
```

## Running tests with coverage

```bash
cd src
python -m pytest -q --cov=core test.py
# To generate an HTML report:
python -m pytest -q --cov=core --cov-report=html test.py
```