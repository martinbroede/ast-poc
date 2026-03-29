# AST POC

A proof-of-concept for evaluating logical expressions with three-valued logic using the built-in Python AST module.

The `TriState` class represents three logical states:

- `true`
- `false`
- `unknown`

Relations between those states are described in detail in
[this Wikipedia article](https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_and_Priest_logics)

The project parses expressions into Python built-in AST nodes and evaluates them with [these](src/core/logic.py)
`TriState` semantics.

## Input syntax

Supported tokens:

- Variables: `A`, `my_var`, ...
- Operators: `&`, `|`, `~`
- Parentheses: `(` and `)`

## Output

`parse_expr` returns a validated Python AST expression node (`ast.expr`).

Supported AST node types:

- `ast.Name`
- `ast.BinOp` with `ast.BitAnd` or `ast.BitOr` treated as TriState AND/OR
- `ast.UnaryOp` with `ast.Invert` treated as TriState NOT

## Validation rules

The parser rejects unsupported syntax and operators.

Examples of rejected inputs:

- Empty expressions
- Incomplete expressions (for example `A &`)
- Mismatched parentheses
- Unexpected tokens
- Unsupported operators

On invalid input, `parse_expr` raises `ASTError`, or, more specifically, `ASTParsingError` or `ASTValidationError`.

## Evaluation Contract

`evaluate(expr, context)` evaluates an AST expression using `TriState` values from context.

- Missing variables cause an exception.
- `&` uses `TriState.__and__`
- `|` uses `TriState.__or__`
- `~` uses `TriState.__invert__`

On evaluation errors, `evaluate` raises `ASTEvaluationError`, e.g.
when a variable absent from context is accessed.

## Install required packages

```bash
pip install -r requirements.txt
```

> [!NOTE]
> Please use a virtual environment to avoid conflicts with system packages.

## Run the main script

```bash
python src/main.py
```


## Run tests

```bash
cd src
pytest test*.py
```

## Run tests with coverage

```bash
cd src
python -m pytest -q --cov=core test*.py
# To generate an HTML report:
python -m pytest -q --cov=core --cov-report=html test*.py
```

## Cleaning up generated files

```bash
cd src
find . -type f -name "*.pyc" -delete
find . -type d -name "__pycache__" -delete
find . -type d -name "htmlcov" -exec rm -rf {} +
find . -type f -name ".coverage" -delete
```

## Example output

...using the visualizer to print a readable tree structure:

```
Expression: ((A | (B & (C | ~D))) & ((E | F) & (G | (H & I))))

────────────────────────────────────────────────────────────────────────────────

AST without context:

            [AND]
              │
   ┌──────────┴───────────┐
 [OR]                   [AND]
   │                      │
┌──┴───┐              ┌───┴────┐
A    [AND]          [OR]     [OR]
       │              │        │
    ┌──┴───┐        ┌─┴─┐   ┌──┴──┐
    B    [OR]       E   F   G   [AND]
           │                      │
        ┌──┴──┐                 ┌─┴─┐
        C   [NOT]               H   I
              │
              │
              D

────────────────────────────────────────────────────────────────────────────────

AST with context:

                           [AND]
                             │
          ┌──────────────────┴──────────────────┐
        [OR]                                  [AND]
          │                                     │
   ┌──────┴──────┐                     ┌────────┴─────────┐
UNKNOWN        [AND]                 [OR]               [OR]
                 │                     │                  │
            ┌────┴─────┐           ┌───┴───┐        ┌─────┴──────┐
          TRUE       [OR]        TRUE   UNKNOWN   FALSE        [AND]
                       │                                         │
                   ┌───┴───┐                                 ┌───┴────┐
                 FALSE   [NOT]                            UNKNOWN   TRUE
                           │
                           │
                         TRUE

────────────────────────────────────────────────────────────────────────────────

Result: UNKNOWN
```