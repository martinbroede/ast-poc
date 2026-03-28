import ast


def visualize_ast(expr: ast.expr, context: dict | None = None) -> str:
    """
    Visualize a Python AST expression as a top-down pipe-style tree.
    """

    def label(node: ast.AST) -> str:
        if isinstance(node, ast.BinOp):
            if isinstance(node.op, ast.BitAnd):
                return "[AND]"
            if isinstance(node.op, ast.BitOr):
                return "[OR]"
        if isinstance(node, ast.UnaryOp):
            if isinstance(node.op, ast.Invert):
                return "[NOT]"
        if isinstance(node, ast.Name):
            if not context:
                return node.id
            return context.get(node.id, f"<{node.id}>")
        return str(node)

    def children(node: ast.AST) -> list[ast.AST] | list[ast.expr]:
        if isinstance(node, ast.BinOp):
            return [node.left, node.right]
        if isinstance(node, ast.UnaryOp):
            return [node.operand]
        return []

    def render(node: ast.AST) -> tuple[list[str], int, int]:
        """
        Return (lines, width, root_x) where root_x is the column index
        of the node label anchor within the rendered block.
        """
        node_label = label(node)
        node_children = children(node)

        if not node_children:
            width = max(1, len(node_label))
            root_x = len(node_label) // 2
            return [node_label], width, root_x

        rendered_children = [render(child) for child in node_children]
        child_lines = [item[0] for item in rendered_children]
        child_widths = [item[1] for item in rendered_children]
        child_roots = [item[2] for item in rendered_children]

        gap = 3
        child_total_width = sum(child_widths) + gap * (len(child_widths) - 1)

        offsets: list[int] = []
        cursor = 0
        for width in child_widths:
            offsets.append(cursor)
            cursor += width + gap

        abs_child_roots = [offset + root for offset,
                           root in zip(offsets, child_roots)]

        parent_root = (abs_child_roots[0] + abs_child_roots[-1]) // 2
        label_start = parent_root - (len(node_label) // 2)
        label_end = label_start + len(node_label)

        left_pad = max(0, -label_start)
        right_pad = max(0, label_end - child_total_width)

        total_width = child_total_width + left_pad + right_pad
        parent_root += left_pad
        abs_child_roots = [root + left_pad for root in abs_child_roots]
        label_start += left_pad

        first_line_chars = [" "] * total_width
        for i, ch in enumerate(node_label):
            idx = label_start + i
            if 0 <= idx < total_width:
                first_line_chars[idx] = ch
        first_line = "".join(first_line_chars).rstrip()

        # Vertical drop from parent into the branch row.
        connector_top_chars = [" "] * total_width
        connector_top_chars[parent_root] = "│"
        connector_top = "".join(connector_top_chars).rstrip()

        # Horizontal branch row with pipe-like box drawing connectors.
        connector_branch_chars = [" "] * total_width
        if len(abs_child_roots) == 1:
            connector_branch_chars[abs_child_roots[0]] = "│"
        else:
            left_root = abs_child_roots[0]
            right_root = abs_child_roots[-1]
            for idx in range(left_root, right_root + 1):
                connector_branch_chars[idx] = "─"
            connector_branch_chars[left_root] = "┌"
            connector_branch_chars[right_root] = "┐"
            connector_branch_chars[parent_root] = "┴"
        connector_branch = "".join(connector_branch_chars).rstrip()

        max_child_height = max(len(lines) for lines in child_lines)
        padded_children: list[list[str]] = []
        for lines, width in zip(child_lines, child_widths):
            padded = [line.ljust(width) for line in lines]
            padded.extend([" " * width] * (max_child_height - len(lines)))
            padded_children.append(padded)

        merged_children: list[str] = []
        for row in range(max_child_height):
            row_parts = [padded_children[i][row]
                         for i in range(len(padded_children))]
            merged_children.append((" " * gap).join(row_parts).rstrip())

        return [first_line, connector_top, connector_branch, *merged_children], total_width, parent_root

    lines, _, _ = render(expr)
    return "\n".join(line for line in lines if line)