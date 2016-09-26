import ast
from raco.expression import NamedAttributeRef, UnnamedAttributeRef, StringLiteral, EQ

class ExpressionVisitor(ast.NodeVisitor):
    def __init__(self, schema):
        self.arity = len(schema)
        self.schema = schema
        self.names = None
        self.expression = []

    def visit_arguments(self, node):
        assert len(node.args) == self.arity
        self.names = [n.id for n in node.args]

    def visit_Compare(self, node):
        if len(node.ops) == 1 and isinstance(node.ops[0], ast.Eq) and len(node.comparators) == 1:
            left = self.visit(node.left)
            right = self.visit(node.comparators[0])
            self.expression = EQ(left, right)
            return self.expression

    def visit_Attribute(self, node):
        scheme = self.schema[self.names.index(node.value.id)]
        assert node.attr in [s[0] for s in scheme]
        self.expression = NamedAttributeRef(node.attr)
        return self.expression

    def visit_Subscript(self, node):
        offset = sum([len(s) for s in self.schema[:self.names.index(node.value.id)]])
        assert node.slice.value.n < len(self.schema[self.names.index(node.value.id)])
        self.expression = UnnamedAttributeRef(node.slice.value.n + offset)
        return self.expression

    def visit_Str(self, node):
        return StringLiteral(node.s)

    def generic_visit(self, node):
        ast.NodeVisitor.generic_visit(self, node)
        return self.expression


def convert(source, schema):
    node = ast.parse(source)
    expression = ExpressionVisitor(schema).visit(node)
    assert expression
    return expression
