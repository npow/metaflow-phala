# Metaflow plugin registration.
# Metaflow discovers these descriptors at import time via the
# metaflow_extensions namespace package convention.

STEP_DECORATORS_DESC = [
    ("phala", ".phala_decorator.PhalaDecorator"),
]

CLIS_DESC = [
    ("phala", ".phala_cli.cli"),
]
