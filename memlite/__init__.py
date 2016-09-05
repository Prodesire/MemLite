"""MemLite"""
# this is a namespace package
import pkg_resources
from . import memlite
from .memlite import Base  # NOQA
pkg_resources.declare_namespace(__name__)
