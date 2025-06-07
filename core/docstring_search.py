import importlib
import inspect
from typing import List, Dict


def collect_docstrings(module_names: List[str]) -> Dict[str, str]:
    """Collect docstrings from modules, classes and functions."""
    docs: Dict[str, str] = {}
    for name in module_names:
        try:
            module = importlib.import_module(name)
        except Exception:
            # Skip modules that fail to import
            continue

        module_doc = inspect.getdoc(module)
        if module_doc:
            docs[name] = module_doc

        for member_name, member in inspect.getmembers(module):
            if inspect.isfunction(member) or inspect.isclass(member):
                doc = inspect.getdoc(member)
                if doc:
                    docs[f"{name}.{member_name}"] = doc
            if inspect.isclass(member):
                for attr_name, attr in inspect.getmembers(member):
                    if inspect.isfunction(attr) or inspect.ismethod(attr):
                        doc = inspect.getdoc(attr)
                        if doc:
                            docs[f"{name}.{member_name}.{attr_name}"] = doc
    return docs


def search_docstrings(query: str, module_names: List[str]) -> Dict[str, str]:
    """Return docstrings containing the query string (case-insensitive)."""
    all_docs = collect_docstrings(module_names)
    query_lower = query.lower()
    return {
        name: doc
        for name, doc in all_docs.items()
        if query_lower in doc.lower()
    }
