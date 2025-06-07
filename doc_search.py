#!/usr/bin/env python3
"""Simple CLI to search docstrings for a query."""
import argparse
from core.docstring_search import search_docstrings


def main() -> None:
    parser = argparse.ArgumentParser(description="Search docstrings")
    parser.add_argument("query", help="Text to search for")
    parser.add_argument(
        "modules",
        nargs="*",
        default=["core"],
        help="Modules to search (default: core)",
    )
    args = parser.parse_args()

    results = search_docstrings(args.query, args.modules)
    if not results:
        print("No matches found.")
        return

    for name, doc in results.items():
        print(f"{name}\n{doc}\n{'-' * 40}")


if __name__ == "__main__":
    main()
