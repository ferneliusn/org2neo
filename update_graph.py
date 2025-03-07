#!/usr/bin/env python3

import py_connector
import database_builder
import sys


def main():
    database_builder.main()
    py_connector.main()
if __name__ == "__main__":
    sys.exit(main())
