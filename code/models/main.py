import argparse
import logging
import os
from typing import Any

import pandas as pd
import duckdb

from abstract import BaseMlService


class CustomerProfilingProcessor(BaseMlService):
    pass


class CustomerSegmentationProcessor(BaseMlService):
    pass


# Main Processor
class MlProcessor:
    pass


def main():
    parser = argparse.ArgumentParser(
        prog="dynamic-customer-segmentation",
        description="Machine learning services for dynamic customer segmentation"
        "utilizing Kmeans, LightGBM, and permutation feature importance.",
    )
    parser.add_argument(
        "--full-flag", "-f", type=str, help="flag description", required=True
    )
    parser.add_argument(
        "--short-flag", "-s", type=str, help="flag description", required=True
    )

    args, unknown_args = parser.parse_known_args()
    # args = parser.parse_args()

    if len(unknown_args) != 0:
        logging.warning(f"Unknow arguments: {unknown_args}")

    # initiate instances and processor
    # print(f'{args.full_flag}_{args.short_flag}')


if __name__ == "__main__":
    main()
