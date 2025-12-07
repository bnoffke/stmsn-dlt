"""
File parsing utilities for Excel and CSV files.

Provides functions to read Excel and CSV files into pandas DataFrames
with automatic header detection and encoding handling.
"""

import re
import warnings
from pathlib import Path
from typing import Optional, Tuple, Union

import pandas as pd


class FileParseError(Exception):
    """Raised when file parsing fails."""

    pass


def find_header_row(df: pd.DataFrame, min_columns: int = 3) -> int:
    """
    Find the header row by identifying first row with sufficient non-empty cells.

    Args:
        df: DataFrame read without headers
        min_columns: Minimum number of non-empty cells to consider as header row

    Returns:
        Index of the header row

    Raises:
        FileParseError: If no suitable header row found
    """
    for i, row in df.iterrows():
        non_empty_cells = row.notna().sum()
        if non_empty_cells >= min_columns:
            return i
    raise FileParseError(
        f"Header row not found (no row with >={min_columns} non-empty cells)"
    )


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names to lowercase with underscores.

    Args:
        df: DataFrame with columns to normalize

    Returns:
        DataFrame with normalized column names
    """
    df.columns = [
        str(col).lower().strip().replace(" ", "_").replace("-", "_")
        for col in df.columns
    ]
    return df


def read_excel(
    file_path: Union[str, Path],
    sheet: Union[int, str] = 0,
    header_detection: str = "auto",
    normalize_columns: bool = True,
) -> Tuple[pd.DataFrame, int]:
    """
    Read an Excel file into a pandas DataFrame.

    Args:
        file_path: Path to the Excel file
        sheet: Sheet index (0-based) or name to read
        header_detection: "auto" to detect header row, or integer for explicit row
        normalize_columns: If True, normalize column names to lowercase with underscores

    Returns:
        Tuple of (DataFrame, row_count)

    Raises:
        FileParseError: If file cannot be parsed
    """
    file_path = Path(file_path)

    try:
        # Read all data as strings first (safest approach for mixed types)
        df = pd.read_excel(file_path, sheet_name=sheet, header=None, dtype=str)

        # Determine header row
        if header_detection == "auto":
            header_row = find_header_row(df)
        else:
            header_row = int(header_detection)

        # Set column names from header row
        df.columns = df.iloc[header_row]

        # Remove rows up to and including header
        df = df.iloc[header_row + 1 :]

        # Clean up
        df = df.replace({pd.NA: None})
        df = df.reset_index(drop=True)

        # Normalize column names if requested
        if normalize_columns:
            df = normalize_column_names(df)

        row_count = len(df)
        return df, row_count

    except FileParseError:
        raise
    except Exception as e:
        raise FileParseError(f"Failed to read Excel file {file_path}: {e}")


def _extract_problem_column(error_msg: str) -> Optional[str]:
    """Extract the column name from a parquet conversion error message."""
    match = re.search(r"Conversion failed for column (\w+)", error_msg)
    if match:
        return match.group(1)
    return None


def read_csv(
    file_path: Union[str, Path],
    separator: str = ",",
    normalize_columns: bool = True,
) -> Tuple[pd.DataFrame, int]:
    """
    Read a CSV file into a pandas DataFrame with encoding fallback.

    Args:
        file_path: Path to the CSV file
        separator: Column separator (default: comma)
        normalize_columns: If True, normalize column names to lowercase with underscores

    Returns:
        Tuple of (DataFrame, row_count)

    Raises:
        FileParseError: If file cannot be parsed with any encoding
    """
    file_path = Path(file_path)
    encodings = ["utf-8", "latin-1", "cp1252"]

    for encoding in encodings:
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)
                df = pd.read_csv(
                    file_path, sep=separator, encoding=encoding, low_memory=False
                )

            if normalize_columns:
                df = normalize_column_names(df)

            row_count = len(df)
            return df, row_count

        except UnicodeDecodeError:
            continue

        except Exception as e:
            # Try reading all columns as strings
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)
                    df = pd.read_csv(
                        file_path,
                        sep=separator,
                        encoding=encoding,
                        dtype=str,
                        low_memory=False,
                    )

                if normalize_columns:
                    df = normalize_column_names(df)

                row_count = len(df)
                return df, row_count

            except UnicodeDecodeError:
                continue

    raise FileParseError(f"Failed to read CSV file with encodings: {encodings}")


def read_file(
    file_path: Union[str, Path],
    file_type: Optional[str] = None,
    **kwargs,
) -> Tuple[pd.DataFrame, int]:
    """
    Read a file into a pandas DataFrame based on file type.

    Args:
        file_path: Path to the file
        file_type: File type (xlsx, xls, csv). If None, inferred from extension.
        **kwargs: Additional arguments passed to the specific reader

    Returns:
        Tuple of (DataFrame, row_count)

    Raises:
        FileParseError: If file type is unsupported or parsing fails
    """
    file_path = Path(file_path)

    # Infer file type from extension if not provided
    if file_type is None:
        ext = file_path.suffix.lower()
        file_type = ext.lstrip(".")

    file_type = file_type.lower()

    if file_type in ("xlsx", "xls"):
        # Extract Excel-specific kwargs
        sheet = kwargs.get("sheet", 0)
        header_detection = kwargs.get("header_detection", "auto")
        normalize_columns = kwargs.get("normalize_columns", True)
        return read_excel(file_path, sheet, header_detection, normalize_columns)

    elif file_type == "csv":
        separator = kwargs.get("separator", ",")
        normalize_columns = kwargs.get("normalize_columns", True)
        return read_csv(file_path, separator, normalize_columns)

    elif file_type == "tsv" or file_type == "txt":
        separator = kwargs.get("separator", "\t")
        normalize_columns = kwargs.get("normalize_columns", True)
        return read_csv(file_path, separator, normalize_columns)

    else:
        raise FileParseError(f"Unsupported file type: {file_type}")
