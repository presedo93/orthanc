"""
Data Quality Module

This module provides tools for validating data quality before use in backtesting
and optimization. It checks for common issues like missing values, gaps, and
duplicates.
"""

from dataclasses import dataclass
from typing import cast

import pandas as pd


@dataclass
class DataQualityReport:
    """Report of data quality checks performed on input data."""

    is_valid: bool
    total_rows: int
    missing_values: dict[str, int]
    duplicate_timestamps: int
    gaps_detected: int
    gap_locations: list[pd.Timestamp]
    timestamp_range: tuple[pd.Timestamp, pd.Timestamp]
    price_range: tuple[float, float] | None
    issues: list[str]


class DataQualityChecker:
    """Validates data quality before WFO."""

    @classmethod
    def check(
        cls,
        data: pd.DataFrame,
        max_gap_ratio: float = 0.05,
        expected_freq: str | None = None,
    ) -> DataQualityReport:
        """
        Perform comprehensive data quality checks.

        Args:
            data: Input DataFrame to validate
            max_gap_ratio: Maximum allowed ratio of gaps to total data points
            expected_freq: Expected frequency of data (e.g., '1H', '1D')

        Returns:
            DataQualityReport with validation results
        """
        issues: list[str] = []
        has_symbol = 'symbol' in data.columns

        # Determine timestamp source
        timestamps: pd.Series | None = None
        if 'timestamp' in data.columns:
            timestamps = data['timestamp']  # type: ignore[assignment]
        elif isinstance(data.index, pd.DatetimeIndex):
            timestamps = data.index.to_series()

        if timestamps is None:
            issues.append(
                "Missing timestamp information (neither 'timestamp' column nor DatetimeIndex found)"
            )

        # Check for empty data
        total_rows = len(data)
        if total_rows == 0:
            return DataQualityReport(
                is_valid=False,
                total_rows=0,
                missing_values={},
                duplicate_timestamps=0,
                gaps_detected=0,
                gap_locations=[],
                timestamp_range=(pd.Timestamp.min, pd.Timestamp.max),
                price_range=None,
                issues=['Empty DataFrame provided'],
            )

        # Check for missing values
        missing_values = data.isnull().sum().to_dict()
        for col, count in missing_values.items():
            if count > 0 and count / total_rows > 0.01:  # More than 1% missing
                issues.append(
                    f"Column '{col}' has {count} ({count / total_rows:.1%}) missing values"
                )

        # Check for duplicate timestamps
        duplicates = 0
        if timestamps is not None:
            # Check duplicates per symbol if multi-asset
            if 'symbol' in data.columns:
                if 'timestamp' in data.columns:
                    duplicates = int(
                        cast(
                            int,
                            data
                            .groupby('symbol')['timestamp']
                            .apply(lambda x: x.duplicated().sum())
                            .sum(),
                        )
                    )
                else:
                    # Index case
                    duplicates = int(
                        cast(
                            int,
                            data
                            .groupby('symbol')
                            .apply(
                                lambda x: x.index.duplicated().sum(),
                                include_groups=False,
                            )
                            .sum(),
                        )
                    )
            else:
                duplicates = int(timestamps.duplicated().sum())

        if duplicates > 0:
            issues.append(f'Found {duplicates} duplicate timestamps')

        # Check for gaps in timestamps
        gap_locations: list[pd.Timestamp] = []
        gaps_detected = 0

        if timestamps is not None and expected_freq:
            expected_td = pd.Timedelta(expected_freq)
            threshold = expected_td * 1.5  # type: ignore[operator]

            if has_symbol:
                for _, group in data.groupby('symbol'):
                    if 'timestamp' in group.columns:
                        ts_series = group['timestamp'].sort_values(kind='mergesort')  # type: ignore[call-overload]
                    else:
                        ts_series = group.index.to_series().sort_values(
                            kind='mergesort'
                        )
                    time_diffs = ts_series.diff()
                    gaps_mask = time_diffs > threshold
                    gap_vals = ts_series[gaps_mask]
                    gaps_detected += len(gap_vals)
                    gap_locations.extend([pd.Timestamp(ts) for ts in gap_vals])  # type: ignore[arg-type]
            else:
                ts_sorted = timestamps.sort_values(kind='mergesort')  # type: ignore[arg-type]
                time_diffs = ts_sorted.diff()
                gaps_mask = time_diffs > threshold
                gap_vals = ts_sorted[gaps_mask]
                gaps_detected = len(gap_vals)
                gap_locations = [
                    pd.Timestamp(ts)  # type: ignore[arg-type]
                    for ts in gap_vals
                ]

            if gaps_detected / total_rows > max_gap_ratio:
                issues.append(
                    f'Too many gaps detected: {gaps_detected} ({gaps_detected / total_rows:.1%})'
                )

        # Get timestamp range
        if timestamps is not None:
            ts_min = timestamps.min()
            ts_max = timestamps.max()

            ts_range: tuple[pd.Timestamp, pd.Timestamp] = (
                pd.Timestamp(ts_min),  # type: ignore[arg-type]
                pd.Timestamp(ts_max),  # type: ignore[arg-type]
            )
        else:
            ts_range = (pd.Timestamp.min, pd.Timestamp.max)

        # Data is valid if there are no issues, or if the only issues are
        # minor missing values (< threshold). Structural issues like missing
        # timestamps, duplicates, or excessive gaps make data invalid.
        is_valid = len(issues) == 0 or all(
            'missing values' in issue.lower() for issue in issues
        )

        price_range: tuple[float, float] | None = None
        if 'close' in data.columns:
            close_min = float(cast(float, data['close'].min()))
            close_max = float(cast(float, data['close'].max()))
            if not (pd.isna(close_min) or pd.isna(close_max)):
                price_range = (close_min, close_max)

        return DataQualityReport(
            is_valid=is_valid,
            total_rows=total_rows,
            missing_values=missing_values,
            duplicate_timestamps=duplicates,
            gaps_detected=gaps_detected,
            gap_locations=gap_locations,
            timestamp_range=ts_range,
            price_range=price_range,
            issues=issues,
        )


def check_data_quality(
    data: pd.DataFrame,
    max_gap_ratio: float = 0.05,
    expected_freq: str | None = None,
) -> DataQualityReport:
    """
    Convenience function to check data quality for a DataFrame.

    Args:
        data: Input DataFrame to validate
        max_gap_ratio: Maximum allowed ratio of gaps to total data points
        expected_freq: Expected frequency of data (e.g., '1H', '1D')

    Returns:
        DataQualityReport with validation results
    """
    return DataQualityChecker.check(data, max_gap_ratio, expected_freq)
