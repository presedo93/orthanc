"""DataBento API adapter for historical data fetching.

This module wraps the databento.Historical client to provide a clean interface
for fetching OHLCV data with proper error handling and response transformation.
"""

import logging
from datetime import datetime, timezone
from typing import Any

import databento as db

from ..errors import DataFetchError
from .types import OHLCV_COLUMNS, Schema, SType

logger = logging.getLogger(__name__)


class BentoAdapter:
    """Adapter for DataBento Historical API.

    Handles API authentication, request construction, and response parsing
    for the timeseries.get_range() endpoint.

    Examples:
        >>> adapter = BentoAdapter(api_key="db-xxx", dataset="GLBX.MDP3")
        >>> records = adapter.fetch_ohlcv(
        ...     symbol="MNQ.v.0",
        ...     schema=Schema.OHLCV_1M,
        ...     start=1704067200000,
        ...     end=1704153600000,
        ... )
    """

    def __init__(self, api_key: str, dataset: str) -> None:
        """Initialize the adapter.

        Args:
            api_key: DataBento API key.
            dataset: Dataset identifier (e.g., 'GLBX.MDP3' for CME futures).
        """
        self._dataset = dataset
        self._client = db.Historical(key=api_key)

    @property
    def dataset(self) -> str:
        """Return the dataset identifier."""
        return self._dataset

    def fetch_ohlcv(
        self,
        symbol: str,
        schema: Schema,
        start: int,
        end: int,
        stype_in: SType = SType.CONTINUOUS,
    ) -> list[dict[str, Any]]:
        """Fetch OHLCV data for a symbol.

        Args:
            symbol: Instrument symbol (e.g., 'MNQ.v.0' for continuous contract).
            schema: OHLCV schema (ohlcv-1s, ohlcv-1m, ohlcv-1h, ohlcv-1d).
            start: Start time in milliseconds since epoch.
            end: End time in milliseconds since epoch.
            stype_in: Symbol type for input resolution (default: continuous).

        Returns:
            List of OHLCV records with keys: timestamp, open, high, low, close, volume.

        Raises:
            DataFetchError: If API request fails.
        """
        # Convert milliseconds to ISO format strings for DataBento API
        start_iso = self._ms_to_iso(start)
        end_iso = self._ms_to_iso(end)

        logger.info(
            'Fetching %s for %s from %s to %s',
            schema.value,
            symbol,
            start_iso,
            end_iso,
        )

        try:
            # Call DataBento API
            data = self._client.timeseries.get_range(
                dataset=self._dataset,
                schema=schema.value,
                symbols=symbol,
                stype_in=stype_in.value,
                start=start_iso,
                end=end_iso,
            )

            # Convert to DataFrame and then to records
            df = data.to_df()

            if df.empty:
                logger.debug('No data returned for %s', symbol)
                return []

            return self._convert_dataframe(df)

        except db.BentoError as e:
            logger.error('DataBento API error for %s: %s', symbol, e)
            raise DataFetchError(f'DataBento API error for {symbol}: {e}') from e
        except Exception as e:
            logger.error('Unexpected error fetching %s: %s', symbol, e)
            raise DataFetchError(f'Unexpected error fetching {symbol}: {e}') from e

    def get_cost(
        self,
        symbols: str | list[str],
        schema: Schema,
        start: int,
        end: int,
        stype_in: SType = SType.CONTINUOUS,
    ) -> float:
        """Estimate cost for a data request before making it.

        This is useful to check costs before committing to expensive requests.

        Args:
            symbols: Symbol or list of symbols to query.
            schema: OHLCV schema.
            start: Start time in milliseconds since epoch.
            end: End time in milliseconds since epoch.
            stype_in: Symbol type for input resolution.

        Returns:
            Estimated cost in USD.
        """
        start_iso = self._ms_to_iso(start)
        end_iso = self._ms_to_iso(end)

        if isinstance(symbols, str):
            symbols = [symbols]

        try:
            cost = self._client.metadata.get_cost(
                dataset=self._dataset,
                schema=schema.value,
                symbols=symbols,
                stype_in=stype_in.value,
                start=start_iso,
                end=end_iso,
            )
            return float(cost)
        except Exception as e:
            logger.warning('Failed to get cost estimate: %s', e)
            return 0.0

    def _convert_dataframe(self, df: Any) -> list[dict[str, Any]]:
        """Convert DataBento DataFrame to standard OHLCV records.

        DataBento returns:
        - ts_event: Event timestamp (nanoseconds since epoch, as DatetimeIndex)
        - open, high, low, close: Price values (as fixed-point integers)
        - volume: Volume

        We convert to:
        - timestamp: Milliseconds since epoch
        - open, high, low, close, volume: Float values
        """
        # Extract timestamps from the DatetimeIndex (nanoseconds → milliseconds)
        ts_ms = df.index.astype('int64') // 1_000_000

        result = {
            'timestamp': ts_ms.tolist(),
            'open': df['open'].astype(float).tolist(),
            'high': df['high'].astype(float).tolist(),
            'low': df['low'].astype(float).tolist(),
            'close': df['close'].astype(float).tolist(),
            'volume': df['volume'].astype(float).tolist(),
        }

        # Transpose dict-of-lists to list-of-dicts
        keys = list(result.keys())
        return [{k: result[k][i] for k in keys} for i in range(len(ts_ms))]

    def _ms_to_iso(self, ms: int) -> str:
        """Convert milliseconds timestamp to ISO 8601 string."""
        dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        return dt.isoformat()

    def close(self) -> None:
        """Close the adapter (no-op for Historical client)."""
        # Historical client doesn't need explicit cleanup
        pass

    def __enter__(self) -> 'BentoAdapter':
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
