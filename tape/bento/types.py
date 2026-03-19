"""DataBento-specific type definitions.

Continuous Contract Roll Types
------------------------------
DataBento supports three roll rules for continuous contracts:

- **Calendar (c)**: Switches to nearest contract by expiration date.
  Example: ES.c.0 (E-mini S&P 500, calendar roll, front month)

- **Volume (v)**: Switches to contract with highest trading volume (previous day).
  Example: MNQ.v.0 (Micro Nasdaq-100, volume roll, front month)

- **Open Interest (n)**: Switches to contract with highest OI (previous day's close).
  Example: CL.n.0 (Crude Oil, open interest roll, front month)

The index after the roll code represents the rank:
- .0 = Lead/front month (highest-ranking contract)
- .1 = Second-ranked contract
- .2 = Third-ranked contract, etc.

Symbol Type (stype_in) Options
------------------------------
- raw_symbol: Original exchange symbols (default)
- instrument_id: Unique numeric ID assigned by DataBento
- continuous: For continuous contracts (used with roll symbols like MNQ.v.0)
- parent: Parent instrument symbol (e.g., 'ES' for all ES futures)
- smart: Databento-specific symbology for groups of symbols
"""

from dataclasses import dataclass
from enum import Enum


class Schema(Enum):
    """Supported OHLCV schemas for DataBento API.

    DataBento provides pre-aggregated OHLCV bars at various intervals.
    These are more cost-effective than raw tick data for analysis.
    """

    OHLCV_1S = 'ohlcv-1s'
    OHLCV_1M = 'ohlcv-1m'
    OHLCV_1H = 'ohlcv-1h'
    OHLCV_1D = 'ohlcv-1d'


class SType(Enum):
    """Symbol type for input symbology resolution.

    Determines how the 'symbols' parameter is interpreted by the API.
    """

    RAW_SYMBOL = 'raw_symbol'
    INSTRUMENT_ID = 'instrument_id'
    CONTINUOUS = 'continuous'
    PARENT = 'parent'
    SMART = 'smart'


class RollType(Enum):
    """Roll type codes for continuous contracts.

    Used as reference for constructing continuous contract symbols.
    Symbol format: {SYMBOL}.{ROLL_TYPE}.{RANK}
    """

    CALENDAR = 'c'  # Roll by expiration date
    VOLUME = 'v'  # Roll by highest volume
    OPEN_INTEREST = 'n'  # Roll by highest open interest


# Column schema for OHLCV data
# DataBento returns: ts_event (nanoseconds), open, high, low, close, volume
OHLCV_COLUMNS = ['timestamp', 'open', 'high', 'low', 'close', 'volume']


# Schema to timeframe string mapping (for cache directory structure)
SCHEMA_TO_TIMEFRAME: dict[Schema, str] = {
    Schema.OHLCV_1S: '1s',
    Schema.OHLCV_1M: '1m',
    Schema.OHLCV_1H: '1h',
    Schema.OHLCV_1D: '1d',
}


@dataclass
class BentoFeed:
    """Configuration for a DataBento data feed.

    Attributes:
        name: Identifier for cache directory (e.g., "bento_ohlcv").
        schema: DataBento schema for the request.
        columns: Column names for the DataFrame schema.
        checkpoint_interval: Number of iterations between checkpoints.
    """

    name: str
    schema: Schema
    columns: list[str]
    checkpoint_interval: int = 50


# Pre-configured feeds for each OHLCV schema
FEED_OHLCV_1S = BentoFeed(
    name='bento_ohlcv',
    schema=Schema.OHLCV_1S,
    columns=OHLCV_COLUMNS,
)

FEED_OHLCV_1M = BentoFeed(
    name='bento_ohlcv',
    schema=Schema.OHLCV_1M,
    columns=OHLCV_COLUMNS,
)

FEED_OHLCV_1H = BentoFeed(
    name='bento_ohlcv',
    schema=Schema.OHLCV_1H,
    columns=OHLCV_COLUMNS,
)

FEED_OHLCV_1D = BentoFeed(
    name='bento_ohlcv',
    schema=Schema.OHLCV_1D,
    columns=OHLCV_COLUMNS,
)


# Schema lookup by string
SCHEMA_MAP: dict[str, Schema] = {
    'ohlcv-1s': Schema.OHLCV_1S,
    'ohlcv-1m': Schema.OHLCV_1M,
    'ohlcv-1h': Schema.OHLCV_1H,
    'ohlcv-1d': Schema.OHLCV_1D,
    '1s': Schema.OHLCV_1S,
    '1m': Schema.OHLCV_1M,
    '1h': Schema.OHLCV_1H,
    '1d': Schema.OHLCV_1D,
}


# Feed lookup by schema
FEED_MAP: dict[Schema, BentoFeed] = {
    Schema.OHLCV_1S: FEED_OHLCV_1S,
    Schema.OHLCV_1M: FEED_OHLCV_1M,
    Schema.OHLCV_1H: FEED_OHLCV_1H,
    Schema.OHLCV_1D: FEED_OHLCV_1D,
}
