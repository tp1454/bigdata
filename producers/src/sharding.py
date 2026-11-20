"""Split a symbol list across producer pods."""

import os


def parse_shard(value):
    """Parse "index/count" (for example "1/3"); empty means one shard."""
    if not value:
        return 0, 1
    try:
        index, count = (int(part) for part in value.split("/"))
    except ValueError as e:
        raise ValueError(f"invalid shard {value!r}; use index/count") from e
    if count < 1 or not 0 <= index < count:
        raise ValueError(f"invalid shard {value!r}; need 0 <= index < count")
    return index, count


def shard_symbols(symbols, index, count):
    """Return every count-th symbol starting at index; count=1 keeps the list."""
    return [symbol for i, symbol in enumerate(symbols) if i % count == index]


def symbols_from_env(env_name, default, shard_env):
    """Read a symbol list and return this pod's shard; fail on an empty shard."""
    symbols = [
        symbol.strip()
        for symbol in os.getenv(env_name, default).split(",")
        if symbol.strip()
    ]
    index, count = parse_shard(os.getenv(shard_env, ""))
    result = shard_symbols(symbols, index, count)
    if not result:
        raise ValueError(
            f"{shard_env}={index}/{count} leaves no symbols; "
            "use fewer shards or add more symbols"
        )
    return result
