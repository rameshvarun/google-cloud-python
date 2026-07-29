"""Arrow integration submodule for pandas-gbq."""
from typing import Any, Optional

import pyarrow as pa


def from_read_rows_response(
    message: Any,
    arrow_schema: Optional["pa.Schema"] = None,
) -> "pa.RecordBatch":
    if pa is None:
        raise ImportError(
            "pyarrow is required to use 'from_read_rows_response'. "
            "Please install pyarrow to use this function."
        )
    """Decodes a ReadRowsResponse protobuf message into a pyarrow.RecordBatch."""
    if not hasattr(message, "arrow_record_batch") or not message.arrow_record_batch.serialized_record_batch:
        return pa.RecordBatch.from_arrays([], schema=arrow_schema or pa.schema([]))
        return pa.RecordBatch.from_pylist([], schema=empty_schema)
    serialized_batch = message.arrow_record_batch.serialized_record_batch
    reader = pa.ipc.RecordBatchStreamReader(serialized_batch)
    try:
        return reader.read_next_batch()
    except StopIteration:
        return pa.RecordBatch.from_arrays([], schema=arrow_schema or pa.schema([]))
    if arrow_schema is not None:
        try:
            return pa.ipc.read_record_batch(buffer, arrow_schema)
        except Exception:
            pass

    try:
        reader = pa.ipc.RecordBatchStreamReader(buffer)
        return reader.read_next_batch()
    except Exception:
        msg = pa.ipc.read_message(buffer)
        batch_schema = arrow_schema if arrow_schema is not None else getattr(msg, "schema", pa.schema([]))
        return pa.ipc.read_record_batch(msg, batch_schema)
