"""Arrow integration submodule for pandas-gbq."""
from typing import Any, Optional

import pyarrow as pa


def from_read_rows_response(
    message: Any,
    arrow_schema: Optional[pa.Schema] = None,
) -> pa.RecordBatch:
    """Decodes a ReadRowsResponse protobuf message into a pyarrow.RecordBatch."""
    if not hasattr(message, "arrow_record_batch") or not message.arrow_record_batch.serialized_record_batch:
        return pa.RecordBatch.from_batches(schema=arrow_schema or pa.schema([]), batches=[])

    serialized_batch = message.arrow_record_batch.serialized_record_batch
    reader = pa.ipc.RecordBatchStreamReader(serialized_batch)
    batch = reader.read_next_batch()
    return batch
