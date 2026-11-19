
import io
from fastavro import schemaless_reader, parse_schema


def decode_confluent_avro(message_bytes, schema):
    """
    Decode a Confluent Avro message.

    Confluent wire format:
    [Magic byte: 1 byte][Schema ID: 4 bytes][Avro payload]
    """

    if message_bytes is None:
        return None

    # Validate Confluent magic byte
    if message_bytes[0] != 0:
        raise ValueError("Invalid Confluent Avro magic byte")

    # Skip the 5-byte Confluent header
    avro_payload = message_bytes[5:]

    parsed_schema = parse_schema(schema)

    # Decode Avro payload
    buffer = io.BytesIO(avro_payload)

    return schemaless_reader(buffer, parsed_schema)
