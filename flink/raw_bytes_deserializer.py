from pyflink.common.serialization import DeserializationSchema
from pyflink.common import Types


class RawBytesDeserializer(DeserializationSchema):

    def deserialize(self, message):
        return message

    def get_type_info(self):
        return Types.PICKLED_BYTE_ARRAY()