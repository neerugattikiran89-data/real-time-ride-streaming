
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

numbers = env.from_collection([10, 20, 30, 40, 50])

numbers.print()

env.execute("Cluster Test PyFlink Job")
