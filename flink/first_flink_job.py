from pyflink.datastream import StreamExecutionEnvironment

# Create Flink execution environment
env = StreamExecutionEnvironment.get_execution_environment()

# Set parallelism
env.set_parallelism(1)

# Create a simple stream
data_stream = env.from_collection([1, 2, 3, 4, 5])

# Print the stream
data_stream.print()

# Execute the job
env.execute("First PyFlink Streaming Job")