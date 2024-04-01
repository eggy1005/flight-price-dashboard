from pyflink.datastream import StreamExecutionEnvironment
# /Users/jessy/Documents/Book/Kafka Summit/flink-1.19.0/bin/flink run --python main.py
# ./flink
# Create a Flink execution environment
env = StreamExecutionEnvironment.get_execution_environment()

# Define your data source
data = [
    (1, "Hello"),
    (2, "Fred"),
    (3, "Apache"),
    (4, "Wong")
]

# Create a Flink DataSet from the data
dataset = env.from_collection(data)
# Perform transformations
result = dataset \
    .filter(lambda x: x[0] % 2 == 0) \
    .map(lambda x: (x[0], x[1].upper()))

print("Testing Testing")

# Define the sink to write the result to
result.print()

# Execute the job
env.execute("Python Batch Job")
