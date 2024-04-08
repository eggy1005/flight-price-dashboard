import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
# from pyflink.datastream.connectors.kafka import FlinkKafkaProducer

def main():
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()

    settings = EnvironmentSettings.new_instance()\
                      .in_streaming_mode()\
                      .build()

    # create table environment
    tbl_env = StreamTableEnvironment.create(stream_execution_environment=env,
                                            environment_settings=settings)

    # add kafka connector dependency
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                            'flink-sql-connector-kafka-3.1.0-1.18.jar')

    tbl_env.get_config()\
            .get_configuration()\
            .set_string("pipeline.jars", "file://{}".format(kafka_jar))

    #######################################################################
    # Create Kafka Source Table with DDL
    #######################################################################
    src_ddl = """
        CREATE TABLE FLIGHTPRICE (
            FLIGHTTIME STRING,
            DEPARTURE STRING,
            ARRIVAL STRING,
            PRICE_USD INT,
            CURRENCY STRING,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'flight-price-usd',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'flight-usd',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset',    
            'properties.auto.offset.reset' = 'earliest'
        )
    """

    tbl_env.execute_sql(src_ddl)

    # create and initiate loading of source Table
    tbl = tbl_env.from_path('FLIGHTPRICE')


    print('\nSource Schema')
    tbl.print_schema()

    # sql = """
    # SELECT * FROM FLIGHTPRICE """
    # tbl_env.execute_sql(sql).print();
    #####################################################################
    # Define Tumbling Window Aggregate Calculation (Seller Sales Per Minute)
    #####################################################################
    average_sql_processor = """
    SELECT
    DEPARTURE, ARRIVAL, AVG(PRICE_USD) AS AVERAGE_PRICE, CURRENCY
    FROM
    FLIGHTPRICE
    GROUP BY DEPARTURE, ARRIVAL, CURRENCY;
    """
    average_price_tbl = tbl_env.sql_query(average_sql_processor)

    tbl_env.execute_sql(average_sql_processor).print()
    print('\nProcess Sink Schema')
    average_price_tbl.print_schema()

    # ###############################################################
    # # Create Kafka Sink Table
    # ###############################################################
    # sink_ddl = """
    #     CREATE TABLE FLIGHTPRICE_AVG (
    #         DEPARTURE STRING,
    #         ARRIVAL STRING,
    #         AVERAGE_PRICE INT,
    #         CURRENCY STRING
    #     ) WITH (
    #         'connector' = 'kafka',
    #         'topic' = 'flight-price-average-usd',
    #         'properties.bootstrap.servers' = 'localhost:9092',
    #         'format' = 'json'
    #     )
    # """
    # tbl_env.execute_sql(sink_ddl)

    # write time windowed aggregations to sink table
    # average_price_tbl.execute_insert('FLIGHTPRICE_AVG').wait()




if __name__ == '__main__':
    main()