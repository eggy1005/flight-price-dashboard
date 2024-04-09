import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


def consume_data():
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

    src_ddl = """
        CREATE TABLE HOTELPRICE (
            CITY STRING,
            PRICE_USD INT,
            CURRENCY STRING,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'hotel-price-usd',
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
    hotel_tbl = tbl_env.from_path('HOTELPRICE')


    print('\nSource Schema')
    tbl.print_schema()
    hotel_tbl.print_schema()

    # sql = """
    # SELECT * FROM FLIGHTPRICE """
    # tbl_env.execute_sql(sql).print();
    #####################################################################
    # Define Tumbling Window Aggregate Calculation (Seller Sales Per Minute)
    #####################################################################


    # ###############################################################
    # # Create Kafka Sink Table
    # ###############################################################
    sink_ddl = """
        CREATE TABLE FLIGHT_HOTEL_PRICE_COMBINED (
            DEPARTURE STRING,
            ARRIVAL STRING,
            TOTAL_PRICE INT,
            CURRENCY STRING,
            primary key (DEPARTURE, ARRIVAL, CURRENCY) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = 'flight_hotel_price_combined',
            'properties.bootstrap.servers' = 'localhost:9092',
            'key.format' = 'json',
            'value.format' = 'json'
        )
    """
    tbl_env.execute_sql(sink_ddl)

    total_price_query = """
    INSERT INTO FLIGHT_HOTEL_PRICE_COMBINED
    SELECT
    DEPARTURE, ARRIVAL, min(F.PRICE_USD + H.PRICE_USD), F.CURRENCY AS TOTAL_PRICE
    FROM
    FLIGHTPRICE F
    JOIN HOTELPRICE H 
    ON F.ARRIVAL = H.CITY
    GROUP BY DEPARTURE, ARRIVAL, F.CURRENCY;
    """
    average_price_tbl = tbl_env.execute_sql(total_price_query)

    tbl_env.execute_sql(total_price_query).print()
    print('\nProcess Sink Schema')
    average_price_tbl.print_schema()
    # write time windowed aggregations to sink table
    average_price_tbl.execute_insert('FLIGHT_HOTEL_PRICE_COMBINED').wait()


if __name__ == '__main__':
    consume_data()