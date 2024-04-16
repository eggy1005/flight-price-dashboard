import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


def consume_data():
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()

    settings = EnvironmentSettings.new_instance() \
        .in_streaming_mode() \
        .build()

    # create table environment
    tbl_env = StreamTableEnvironment.create(stream_execution_environment=env,
                                            environment_settings=settings)

    # add kafka connector dependency
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                             'flink-sql-connector-kafka-3.1.0-1.18.jar')
    kafka_jar_2 = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               'kafka-clients-3.7.0.jar')
    tbl_env.get_config() \
        .get_configuration() \
        .set_string("pipeline.jars", "file://{};file://{}".format(kafka_jar, kafka_jar_2))
# SPLIT_INDEX(name_rating_city_country, '_', 0) AS name,
# SPLIT_INDEX(name_rating_city_country, '_', 1) AS rating,
# SPLIT_INDEX(name_rating_city_country, '_', 2) AS city,
# SPLIT_INDEX(name_rating_city_country, '_', 3) AS country,
    #######################################################################
    # Create Kafka Source Table with DDL
    #######################################################################
    src_ddl = """
        CREATE TABLE HOTELPRICE (
            name_rating_city_country String,
            price Double,
            breakfast_included BOOLEAN,
            update_timestamp BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'hotel-price',
            'properties.bootstrap.servers' = 'pkc-41wq6.eu-west-2.aws.confluent.cloud:9092',
            'properties.group.id' = 'flight-usd',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset',    
            'properties.client.dns.lookup' ='use_all_dns_ips',
            'properties.auto.offset.reset' = 'latest',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="7AICBYXFYBHMF5PJ" password="yylYReFtxTnInxAeng6ubVTs+sMXGBi8Hz0AdAIO4eVB8/LXJHxKASyh51B0eWu8";'
        )
    """

    tbl_env.execute_sql(src_ddl)

    src_ddl = """
        CREATE TABLE FLIGHTPRICE (
            airline STRING,
            flightno int,
            departure STRING,
            arrival STRING,
            price INT,
            currency STRING,
            update_timestamp BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'flight-price',
            'properties.bootstrap.servers' = 'pkc-41wq6.eu-west-2.aws.confluent.cloud:9092',
            'properties.group.id' = 'flight-usd',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset',    
            'properties.client.dns.lookup' ='use_all_dns_ips',
            'properties.auto.offset.reset' = 'earliest',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="7AICBYXFYBHMF5PJ" password="yylYReFtxTnInxAeng6ubVTs+sMXGBi8Hz0AdAIO4eVB8/LXJHxKASyh51B0eWu8";'
        )
    """
    tbl_env.execute_sql(src_ddl)

    # create and initiate loading of source Table
    tbl = tbl_env.from_path('HOTELPRICE')
    tbl = tbl_env.from_path('FLIGHTPRICE')

    average_sql_processor = """
    SELECT AVG(F.price + H.price),
    F.departure,
    F.arrival,
    TO_DATE(FROM_UNIXTIME(F.update_timestamp / 1000))
    FROM
    HOTELPRICE H JOIN FLIGHTPRICE F ON
    SPLIT_INDEX(name_rating_city_country, '_', 2) = F.arrival
    GROUP BY F.departure, F.arrival, TO_DATE(FROM_UNIXTIME(F.update_timestamp / 1000))
    """


    average_price_tbl = tbl_env.sql_query(average_sql_processor)
    # tbl_env.execute_sql(average_sql_processor).print()
    print('\nProcess Sink Schema')
    # average_price_tbl.print_schema()

    sink_ddl = """
            CREATE TABLE FLIGHT_HOTEL_PRICE_COMBINED (
                TOTAL_PRICE DOUBLE,
                DEPARTURE STRING,
                ARRIVAL STRING,
                ARRIVAL_DATE DATE,
                PRIMARY KEY (DEPARTURE, ARRIVAL, ARRIVAL_DATE) NOT ENFORCED
            ) WITH (
                'connector' = 'upsert-kafka',
                'topic' = 'flight_hotel_price_combined',
                'properties.bootstrap.servers' = 'pkc-41wq6.eu-west-2.aws.confluent.cloud:9092',
                'key.format' = 'json',
                'value.format' = 'json',  
                'properties.client.dns.lookup' ='use_all_dns_ips',
                'properties.auto.offset.reset' = 'earliest',
                'properties.security.protocol' = 'SASL_SSL',
                'properties.sasl.mechanism' = 'PLAIN',
                'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="7AICBYXFYBHMF5PJ" password="yylYReFtxTnInxAeng6ubVTs+sMXGBi8Hz0AdAIO4eVB8/LXJHxKASyh51B0eWu8";'
            )
    """
    tbl_env.execute_sql(sink_ddl)

    total_price_query = """
    INSERT INTO FLIGHT_HOTEL_PRICE_COMBINED
    SELECT AVG(F.price + H.price),
    F.departure,
    F.arrival,
    TO_DATE(FROM_UNIXTIME(F.update_timestamp / 1000))
    FROM
    HOTELPRICE H JOIN FLIGHTPRICE F ON
    SPLIT_INDEX(name_rating_city_country, '_', 2) = F.arrival
    GROUP BY F.departure, F.arrival, TO_DATE(FROM_UNIXTIME(F.update_timestamp / 1000))
    """
    combined_tbl = tbl_env.execute_sql(total_price_query).print()
    # combined_tbl.print_schema()
    # average_price_tbl.execute_insert('FLIGHT_HOTEL_PRICE_COMBINED').wait()

if __name__ == '__main__':
    consume_data()