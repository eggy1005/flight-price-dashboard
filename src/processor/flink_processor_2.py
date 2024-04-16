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
    kafka_jar_2 = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                             'kafka-clients-3.7.0.jar')
    tbl_env.get_config()\
            .get_configuration()\
            .set_string("pipeline.jars", "file://{};file://{}".format(kafka_jar, kafka_jar_2))

    #######################################################################
    # Create Kafka Source Table with DDL
    #######################################################################
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
    tbl = tbl_env.from_path('FLIGHTPRICE')

    average_sql_processor = """
        SELECT
        airline, departure, arrival, flightno, price, currency
        FROM
        FLIGHTPRICE
        """
    average_price_tbl = tbl_env.sql_query(average_sql_processor)

    tbl_env.execute_sql(average_sql_processor).print()
    print('\nProcess Sink Schema')
    average_price_tbl.print_schema()


if __name__ == '__main__':
    consume_data()