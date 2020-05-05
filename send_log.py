import sys
import json
import argparse
from kafka import KafkaProducer


def main():
    parser = argparse.ArgumentParser(description='Send a file through Kafka.')
    parser.add_argument('-f', '--file', help='file to send', metavar='FILE.jtl', required=True)
    parser.add_argument('-k', '--kafka-url', help='Kafka url and port', metavar='URL:PORT', default='localhost:9092')
    parser.add_argument('-t', '--kafka-topic', help='Kafka topic to use', metavar='TOPIC_NAME', required=True)
    parser.add_argument('fields', nargs='*', help='values to append to each csv line before sending', metavar='FIELD=VALUE')

    args = parser.parse_args()

    keys = [arg.split('=')[0] for arg in args.fields]
    values = [arg.split('=')[1] for arg in args.fields]

    keys_to_append = ','.join(keys)
    print('keys_to_append = {}'.format(keys_to_append))

    values_to_append = ','.join(values)
    print('values_to_append = {}'.format(values_to_append))

    with open(args.file) as f:
        content = f.readlines()
        first_line = ','.join([content[0].strip(), keys_to_append])
        subsequent_lines = [','.join([line.strip(), values_to_append]) for line in content[1:]]

        content_appended = [first_line]
        content_appended += subsequent_lines

    data = {'lines': content_appended}

    producer = KafkaProducer(bootstrap_servers=[args.kafka_url],
                             value_serializer=lambda x:
                             json.dumps(x).encode('utf-8'))

    print('sending data')
    producer.send('{}'.format(args.kafka_topic), value=data)

if __name__ == '__main__':
    main()
