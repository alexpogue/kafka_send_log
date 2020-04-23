from time import sleep
from datetime import datetime
from kafka import KafkaProducer
import string
import sys
import json


def main():
    if len(sys.argv) < 5:
        print('Usage: python {} <filename> <cluster> <servicename> <jobid> [<kafka_url:port]'.format(sys.argv[0]))
        sys.exit(1)

    filename, cluster, servicename, jobid = sys.argv[1:5]

    kafka_url_and_port = sys.argv[5] if len(sys.argv) > 5 else 'localhost:9092'
    print('kakfa_url_and_port = {}'.format(kafka_url_and_port))

    producer = KafkaProducer(bootstrap_servers=[kafka_url_and_port],
                             value_serializer=lambda x:
                             json.dumps(x).encode('utf-8'))
    with open(filename) as f:
        content = f.readlines()

        to_append = ',{},{},{}'.format(cluster, servicename, jobid)
        content_appended = [''.join([line.strip(), to_append]) for line in content]

    data = {}
    data['lines'] = content_appended

    print('sending data')
    producer.send('test-new', value=data)

if __name__ == '__main__':
    main()
