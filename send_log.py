import sys
import json
from kafka import KafkaProducer

def printVals(**kwargs):
    for name, value in kwargs.items():
        print('{} = {}'.format(name, value))

def main():
    if len(sys.argv) < 6 or len(sys.argv) > 7:
        print('Usage: python {} <filename> <cluster> <service_name> <job_id> <topic_name> [<kafka_url:port]'.format(sys.argv[0]))
        sys.exit(1)

    filename, cluster, service, job_id, topic = sys.argv[1:6]
    kafka_url_and_port = sys.argv[6] if len(sys.argv) > 6 else 'localhost:9092'

    printVals(filename=filename, cluster=cluster, service=service, job_id=job_id, topic=topic, kafka_url_and_port=kafka_url_and_port)

    with open(filename) as f:
        content = f.readlines()

        to_append = ',{},{},{}'.format(cluster, service, job_id)
        content_appended = [''.join([line.strip(), to_append]) for line in content[1:]]

    content_appended[0] = ''.join([content_appended[0], ',clustername,servicename,jobid'])

    data = {'lines': content_appended}

    producer = KafkaProducer(bootstrap_servers=[kafka_url_and_port],
                             value_serializer=lambda x:
                             json.dumps(x).encode('utf-8'))

    print('sending data')
    producer.send('{}'.format(topic), value=data)

if __name__ == '__main__':
    main()
