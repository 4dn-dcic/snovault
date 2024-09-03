import os.path
import requests
import sys
import subprocess
import time

import atexit
import shutil
import tempfile


def server_process(datadir, host='localhost', port=9200, prefix='', echo=False, transport_ports=None):
    # args = [
    #     os.path.join(prefix, 'elasticsearch'),
    #     '-f',  # foreground
    #     '-Des.path.data="%s"' % os.path.join(datadir, 'data'),
    #     '-Des.path.logs="%s"' % os.path.join(datadir, 'logs'),
    #     '-Des.node.local=true',
    #     '-Des.discovery.zen.ping.multicast.enabled=false',
    #     '-Des.network.host=%s' % host,
    #     '-Des.http.port=%d' % port,
    #     '-Des.index.number_of_shards=1',
    #     '-Des.index.number_of_replicas=0',
    #     '-Des.index.store.type=memory',
    #     '-Des.index.store.fs.memory.enabled=true',
    #     '-Des.index.gateway.type=none',
    #     '-Des.gateway.type=none',
    #     '-XX:MaxDirectMemorySize=4096m',
    # ]
    args = [
        os.path.join(prefix, 'opensearch'),
        '-Enetwork.host=%s' % host,
        '-Ehttp.port=%d' % port,
        '-Epath.data=%s' % os.path.join(datadir, 'data'),
        '-Epath.logs=%s' % os.path.join(datadir, 'logs'),
        '-Epath.repo=%s' % os.path.join(datadir, 'snapshots'),
    ]
    # elasticsearch for travis
    if os.environ.get('TRAVIS'):
        echo = True  # noqa
        args.append('-Epath.conf=%s/deploy' % os.environ['TRAVIS_BUILD_DIR'])
    elif os.path.exists('/etc/elasticsearch'):
        # elasticsearch.deb setup
       args.append('-Epath.conf=/etc/elasticsearch')
    # set JVM heap size for ES
    if not os.environ.get('ES_JAVA_OPTS'):
        os.environ['ES_JAVA_OPTS'] = "-Xms4G -Xmx4G"

    elasticsearch_command = args
    elasticsearch_command_string = " ".join(elasticsearch_command)
    elasticsearch_url = f"http://{host}:{port}"
    log(f"Starting ElasticSearch ...")
    log(f"ElasticSearch command: {elasticsearch_command_string}")
    log(f"ElasticSearch endpoint: {elasticsearch_url}")
    # Note that (ElasticSearch subprocess) this inherits stdout/stderr from parent.
    # Previously (pre 2024-09-03) we did not do this and got tripped up for some
    # not completely understood reason related to logging output from ElasticSearch;
    # in any case this is the more correct way to do this.
    process = subprocess.Popen(args, stdin=None, stdout=None, stderr=None)
    log(f"Waiting for ElasticSearch to be up/running: {elasticsearch_url}")
    if not wait_for_elasticsearch_be_up_and_running(elasticsearch_url):
        log(f"WARNING: Did not detect that ElasticSearch is up/running: {elasticsearch_url} (but continuing)")
    else:
        log(f"ElasticSearch appears to be up/running: {elasticsearch_url}")

    return process


def wait_for_elasticsearch_be_up_and_running(url: str) -> bool:
    wait_interval_seconds = 3
    number_of_times_to_check = 20
    for n in range(number_of_times_to_check):
        time.sleep(wait_interval_seconds)
        try:
            _ = requests.get(url)
            return True
        except Exception:
            pass
    return False


def main():
    datadir = tempfile.mkdtemp()

    print('Starting in dir: %s' % datadir)
    started_ok = False
    try:
        process = server_process(datadir, echo=True)
        started_ok = True
    finally:
        if not started_ok:
            shutil.rmtree(datadir)

    @atexit.register
    def cleanup_process():
        try:
            if process.poll() is None:
                process.terminate()
                for line in process.stdout:
                    sys.stdout.write(line.decode('utf-8'))
                process.wait()
        finally:
            shutil.rmtree(datadir)


def log(message: str) -> None:
    print(f"PORTAL: {message}", flush=True)


if __name__ == '__main__':
    main()

