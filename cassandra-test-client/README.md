# DCOS Cassandra test client

A python helper script named `launcher.py` which launches Cassandra-Stress in a DCOS cluster as Marathon tasks.

Basic usage looks like this:

```
$ pip install -r requirements.txt
$ python launcher.py http://your-dcos-cluster.com
or...
$ DCOS_URI=http://your-dcos-cluster.com python launcher.py
```

See `python launcher.py --help` for a list of available options. Feel free to add more options for desirable Cassandra-Stress settings.
