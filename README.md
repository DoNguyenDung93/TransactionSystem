

# Cassandra Benchmarking Project

### Installing dependencies

- On each of the 5 cluster nodes, install [Cassandra 3.11](http://www.apache.org/dyn/closer.lua/cassandra/3.11.1/apache-cassandra-3.11.1-bin.tar.gz)
 by following the instruction to install Cassandra from tarball [here](http://cassandra.apache.org/doc/latest/getting_started/installing.html#installation-from-binary-tarball-files). Add `$CASSANDRA_HOME/bin` to your `PATH` so that you can run `cassandra` and `cqlsh` from anywhere.
- You should also have `Python 2.7` and `pip` installed. If you have no sudo access to the server, refer to [this guide](https://gist.github.com/saurabhshri/46e4069164b87a708b39d947e4527298) on how to install pip without sudo access.
- Install the python dependencies using `pip install --user -r requirements.txt`

### Configuring Cassandra

- Specify your cluster nodes' ip addresses and login details in `config.txt`. Make sure they are in order.
- Run `./configure.sh`

In the directory `conf`, you should find the cassandra configuration files for each of 5 cluster nodes. Replace the `cassandra.yaml`
in each of your cluster nodes with these files.

### Setup the schema

- Start the cassandra servers on all 5 nodes (by running `cassandra`)

- Go to any one of your cluster nodes, again make sure that `cqlsh` can be run from anywhere, and run `./setup.sh`

### Running benchmarks

- On each of the cluster nodes, download the project codes from [here](https://github.com/DoNguyenDung93/TransactionSystem/archive/master.zip) and unzip it inside the home folder

- For benchmarking, it's required that you have a server that has passwordless ssh access to the 5 cluster nodes.

- On this server that has passwordless ssh access to all the cluster node, run `./benchmark.sh <NUMBER_OF_CLIENTS> <CONSISTENCY_LEVEL>` to start benchmarking. For `CONSISTENCY_LEVEL`, `1` denotes consistency 
level `ONE`, while `4` denotes consistency level `QUORUM`. Log files will be created in your working directory when the benchmark script is running, so it is recommended that you run
the benchmarking inside a separate directory, and allocate enough memory to it (around 2GB).
