all: env test packages

clean:
	bin/clean.sh

env:
	bin/env.sh

test:
	bin/test.sh

packages:
	bin/packages-datastax.sh
	bin/packages-cassandra.sh

binary:  clean env packages
	pyinstaller binary/binary-datastax.spec
	pyinstaller binary/binary-cassandra.spec
