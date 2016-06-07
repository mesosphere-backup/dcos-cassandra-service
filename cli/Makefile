all: env test packages

clean:
	bin/clean.sh

env:
	bin/env.sh

test:
	bin/test.sh

packages:
	bin/packages.sh

binary:  clean env packages
	pyinstaller binary/binary.spec
