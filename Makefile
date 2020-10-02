all: TestBuildMR TestWC TestIndexer TestMapParallelism TestReduceParallelism TestCrash
###DO NOT MAKE ANY CHANGES TO THIS FILE
###OR YOU MAY LOSE YOUR POINTS!
SHELL := /bin/bash

TestBuildMR:
	chmod 0777 ./setEnv.sh; \
	source ./setEnv.sh; \
	export HOME="${CURDIR}"; \
	cd "${CURDIR}/src/main/"; \
	chmod 0777 ./test-mr.sh; \
	source ./test-mr.sh; \
	buildMR

TestWC:
	chmod 0777 ./setEnv.sh; \
	source ./setEnv.sh; \
	export HOME="${CURDIR}"; \
	cd "${CURDIR}/src/main/"; \
	chmod 0777 ./test-mr.sh; \
	source ./test-mr.sh; \
	TestWC

TestIndexer:
	chmod 0777 ./setEnv.sh; \
	source ./setEnv.sh; \
	export HOME="${CURDIR}"; \
	cd "${CURDIR}/src/main/"; \
	chmod 0777 ./test-mr.sh; \
	source ./test-mr.sh; \
	TestIndexer

TestMapParallelism:
	chmod 0777 ./setEnv.sh; \
	source ./setEnv.sh; \
	export HOME="${CURDIR}"; \
	cd "${CURDIR}/src/main/"; \
	chmod 0777 ./test-mr.sh; \
	source ./test-mr.sh; \
	TestMapParallelism

TestReduceParallelism:
	chmod 0777 ./setEnv.sh; \
	source ./setEnv.sh; \
	export HOME="${CURDIR}"; \
	cd "${CURDIR}/src/main/"; \
	chmod 0777 ./test-mr.sh; \
	source ./test-mr.sh; \
	TestReduceParallelism


TestCrash:
	chmod 0777 ./setEnv.sh; \
	source ./setEnv.sh; \
	export HOME="${CURDIR}"; \
	cd "${CURDIR}/src/main/"; \
	chmod 0777 ./test-mr.sh; \
	source ./test-mr.sh; \
	TestCrash







