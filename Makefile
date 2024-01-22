build:
	echo ${N26_PIP_INDEX_URL} | DOCKER_BUILDKIT=1 \
	docker build . \
	--secret id=n26_pip_index_url,src=/dev/stdin \
	--target main \
	--tag boston:build

format:
	echo ${N26_PIP_INDEX_URL} | DOCKER_BUILDKIT=1 \
	docker build . \
	--secret id=n26_pip_index_url,src=/dev/stdin \
	--target format \
	--tag boston:black

notebook: build
	docker-compose -f src/setup/hub/docker-compose.yaml up

up: build
	docker-compose -f src/setup/frontend/docker-compose.yaml up
