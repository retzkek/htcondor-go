.PHONY: test
test: | docker submit-in-docker test-in-docker clean

.PHONY: docker
docker:
	-docker rm -f htcondor-go-test
	docker build -t htcondor-go-test .
	docker run -d --name htcondor-go-test htcondor-go-test
	sleep 5 # let htcondor start up

.PHONY: submit-in-docker
submit-in-docker:
	docker exec -it htcondor-go-test su -l tester -c 'condor_submit hello.sub && condor_submit hello_neverrun.sub'
	sleep 30 # let job run

.PHONY: test-in-docker
test-in-docker:
	docker exec -it htcondor-go-test go test -v

.PHONY: clean
clean:
	-docker rm -f htcondor-go-test
