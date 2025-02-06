FROM htcondor/mini:latest

RUN dnf -y install go

# test user and job
RUN useradd -d /htcondor-test -m tester
COPY htcondor-docker/hello* /htcondor-test/

# install library
WORKDIR /build/github.com/retzkek/htcondor-go
COPY . .
RUN go install
