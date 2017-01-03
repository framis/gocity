FROM golang:1.6.2
MAINTAINER Francois Misslin <francois.misslin@gmail.com>

ENV GOPATH /go
ENV USER root

# pre-install known dependencies before the source, so we don't redownload them whenever the source changes
RUN go get github.com/spf13/viper \
	&& go get github.com/algolia/algoliasearch-client-go/algoliasearch \
	&& go get github.com/algolia/algoliasearch-client-go/algoliasearch \
	&& go get github.com/mitchellh/ioprogress \
	&& go get github.com/fatih/structs



COPY . /go/src/github.com/framis/gocity

WORKDIR /go/src/github.com/framis/gocity

RUN go get -d -v \
	&& go install