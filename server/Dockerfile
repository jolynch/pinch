ARG REGISTRY=docker.io

###### BUILD CONTAINER ######
FROM $REGISTRY/golang:latest as builder

# Dependencies e.g.
# RUN go get -u google.golang.org/grpc

ENV PATH=$PATH:$GOPATH/bin:/opt/protoc/bin

# Now copy over our code
RUN mkdir -p /go/src/

COPY go.mod /go/src/go.mod
COPY main.go /go/src/main.go

WORKDIR /go/src/
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -tags netgo -ldflags='-w -extldflags "-static"' -o pinch-server

# Get a proper init
RUN wget -O /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.4/dumb-init_1.2.4_x86_64
RUN chmod +x /usr/local/bin/dumb-init

###### RUNTIME CONTAINER   ######
FROM jolynch/pinch

COPY --from=builder /usr/local/bin/dumb-init /usr/bin/dumb-init
COPY --from=builder /go/src/pinch-server /usr/local/bin/pinch-server

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["/usr/local/bin/pinch-server"]
