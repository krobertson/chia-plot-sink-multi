FROM golang:1.21-alpine AS builder

WORKDIR /go/src/github.com/krobertson/chia-plot-sink-multi
ADD . /go/src/github.com/krobertson/chia-plot-sink-multi/
RUN go get ./...

RUN CGO_ENABLED=0 GOOS=linux go build -o chia-plot-sink-multi *.go

FROM alpine:latest

EXPOSE 1337
WORKDIR /
CMD ["./chia-plot-sink-multi"]

COPY --from=builder /go/src/github.com/krobertson/chia-plot-sink-multi/chia-plot-sink-multi /chia-plot-sink-multi

