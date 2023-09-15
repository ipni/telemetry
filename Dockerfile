FROM golang:1.21-bullseye as build

WORKDIR /go/src/telemetry

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /go/bin/telemetry ./cmd/telemetry

FROM gcr.io/distroless/static-debian11
COPY --from=build /go/bin/telemetry /usr/bin/

ENTRYPOINT ["/usr/bin/telemetry"]
