FROM docker.io/library/golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG BIN=server
RUN CGO_ENABLED=0 go build -o /out/app ./cmd/${BIN}

FROM docker.io/library/alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
