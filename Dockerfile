# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
COPY web ./web

ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags="-s -w" -o /out/hooklens .

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/hooklens /hooklens

EXPOSE 8080
USER nonroot:nonroot

# 用 CMD 而不是把参数写死在 ENTRYPOINT 里，compose / PaaS 可以直接覆盖。
ENTRYPOINT ["/hooklens"]
CMD ["-addr", ":8080"]
