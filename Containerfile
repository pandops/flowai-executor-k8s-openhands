FROM docker.io/library/golang:1.26.2-bookworm@sha256:47ce5636e9936b2c5cbf708925578ef386b4f8872aec74a67bd13a627d242b19 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -mod=readonly -trimpath -ldflags='-s -w' -o /out/executor_k8s_openhands ./cmd/executor_k8s_openhands

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/executor_k8s_openhands /executor_k8s_openhands
COPY configs/executor_k8s_openhands.yaml /usr/share/flowai/config.example.yaml
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/executor_k8s_openhands"]
CMD ["--config", "/var/lib/flowai/executor-k8s/config/config.yaml"]
