FROM docker.io/library/rust as builder
WORKDIR /usr/src/pinka
COPY . .

RUN cargo build --release

FROM registry.fedoraproject.org/fedora-minimal
COPY --from=builder /usr/src/pinka/target/release/pinka /usr/local/bin/pinka

EXPOSE 8080
ENTRYPOINT [ "/usr/local/bin/pinka" ]