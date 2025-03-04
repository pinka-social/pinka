FROM docker.io/library/rust as builder
WORKDIR /usr/src/pinka
COPY . .

RUN cargo build --release

FROM docker.io/library/debian:stable-slim
COPY --from=builder /usr/src/pinka/target/release/pinka /usr/local/bin/pinka

EXPOSE 8080
ENTRYPOINT [ "/usr/local/bin/pinka" ]