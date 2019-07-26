FROM alpine:3.10

RUN \
	apk add --no-cache \
		python3

COPY LICENSE README.md mobius3.py setup.py requirements.txt app/
WORKDIR /app

RUN \
	pip3 install \
		. \
		-r requirements.txt && \
	pip3 check

RUN \
	addgroup -S mobius3 && \
	adduser -S mobius3 -G mobius3
USER mobius3

WORKDIR /home/mobius3
