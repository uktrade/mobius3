FROM alpine:3.16

COPY requirements.txt /app/
RUN \
	apk add --no-cache \
		py3-pip \
		python3 && \
	pip install \
		-r /app/requirements.txt

COPY LICENSE README.md mobius3.py setup.py /app/
RUN \
	pip install /app && \
	pip check

RUN \
	addgroup -S mobius3 && \
	adduser -S mobius3 -G mobius3
USER mobius3

RUN mkdir /home/mobius3/data

WORKDIR /home/mobius3
