FROM python:3

WORKDIR /usr/src/app

RUN apt update -yq && \
    pip3 install mkdocs mkdocs-material markdown-include mkdocs-markdownextradata-plugin

EXPOSE 8000

ENTRYPOINT [ "python", "-m", "mkdocs" ]

CMD ["serve", "--dev-addr=0.0.0.0:8000"]